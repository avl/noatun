use crate::cutoff::{CutOffConfig, CutoffHash};
use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::message_store::{IndexEntry, OnDiskMessageStore};
use crate::sequence_nr::SequenceNr;
use crate::update_head_tracker::UpdateHeadTracker;
use crate::{
    Application, ContextGuardMut, Database, DatabaseContextData, Message, MessageHeader, MessageId,
    MessagePayload, NoatunContext, NoatunTime, Target, CONTEXT,
};
use anyhow::Result;
use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Utc};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

pub(crate) struct Projector<APP: Application> {
    messages: OnDiskMessageStore<APP::Message>,
    head_tracker: UpdateHeadTracker,
    phantom_data: PhantomData<APP>,
    cut_off_config: CutOffConfig,
}

impl<APP: Application> Projector<APP> {
    pub fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*count*/ usize)> + '_> {
        self.messages.get_upstream_of(message_id)
    }

    pub(crate) fn get_update_heads(&self) -> &[MessageId] {
        self.head_tracker.get_update_heads()
    }
    pub(crate) fn get_messages_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        self.messages.get_messages_at_or_after(message, count)
    }

    pub fn nominal_cutoffhash(&self) -> Result<CutoffHash> {
        self.messages.nominal_cutoffhash()
    }

    pub fn is_acceptable_cutoff_hash(&self, hash: CutoffHash) -> Result<bool> {
        self.messages.is_acceptable_cutoff_hash(hash)
    }

    pub(crate) fn contains_message(&self, id: MessageId) -> Result<bool> {
        self.messages.contains_message(id)
    }

    pub(crate) fn load_message(&self, id: MessageId) -> Result<Message<APP::Message>> {
        Ok(self
            .messages
            .read_message(id)?
            .ok_or_else(|| anyhow::anyhow!("Message not found"))?)
    }

    pub fn recover(&mut self) -> Result<()> {
        self.messages
            .recover(|id, parents| self.head_tracker.add_new_update_head(id, parents))
    }
    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.messages.get_all_message_ids()
    }
    pub fn get_all_messages(&self) -> Result<Vec<Message<APP::Message>>> {
        self.messages.get_all_messages()
    }
    pub fn get_all_messages_with_children(
        &self,
    ) -> Result<Vec<(Message<APP::Message>, Vec<MessageId>)>> {
        self.messages.get_all_messages_with_children()
    }

    pub(crate) fn new<D: Disk>(
        s: &mut D,
        target: &Target,
        max_size: usize,
        cutoff_interval: Duration,
    ) -> Result<Projector<APP>> {
        Ok(Projector {
            messages: OnDiskMessageStore::new(s, target, max_size)?,
            head_tracker: UpdateHeadTracker::new(s, target)?,
            phantom_data: PhantomData,
            cut_off_config: CutOffConfig::default(),
        })
    }

    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<()> {
        self.messages.mark_transmitted(message_id)
    }

    /// Returns true if the message did not exist and was inserted
    fn push_message(
        &mut self,
        context: &mut DatabaseContextData,
        message: Message<APP::Message>,
        local: bool,
    ) -> Result<bool> {
        self.push_sorted_messages(context, std::iter::once(message), local)
    }

    /// Returns true if any of the messages were not previously present
    pub(crate) fn push_messages(
        &mut self,
        context: &mut DatabaseContextData,
        message: impl Iterator<Item = Message<APP::Message>>,
        local: bool,
    ) -> Result<bool> {
        let mut messages: Vec<Message<APP::Message>> = message.collect();
        messages.sort_unstable_by_key(|x| x.id());

        self.push_sorted_messages(context, messages.into_iter(), local)
    }
    pub(crate) fn push_sorted_messages(
        &mut self,
        context: &mut DatabaseContextData,
        messages: impl ExactSizeIterator<Item = Message<APP::Message>>,
        local: bool,
    ) -> Result<bool> {
        //debug_assert_eq!(self.messages.count_messages()?, context.next_seqnr().try_index().unwrap_or(0));
        if let Some(insert_point) = self.messages.append_many_sorted(
            messages,
            |id, parents| self.head_tracker.add_new_update_head(id, parents),
            local,
        )? {
            if let Some(cur_main_db_next_index) = context.next_seqnr().try_index() {
                if insert_point < cur_main_db_next_index {
                    self.rewind(context, insert_point)?;
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub(crate) fn rewind(&mut self, context: &mut DatabaseContextData, point: usize) -> Result<()> {
        context.rewind(SequenceNr::from_index(point));
        Ok(())
    }

    fn apply_single_message(
        context: &mut DatabaseContextData,
        root: &mut APP,
        msg: &Message<APP::Message>,
        seqnr: SequenceNr,
    ) {

        let guard = ContextGuardMut::new(context);

        if context.next_seqnr() != seqnr {
            context.set_next_seqnr(seqnr); //TODO: Maybe we can optimize this somehow?
        }

        //println!("Applying message #{}", context.next_seqnr());
        msg.payload
            .apply(NoatunTime(msg.header.id.timestamp()), unsafe {
                Pin::new_unchecked(root)
            }); //TODO: Handle panics in apply gracefully
        drop(guard);


        context.set_next_seqnr(seqnr.successor()); //TODO: Don't record a snapshot for _every_ message.
        context.finalize_message(seqnr);
    }

    pub(crate) fn apply_preview(
        &mut self,
        time: DateTime<Utc>,
        mut root: Pin<&mut APP>,
        preview: impl Iterator<Item = APP::Message>,
    ) -> Result<()> {
        NoatunContext.clear_unused_tracking();
        let time = NoatunTime(time.timestamp_millis() as u64);
        for msg in preview {
            msg.apply(time, root.as_mut());
        }

        Ok(())
    }

    /// Returns the first index _after_ the given time.
    /// I.e, rewinding to this index will leave only messages at time and before.
    pub(crate) fn get_index_of_time(&mut self, time: DateTime<Utc>) -> Result<usize> {
        let stamp = time.timestamp_millis() as u64;
        let key = MessageId::from_parts_raw(stamp + 1, [0; 10])?;
        let index = self.messages.get_insertion_point(key)?;
        Ok(index)
    }

    pub(crate) fn apply_missing_messages(
        &mut self,
        context: &mut DatabaseContextData,
        root: &mut APP,
        real_time_now: DateTime<Utc>,
        max_project_to: Option<DateTime<Utc>>,
    ) -> Result<()> {
        //println!("Max project to : {:?}", max_project_to);
        let cutoff = self.cut_off_config.nominal_cutoff(real_time_now);

        let cur_seqnr = context.next_seqnr();

        context.clear_unused_tracking();

        let first_run = self
            .messages
            .query_by_index(context.next_seqnr().try_index().unwrap())?;

        let max_project_to = match max_project_to {
            None => u64::MAX,
            Some(max_project_to) => max_project_to.timestamp_millis().try_into()?,
        };

        match do_run::<APP>(context, root, first_run, cutoff, max_project_to)? {
            RunResult::NeedRunAfterCutoff(next_run_start) => {
                remove_stale_messages(self, context, true);
                let second_run = self.messages.query_by_index(next_run_start)?;
                let RunResult::Finished(before_cutoff) =
                    do_run::<APP>(context, root, second_run, cutoff, max_project_to)?
                else {
                    unreachable!(
                        "Second run _also_ encountered elements that were both before and after cutoff!"
                    )
                };
                remove_stale_messages(self, context, before_cutoff);
            }
            RunResult::Finished(before_cutoff) => {
                remove_stale_messages(self, context, before_cutoff);
            }
        }

        enum RunResult {
            NeedRunAfterCutoff(usize),
            Finished(bool /*before cutoff*/),
        }

        /// If returns true, need to finalize before-cutoff-part, then continue at given index
        fn do_run<APP: Application>(
            context: &mut DatabaseContextData,
            root: &mut APP,
            items: impl Iterator<Item = (usize, Message<APP::Message>)>,
            cutoff: u64,
            max_project_to: u64,
        ) -> Result<RunResult> {
            let mut seen_before_cutoff = false;
            let mut last_element_was_before_cutoff = false;
            for (seq, msg) in items {
                if msg.header.id.timestamp() > max_project_to {
                    return Ok(RunResult::Finished(last_element_was_before_cutoff));
                }
                let seqnr = SequenceNr::from_index(seq);
                //println!("Projecting {}, time: {:?}, cutoff: {:?}", seq, DateTime::from_timestamp_millis(msg.id().timestamp() as i64), DateTime::from_timestamp_millis(cutoff as i64));
                let is_before_cutoff = msg.id().timestamp() < cutoff;
                if is_before_cutoff {
                    seen_before_cutoff = true;
                }
                if !is_before_cutoff && seen_before_cutoff {
                    //println!("Need run after cutoff");
                    return Ok(RunResult::NeedRunAfterCutoff(seqnr.index()));
                }
                Projector::<APP>::apply_single_message(context, root, &msg, seqnr);
                last_element_was_before_cutoff = is_before_cutoff;
            }
            Ok(RunResult::Finished(last_element_was_before_cutoff))
        }

        fn remove_stale_messages<APP: Application>(
            tself: &mut Projector<APP>,
            context: &mut DatabaseContextData,
            before_cutoff: bool,
        ) -> Result<()> {
            let must_remove =
                context.calculate_stale_messages(&mut tself.messages, before_cutoff)?;
            for index in must_remove {
                tself.messages.mark_deleted_by_index(index);
                //*self.messages.get_index_mut(index.index()).unwrap().1 = None;
            }
            Ok(())
        }
        //let next_index = self.messages.next_index()?;
        //context.set_next_seqnr(SequenceNr::from_index(next_index));
        Ok(())
    }
}
