use crate::cutoff::{Acceptability, CutOffConfig, CutOffDuration, CutOffHashPos, CutOffTime};
use crate::disk_abstraction::Disk;
use crate::message_store::OnDiskMessageStore;
use crate::sequence_nr::SequenceNr;
use crate::update_head_tracker::UpdateHeadTracker;
use crate::{
    Application, ContextGuardMut, DatabaseContextData, Message, MessageHeader, MessageId,
    MessagePayload, NoatunContext, NoatunTime, Target,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::marker::PhantomData;
use std::pin::Pin;
use tracing::{error, info};

pub(crate) struct Projector<APP: Application> {
    messages: OnDiskMessageStore<APP::Message>,
    head_tracker: UpdateHeadTracker,
    phantom_data: PhantomData<APP>,
    cut_off_config: CutOffConfig,
}

impl<APP: Application> Projector<APP> {
    pub(crate) fn advance_cutoff(
        &mut self,
        new_cutoff_at: CutOffTime,
        context: &mut DatabaseContextData,
    ) -> Result<()> {
        let mut cutoff_state = self.messages.current_cutoff_hash()?;

        let old_cutoff_index = self
            .messages
            .get_index_after(cutoff_state.before_time.to_noatun_time())?;
        let cutoff_index = self
            .messages
            .get_index_after(new_cutoff_at.to_noatun_time())?;

        //println!("Advancing cutoff from {:?} to {:?}, index = {}, comp : {}", cutoff_state.before_time, new_cutoff_at, cutoff_index, cutoff_state.before_time >= new_cutoff_at);
        let unused_list = unsafe { context.get_unused_list() };
        let unused_list = unused_list.get_full_slice(context);

        //println!("Unused list: {:#?}", unused_list);
        debug_assert!(unused_list.is_sorted_by_key(|x| x.last_overwriter));

        //println!("Unused list: {:?}", unused_list);
        let (Ok(unused_list_last) | Err(unused_list_last)) =
            unused_list.binary_search_by_key(&cutoff_index, |x| x.last_overwriter);

        //println!("Last index: {}, last overwriter: {:?}", unused_list_last, cutoff_index);
        let mut process_now = vec![];
        cutoff_state.before_time = new_cutoff_at;

        let messages_slice = self.messages.get_messages_slice()?;
        let new_cutoff_at_noatun_time = new_cutoff_at.to_noatun_time();
        //println!("Advancing {:?}", old_cutoff_index.index()..cutoff_index.index());
        let mut remove_orders = Vec::new();
        for index_entry in &messages_slice[old_cutoff_index.index()..cutoff_index.index()] {
            if index_entry.file_offset.is_deleted() {
                continue;
            }
            if let Some((_hdr, mut children_to_remove)) = self
                .messages
                .read_message_header_and_children(index_entry.message)?
            {
                children_to_remove.retain(|x| x.timestamp() < new_cutoff_at_noatun_time);
                remove_orders.push((index_entry.message, children_to_remove));
            } else {
                error!("Encountered deleted message in cutoff-processing");
            }
            cutoff_state.apply(index_entry.message, "advance add");
        }
        for (message, children_to_remove) in remove_orders {
            self.messages
                .remove_all_parents_and_some_children(message, &children_to_remove)?;
        }
        for item in &unused_list[..unused_list_last] {
            debug_assert!(item.last_overwriter < cutoff_index);
            process_now.push(*item);
        }
        self.messages.set_cutoff_hash(cutoff_state)?;

        let must_remove =
            context.rt_calculate_stale_messages_impl(&mut self.messages, process_now, true)?; //TODO: Rename
        for index in must_remove {
            self.messages
                .mark_deleted_by_index(index, &mut self.head_tracker)?;
        }

        self.head_tracker
            .remove_before_cutoff(cutoff_state.before_time.to_noatun_time())?;

        self.messages.set_cutoff_time(new_cutoff_at)?;
        Ok(())
    }

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

    pub fn current_cutoff_hash(&self) -> Result<CutOffHashPos> {
        self.messages.current_cutoff_hash()
    }
    pub fn current_cutoff_time(&self) -> Result<NoatunTime> {
        self.messages.current_cutoff_time()
    }
    pub fn nominal_cutoff_time(&self, now: NoatunTime) -> CutOffTime {
        self.cut_off_config
            .nominal_cutoff(CutOffTime::from_noatun_time(now))
    }

    pub fn is_acceptable_cutoff_hash(&self, hash: CutOffHashPos) -> Result<Acceptability> {
        self.messages
            .is_acceptable_cutoff_hash(hash, &self.cut_off_config)
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

    pub fn recover(&mut self, now: NoatunTime) -> Result<()> {
        self.head_tracker.clear();
        let cutoff = self.messages.current_cutoff_time()?;
        self.messages.recover(
            |id, parents| self.head_tracker.add_new_update_head(id, parents, cutoff),
            now,
            &self.cut_off_config,
        )
    }
    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.messages.get_all_message_ids()
    }
    pub fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        //TODO: Return iterator instead of Vec, for perf
        self.messages.get_children_of(msg)
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
        cutoff_interval: CutOffDuration,
    ) -> Result<Projector<APP>> {
        Ok(Projector {
            messages: OnDiskMessageStore::new(s, target, max_size)?,
            head_tracker: UpdateHeadTracker::new(s, target)?,
            phantom_data: PhantomData,
            cut_off_config: CutOffConfig::new(cutoff_interval)?,
        })
    }

    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
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
        messages.dedup_by_key(|x| x.id());
        info!("Deduped list to insert: {:?}", messages);

        let cutoff_time = self.messages.current_cutoff_time()?;
        for message in messages.iter_mut() {
            if message.header.id.timestamp() < cutoff_time {
                message.header.parents.clear();
            }
        }

        self.push_sorted_messages(context, messages.into_iter(), local)
    }
    pub(crate) fn push_sorted_messages(
        &mut self,
        context: &mut DatabaseContextData,
        messages: impl ExactSizeIterator<Item = Message<APP::Message>>,
        local: bool,
    ) -> Result<bool> {
        //debug_assert_eq!(self.messages.count_messages()?, context.next_seqnr().try_index().unwrap_or(0));
        let cutoff = self.current_cutoff_time()?;
        if let Some(insert_point) = self.messages.append_many_sorted(
            messages,
            |id, parents| self.head_tracker.add_new_update_head(id, parents, cutoff),
            local,
        )? {
            if let Some(cur_main_db_next_index) = context.next_seqnr().try_index() {
                if insert_point < cur_main_db_next_index {
                    #[cfg(debug_assertions)]
                    if insert_point > 0 {
                        info!(
                            "checking if insertion point {} exists: {}",
                            insert_point,
                            self.messages.contains_index(insert_point)?
                        );
                        debug_assert!(self.messages.contains_index(insert_point)?);
                    }
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
        msg.payload.apply(msg.header.id.timestamp(), unsafe {
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
    pub(crate) fn get_index_of_time(&mut self, time: NoatunTime) -> Result<usize> {
        //let stamp = time.timestamp_millis() as u64;
        let key = MessageId::from_parts_raw(time.as_ms() + 1, [0; 10])?;
        let index = self.messages.get_insertion_point(key)?;
        Ok(index)
    }

    pub(crate) fn apply_missing_messages(
        &mut self,
        context: &mut DatabaseContextData,
        root: &mut APP,
        max_project_to: Option<NoatunTime>,
    ) -> Result<()> {
        //println!("Max project to : {:?}", max_project_to);
        //let cutoff = self.cut_off_config.nominal_cutoff(real_time_now);

        //let cur_seqnr = context.next_seqnr();

        context.clear_unused_tracking();

        let first_run = self
            .messages
            .query_by_index(context.next_seqnr().try_index().unwrap())?;

        let max_project_to = match max_project_to {
            None => NoatunTime::MAX,
            Some(max_project_to) => max_project_to,
        };

        do_run::<APP>(context, root, first_run, max_project_to)?;
        remove_stale_messages(self, context)?;

        /// If returns true, need to finalize before-cutoff-part, then continue at given index
        fn do_run<APP: Application>(
            context: &mut DatabaseContextData,
            root: &mut APP,
            items: impl Iterator<Item = (usize, Message<APP::Message>)>,
            max_project_to: NoatunTime,
        ) -> Result<()> {
            for (seq, msg) in items {
                if msg.header.id.timestamp() > max_project_to {
                    return Ok(());
                }
                let seqnr = SequenceNr::from_index(seq);
                //println!("Projecting {}, time: {:?}, cutoff: {:?}", seq, DateTime::from_timestamp_millis(msg.id().timestamp() as i64), DateTime::from_timestamp_millis(cutoff as i64));
                Projector::<APP>::apply_single_message(context, root, &msg, seqnr);
            }
            Ok(())
        }

        fn remove_stale_messages<APP: Application>(
            tself: &mut Projector<APP>,
            context: &mut DatabaseContextData,
        ) -> Result<()> {
            let must_remove = context.calculate_stale_messages(&mut tself.messages)?;
            for index in must_remove {
                tself
                    .messages
                    .mark_deleted_by_index(index, &mut tself.head_tracker)?;
                //*self.messages.get_index_mut(index.index()).unwrap().1 = None;
            }
            Ok(())
        }
        //let next_index = self.messages.next_index()?;
        //context.set_next_seqnr(SequenceNr::from_index(next_index));
        Ok(())
    }
}
