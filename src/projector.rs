use crate::cutoff::{Acceptability, CutOffConfig, CutOffDuration, CutOffHashPos, CutOffTime};
use crate::disk_abstraction::Disk;
use crate::message_store::OnDiskMessageStore;
use crate::sequence_nr::SequenceNr;
use crate::update_head_tracker::UpdateHeadTracker;
use crate::{
    catch_and_log, dprintln, ContextGuardMut, DatabaseContextData, Message, MessageFrame,
    MessageHeader, MessageId, NoatunTime, Persistence, Target,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::pin::Pin;
use tracing::{error, info, trace};

pub(crate) struct Projector<MSG: Message> {
    messages: OnDiskMessageStore<MSG>,
    head_tracker: UpdateHeadTracker,
    /// The configured cutoff interval and stride
    cut_off_config: CutOffConfig,
}

impl<MSG: Message + 'static> Projector<MSG> {
    pub(crate) fn disable_filesystem_sync(&mut self) {
        self.messages.disable_filesystem_sync()
    }
    pub(crate) fn sync_all(&mut self) -> Result<()> {
        self.messages.sync_all()?;
        self.head_tracker.sync_all()?;
        Ok(())
    }
    pub(crate) fn advance_cutoff(
        &mut self,
        new_cutoff_at: CutOffTime,
        context: &mut DatabaseContextData,
    ) -> Result<()> {
        let mut prev_cutoff_state = self.messages.prev_cutoff_hash()?;
        let mut cutoff_state = self.messages.current_cutoff_hash()?;

        dprintln!(
            "@{} {:?} Advancing cutoff {} -> {}",
            crate::cur_node(),
            crate::test_elapsed(),
            cutoff_state.before_time.to_noatun_time(),
            new_cutoff_at.to_noatun_time()
        );

        let old_prev_cutoff_index = self
            .messages
            .get_index_at_or_after(prev_cutoff_state.before_time.to_noatun_time())?;

        let old_cutoff_index = self
            .messages
            .get_index_at_or_after(cutoff_state.before_time.to_noatun_time())?;

        assert_eq!(old_cutoff_index, self.messages.cutoff_index());

        let cutoff_index = self
            .messages
            .get_index_at_or_after(new_cutoff_at.to_noatun_time())?;

        assert!(new_cutoff_at > cutoff_state.before_time);
        cutoff_state.before_time = new_cutoff_at;
        prev_cutoff_state.before_time = new_cutoff_at.saturating_sub(self.cut_off_config.stride);

        let messages_slice = self.messages.get_messages_slice()?;

        let mut remove_orders = Vec::new();

        for index_entry in &messages_slice[old_prev_cutoff_index.index()..old_cutoff_index.index()]
        {
            if index_entry.file_offset.is_deleted() {
                continue;
            }
            prev_cutoff_state.apply(index_entry.message, "advance add");
        }

        for index_entry in &messages_slice[old_cutoff_index.index()..cutoff_index.index()] {
            if index_entry.file_offset.is_deleted() {
                continue;
            }
            if let Some((_hdr, children_to_remove)) = self
                .messages
                .read_message_header_and_children(index_entry.message)?
            {
                remove_orders.push((index_entry.message, children_to_remove));
            } else {
                error!("Encountered deleted message in cutoff-processing");
            }
            cutoff_state.apply(index_entry.message, "advance add");
        }

        for (message, children_to_remove) in remove_orders {
            self.messages
                .remove_all_parents_and_some_children(message, &children_to_remove)?;
            for child in children_to_remove {
                self.messages.add_remove_parents_and_children(
                    child,
                    &[],
                    Some(message),
                    &[],
                    None,
                )?;
            }
        }

        /*
                let unused_list = unsafe { context.get_unused_list() };
                let unused_list = unused_list.get_full_slice(context);

                dprintln!("@{} Advancing list: {:?}, cutoff_index: {}", crate::cur_node(), unused_list, cutoff_index);

                debug_assert!(unused_list.is_sorted_by_key(|x| x.last_overwriter));

                let (Ok(unused_list_last) | Err(unused_list_last)) =
                    unused_list.binary_search_by_key(&cutoff_index, |x| x.last_overwriter);


                dprintln!("@{} Selected Advancing list: {:?}, last: {}", crate::cur_node(), &unused_list[..unused_list_last] , unused_list_last);
        */

        let mut must_remove = Vec::new();

        /*
        for index_entry in &messages_slice[old_cutoff_index.index()..cutoff_index.index()] {
            context.
        }*/

        /*let mut process_now = vec![];
        for item in &unused_list[..unused_list_last] {
            debug_assert!(item.last_overwriter < cutoff_index);
            process_now.push(*item);
        }*/
        self.messages
            .advance_cutoff_hash(prev_cutoff_state, cutoff_state)?;

        self.messages.set_cutoff_index(cutoff_index);

        dprintln!(
            "advance {:?}..{:?}",
            old_cutoff_index.index(),
            cutoff_index.index()
        );
        context.try_delete_all_that_were_overwritten_by_range(
            old_cutoff_index.index()..cutoff_index.index(),
            &self.messages,
            &mut must_remove,
        )?;

        //dprintln!("@{} Calling rt_calc with {:?}", crate::cur_node(), process_now);

        /*let must_remove =
            context.rt_calculate_stale_messages_impl(&mut self.messages)?;
        //debug_assert!(process_now.is_empty());*/

        for index in must_remove {
            self.messages
                .mark_deleted_by_index(index, &mut self.head_tracker)?;
        }

        self.head_tracker
            .remove_before_cutoff(cutoff_state.before_time.to_noatun_time())?;

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

    pub fn is_acceptable_cutoff_hash(
        &self,
        hash: CutOffHashPos,
        now: NoatunTime,
    ) -> Result<Acceptability> {
        self.messages
            .is_acceptable_cutoff_hash(hash, &self.cut_off_config, now)
    }

    pub(crate) fn contains_message(&self, id: MessageId) -> Result<bool> {
        self.messages.contains_message(id)
    }

    pub(crate) fn load_message(&self, id: MessageId) -> Result<MessageFrame<MSG>> {
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
        self.messages.get_children_of(msg)
    }
    pub fn get_all_messages(
        &self,
    ) -> Result<impl Iterator<Item = MessageFrame<MSG>> + use<'_, MSG>> {
        self.messages.get_all_messages()
    }
    pub fn get_all_messages_with_children(
        &self,
    ) -> Result<Vec<(MessageFrame<MSG>, Vec<MessageId>)>> {
        self.messages.get_all_messages_with_children()
    }

    pub(crate) fn new<D: Disk>(
        s: &mut D,
        target: &Target,
        max_size: usize,
        cutoff_interval: CutOffDuration,
    ) -> Result<Projector<MSG>> {
        Ok(Projector {
            messages: OnDiskMessageStore::new(s, target, max_size)?,
            head_tracker: UpdateHeadTracker::new(s, target)?,
            cut_off_config: CutOffConfig::new(cutoff_interval)?,
        })
    }

    pub fn loaded_existing_db(&self) -> bool {
        self.messages.loaded_existing_db()
    }
    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        self.messages.mark_transmitted(message_id)
    }

    pub fn count_messages(&self) -> usize {
        self.messages.count_messages()
    }

    pub(crate) fn compact(&mut self) -> Result<()> {
        self.messages.compact()
    }

    /// Returns true if the message did not exist and was inserted
    fn push_message(
        &mut self,
        context: &mut DatabaseContextData,
        message: MessageFrame<MSG>,
        local: bool,
    ) -> Result<bool> {
        self.push_sorted_messages(context, std::iter::once(&message), local)
    }

    /// Returns true if any of the messages were not previously present
    pub(crate) fn push_messages<'a>(
        &mut self,
        context: &mut DatabaseContextData,
        message: impl Iterator<Item = &'a MessageFrame<MSG>>,
        local: bool,
    ) -> Result<bool> {
        let mut messages: Vec<&MessageFrame<MSG>> = message.collect();
        messages.sort_unstable_by_key(|x| x.id());
        messages.dedup_by_key(|x| x.id());
        trace!("Deduped list to insert: {:?}", messages);

        let cutoff_time = self.messages.current_cutoff_time()?;
        for message in messages.iter_mut() {
            if message.header.id.timestamp() < cutoff_time {
                //TODO: Surely we should just be pruning parents here instead?
                debug_assert!(message.header.parents.is_empty());
            }
        }

        self.push_sorted_messages(context, messages.into_iter(), local)
    }
    pub(crate) fn push_sorted_messages<'a>(
        &mut self,
        context: &mut DatabaseContextData,
        messages: impl ExactSizeIterator<Item = &'a MessageFrame<MSG>>,
        local: bool,
    ) -> Result<bool> {
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
                        trace!(
                            "checking if insertion point {} exists: {}",
                            insert_point,
                            self.messages.contains_index(insert_point)?
                        );
                        debug_assert!(self.messages.contains_index(insert_point)?);
                    }
                    info!("Rewinding to {} after insertion", insert_point);
                    self.rewind(context, SequenceNr::from_index(insert_point))?;
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub(crate) fn remove_message(
        &mut self,
        message_id: MessageId,
        force: bool,
    ) -> Result<Option<SequenceNr>> {
        self.messages
            .delete_many(std::iter::once(message_id), &mut self.head_tracker, force)
    }
    pub(crate) fn rewind(
        &mut self,
        context: &mut DatabaseContextData,
        point: SequenceNr,
    ) -> Result<()> {
        context.rewind(point);
        Ok(())
    }

    fn apply_single_message<M: Message>(
        context: &mut DatabaseContextData,
        root: &mut MSG::Root,
        msg: &MessageFrame<MSG>,
        seqnr: SequenceNr,
        must_remove: &mut Vec<SequenceNr>,
        messages: &OnDiskMessageStore<M>,
    ) -> Result<()> {
        if context.next_seqnr() != seqnr {
            context.set_next_seqnr(seqnr);
        }
        context.clear_wrote_tombstone();
        match msg.payload.persistence() {
            Persistence::UntilOverwritten => {
                context.clear_tainted();
            }
            Persistence::AtLeastUntilCutoff => {
                context.set_tainted();
            }
        }
        let guard = ContextGuardMut::new(context, true);

        catch_and_log(|| {
            msg.payload
                .apply(msg.header.id, unsafe { Pin::new_unchecked(root) });
        });

        drop(guard);

        context.set_next_seqnr(seqnr.successor());
        context.rt_finalize_message(seqnr, must_remove, messages)?;
        Ok(())
    }

    pub(crate) fn apply_preview(
        &mut self,
        time: DateTime<Utc>,
        mut root: Pin<&mut MSG::Root>,
        preview: impl Iterator<Item = MSG>,
    ) -> Result<()> {
        let time = NoatunTime(time.timestamp_millis() as u64);
        catch_and_log(|| {
            for msg in preview {
                msg.apply(MessageId::from_parts_for_test(time, 0), root.as_mut());
            }
        });

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
        root: &mut MSG::Root,
        context: &mut DatabaseContextData,
        max_project_to: Option<NoatunTime>,
        auto_delete: bool,
    ) -> Result<Option<SequenceNr> /*earliest deleted index*/> {
        //context.clear_unused_tracking();

        let first_run = self
            .messages
            .query_by_index(context.next_seqnr().try_index().unwrap())?;

        let max_project_to = match max_project_to {
            None => NoatunTime::MAX,
            Some(max_project_to) => max_project_to,
        };

        let mut must_remove = Vec::new();
        do_run::<MSG>(
            context,
            &self.messages,
            root,
            first_run,
            max_project_to,
            &mut must_remove,
        )?;

        if !auto_delete {
            return Ok(None);
        }
        return remove_stale_messages(self, context, must_remove);

        /// If returns true, need to finalize before-cutoff-part, then continue at given index
        fn do_run<MSG: Message>(
            context: &mut DatabaseContextData,
            messages: &OnDiskMessageStore<MSG>,
            root: &mut MSG::Root,
            items: impl Iterator<Item = (usize, MessageFrame<MSG>)>,
            max_project_to: NoatunTime,
            must_remove: &mut Vec<SequenceNr>,
        ) -> Result<()> {
            for (seq, msg) in items {
                if msg.header.id.timestamp() > max_project_to {
                    break;
                }
                let seqnr = SequenceNr::from_index(seq);
                Projector::<MSG>::apply_single_message(
                    context,
                    root,
                    &msg,
                    seqnr,
                    must_remove,
                    messages,
                )?;

                //must_remove.extend(context.calculate_stale_messages(messages)?);
            }
            Ok(())
        }

        fn remove_stale_messages<MSG: Message>(
            tself: &mut Projector<MSG>,
            context: &mut DatabaseContextData,
            must_remove: Vec<SequenceNr>,
        ) -> Result<Option<SequenceNr /*minimum deleted*/>> {
            //let must_remove = context.calculate_stale_messages(&mut tself.messages)?;
            let mut earliest_deleted = None;
            for index in must_remove {
                let was_deleted = tself
                    .messages
                    .mark_deleted_by_index(index, &mut tself.head_tracker)?;
                if was_deleted {
                    earliest_deleted = Some(
                        earliest_deleted
                            .map(|x: SequenceNr| x.min(index))
                            .unwrap_or(index),
                    );
                }
            }
            Ok(earliest_deleted)
        }
    }
}
