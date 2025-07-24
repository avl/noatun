use crate::cutoff::{Acceptability, CutOffDuration, CutOffHashPos, CutOffTime};
use crate::disk_abstraction::{InMemoryDisk, StandardDisk};
use crate::projector::Projector;
use crate::sequence_nr::SequenceNr;
use crate::{
    ContextGuard, ContextGuardMut, DatabaseContextData, Message, MessageFrame, MessageHeader,
    MessageId, NoatunContext, NoatunTime, Object, Pointer, Target,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::pin::Pin;
use tracing::{error, info, trace};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoadingStatus {
    NewDatabase,
    CleanLoad,
    RecoveryPerformed,
}

/// If thread execution is halted while a mutable Database-operation is running,
/// the Database will enter a "corrupted" state. Clearing this state requires mutable access,
/// and this state can thus not be recovered from within any of the methods accepting `&self`.
///
/// Any access to a method using `&mut self` (and [`Self::recover`] in particular), will
/// execute the recovery procedure and restore the database to working order.
pub struct Database<MSG: Message> {
    context: DatabaseContextData,
    message_store: Projector<MSG>,
    // Most recently generated local id, or all zeroes.
    // Future local id's will always be greater than this.
    prev_local: Option<MessageId>,
    time_override: Option<NoatunTime>,
    projection_time_limit: Option<NoatunTime>,
    load_status: LoadingStatus,
    /// Automatically prune stale messages
    auto_delete: bool,
}
impl<T: Message> Debug for Database<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Database<{}>", std::any::type_name::<T>())
    }
}

/// Setting that control a database instance
pub struct DatabaseSettings {
    /// If Some, use the provided time instead of system time.
    /// Default None.
    pub mock_time: Option<NoatunTime>,
    /// If Some, do not project any messages timestamped after the given time
    /// Default None.
    pub projection_time_limit: Option<NoatunTime>,
    /// Use false to disable automatic deletion of subsumed messages.
    ///
    /// Normally, Noatun will detect when all effects of a message have been overwritten
    /// by later messages, and automatically delete the former.
    ///
    /// Default value is true - automatically delete messages that no longer affect the
    /// state.
    pub auto_prune: bool,
    /// The maximum size the noatun database can grow to.
    /// This amount of virtual memory space will be reserved by noatun. Note,
    /// the memory isn't actually used until data is put in the database.
    /// It's perfectly reasonable to put a value of 100GB here, even if the database
    /// is usually only a few gigabytes.
    pub max_file_size: usize,
    pub cutoff_interval: CutOffDuration,
}
impl Default for DatabaseSettings {
    fn default() -> Self {
        DatabaseSettings {
            mock_time: None,
            projection_time_limit: None,
            auto_prune: true,
            max_file_size: 1_000_000_000,
            cutoff_interval: CutOffDuration::from_minutes(15),
        }
    }
}

pub struct DatabaseSessionMut<'a, MSG: Message> {
    db: &'a mut Database<MSG>,
}

pub struct DatabaseSession<'a, MSG: Message> {
    db: &'a Database<MSG>,
}

impl<MSG: Message> Drop for DatabaseSessionMut<'_, MSG> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.db.mark_clean();
        }
    }
}

impl<MSG: Message + 'static> DatabaseSession<'_, MSG> {
    pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
        self.db.contains_message(message_id)
    }

    pub fn remove_cutoff_parents(&self, parents: &mut Vec<MessageId>) {
        if let Ok(cutoff) = self.db.current_cutoff_time() {
            parents.retain(|x| x.timestamp() >= cutoff);
        }
    }

    pub(crate) fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)> + '_> {
        self.db.get_upstream_of(message_id)
    }

    pub fn load_message(&self, message_id: MessageId) -> Result<MessageFrame<MSG>> {
        self.db.load_message(message_id)
    }

    pub(crate) fn get_update_heads(&self) -> &[MessageId] {
        self.db.get_update_heads()
    }

    pub(crate) fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        self.db.get_messages_at_or_after(message, count)
    }

    pub(crate) fn is_acceptable_cutoff_hash(&self, hash: CutOffHashPos) -> Result<Acceptability> {
        self.db.is_acceptable_cutoff_hash(hash)
    }
    pub(crate) fn current_cutoff_state(&self) -> Result<CutOffHashPos> {
        self.db.current_cutoff_state()
    }
    pub(crate) fn current_cutoff_time(&self) -> Result<NoatunTime> {
        self.db.current_cutoff_time()
    }
    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.db.get_all_message_ids()
    }
    pub(crate) fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        self.db.get_message_children(msg)
    }

    /// Retrieve all messages in the system.
    pub fn get_all_messages(
        &self,
    ) -> Result<impl Iterator<Item = MessageFrame<MSG>> + use<'_, MSG>> {
        self.db.get_all_messages()
    }
    /// Retrieve all messages in the system in a Vec
    pub fn get_all_messages_vec(&self) -> Result<Vec<MessageFrame<MSG>>> {
        Ok(self.db.get_all_messages()?.collect())
    }
    pub(crate) fn get_all_messages_with_children(
        &self,
    ) -> Result<Vec<(MessageFrame<MSG>, Vec<MessageId>)>> {
        self.db.get_all_messages_with_children()
    }

    /// Access the materialized view.
    ///
    /// This method yields read-only access. The only way to update the materialized view
    /// is through messages. Please add messages to the database using
    /// [`DatabaseSessionMut::append_local`] (for example), and let the message's
    /// [`Message::apply`] method modify the database.
    ///
    /// NOTE!
    /// This method offer very little overhead, but this means it also does not validate
    /// the database before executing. This should always be safe, but it means that recovery
    /// will not occur if the database has been corrupted by a previous operation.
    ///
    /// In normal operation, the database is never corrupted. However, if it somehow is
    /// (by a thread being killed, for example), this method could produce a root object that
    /// is in a state of a message being half-applied, for example.
    pub fn with_root<R>(&self, f: impl FnOnce(&MSG::Root) -> R) -> R {
        self.db.with_root(f)
    }

    pub fn noatun_now(&self) -> NoatunTime {
        self.db.noatun_now()
    }

    /// This method returns 0 on error.
    pub fn count_messages(&self) -> usize {
        self.db.count_messages()
    }
}

/// Determine how any pre-existing database file is to be handled
pub enum OpenMode {
    /// Open existing database, or create new if none exists
    OpenCreate,
    /// Unconditionally overwrite any pre-existing database file
    /// (this is mostly useful for tests)
    Overwrite,
}

impl OpenMode {
    fn overwrite_existing(&self) -> bool {
        matches!(self, OpenMode::Overwrite)
    }
}

impl<MSG: Message + 'static> DatabaseSessionMut<'_, MSG> {
    pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
        self.db.contains_message(message_id)
    }

    pub fn remove_cutoff_parents(&self, parents: &mut Vec<MessageId>) {
        if let Ok(cutoff) = self.db.current_cutoff_time() {
            parents.retain(|x| x.timestamp() >= cutoff);
        }
    }

    pub(crate) fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)> + '_> {
        self.db.get_upstream_of(message_id)
    }

    pub fn load_message(&self, message_id: MessageId) -> Result<MessageFrame<MSG>> {
        self.db.load_message(message_id)
    }

    pub(crate) fn get_update_heads(&self) -> &[MessageId] {
        self.db.get_update_heads()
    }

    pub(crate) fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        self.db.get_messages_at_or_after(message, count)
    }

    pub(crate) fn is_acceptable_cutoff_hash(&self, hash: CutOffHashPos) -> Result<Acceptability> {
        self.db.is_acceptable_cutoff_hash(hash)
    }
    pub(crate) fn current_cutoff_state(&self) -> Result<CutOffHashPos> {
        self.db.current_cutoff_state()
    }
    pub(crate) fn current_cutoff_time(&self) -> Result<NoatunTime> {
        self.db.current_cutoff_time()
    }
    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.db.get_all_message_ids()
    }
    pub(crate) fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        self.db.get_message_children(msg)
    }
    pub fn get_all_messages(
        &self,
    ) -> Result<impl Iterator<Item = MessageFrame<MSG>> + use<'_, MSG>> {
        self.db.get_all_messages()
    }
    pub fn get_all_messages_vec(&self) -> Result<Vec<MessageFrame<MSG>>> {
        Ok(self.db.get_all_messages()?.collect())
    }
    pub(crate) fn get_all_messages_with_children(
        &self,
    ) -> Result<Vec<(MessageFrame<MSG>, Vec<MessageId>)>> {
        self.db.get_all_messages_with_children()
    }

    /// Note, this method offer very little overhead, but this means it also does not validate
    /// the database before executing. This should always be safe, but it means that recovery
    /// will not occur if the database has been corrupted by a previous operation.
    ///
    /// In normal operation, the database is never corrupted. However, if it somehow is
    /// (by a thread being killed, for example), this method could produce a root object that
    /// is in a state of a message being half-applied, for example.
    pub fn with_root<R>(&self, f: impl FnOnce(&MSG::Root) -> R) -> R {
        self.db.with_root(f)
    }

    pub fn noatun_now(&self) -> NoatunTime {
        self.db.noatun_now()
    }

    /// This method returns 0 on error.
    pub fn count_messages(&self) -> usize {
        self.db.count_messages()
    }

    /// Advance the 'cutoff' time of the db, if necessary.
    ///
    /// The cutoff is the time in the past, such that all messages sent before that time
    /// have had time to propagate to all nodes. Since no node will never (by definition)
    /// see any additional message with a timestamp before the cutoff, messages that
    /// don't have any remaining effect on the database state (because the data they wrote
    /// has been overwritten by other messages) can thus be removed.
    ///
    /// This method evaluates if enough time has passed to warrant scanning for old messages
    /// that can be removed according to the principle above, and performs such pruning if
    /// necessary.
    pub fn maybe_advance_cutoff(&mut self) -> Result<()> {
        self.db.maybe_advance_cutoff()
    }

    /// This disables filesystem write back. Write-back will still occur, but a power-cut
    /// or unclean operating system shut down can cause newly written messages to be lost,
    /// even though the call that added them returned an Ok result.
    pub fn disable_filesystem_sync(&mut self) -> Result<()> {
        self.db.disable_filesystem_sync()
    }

    /// Delete the message with the given id.
    ///
    /// Normally, an event that has been distributed to other nodes can't be deleted,
    /// since this would affect eventual consistency. Use the `force` option to
    /// force deletion even in this case. This is generally only safe if the same delete
    /// is performed concurrently on all nodes, and the message is not in flight anywhere
    /// in the network. As a special case, it is safe if no other replicas currently exist.
    pub fn remove_message(&mut self, message_id: MessageId, force: bool) -> Result<()> {
        self.db.remove_message(message_id, force)
    }

    pub(crate) fn advance_cutoff(&mut self, new_cutoff: CutOffTime) -> Result<()> {
        self.db.advance_cutoff(new_cutoff)
    }

    // Only intended for internal tests
    #[cfg(test)]
    pub(crate) fn force_rewind(&mut self, index: SequenceNr) {
        self.db.force_rewind(index);
    }

    pub fn with_root_preview<R>(
        &mut self,
        time: DateTime<Utc>,
        preview: impl Iterator<Item = MSG>,
        f: impl FnOnce(&MSG::Root) -> R,
    ) -> Result<R> {
        self.db.with_root_preview(time, preview, f)
    }

    // This method allows modifying the state outside of the message apply loop.
    // This is completely broken and only useful in tests.
    #[cfg(test)]
    pub(crate) fn with_root_mut<R>(
        &mut self,
        f: impl FnOnce(Pin<&mut MSG::Root>) -> R,
    ) -> Result<R> {
        self.db.with_root_mut(f)
    }

    pub fn set_projection_time_limit(&mut self, limit: NoatunTime) -> Result<()> {
        self.db.set_projection_time_limit(limit)
    }

    /// Force a complete rewind and re-application of all messages.
    ///
    /// You should never need this, but it remains publicly visible to ease in
    /// debugging. Since this re-runs all message-applies, it can be used during debugging
    /// of [`crate::Message::apply`].
    pub fn reproject(&mut self) -> Result<()> {
        self.db.reproject()
    }

    /// Set the current time to the given value.
    /// This does not update the system time, it only affects the time for Noatun.
    ///
    /// This may advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    pub fn set_mock_time(&mut self, time: NoatunTime) -> Result<()> {
        self.db.set_mock_time(time)?;
        Ok(())
    }

    /// Returns true if the message still exists.
    /// If this returns false, the message has been deleted, and *MUST* not *BE* transmitted.
    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        self.db.mark_transmitted(message_id)
    }

    #[inline]
    pub fn append_local(&mut self, message: MSG) -> Result<MessageHeader> {
        self.db.append_local(message)
    }

    pub(crate) fn compact(&mut self) -> Result<()> {
        self.db.compact()
    }

    pub fn append_local_at(&mut self, time: NoatunTime, message: MSG) -> Result<MessageHeader> {
        self.db.append_local_at(time, message)
    }

    pub fn append_many_local_at(
        &mut self,
        time: NoatunTime,
        messages: impl Iterator<Item = MSG>,
    ) -> Result<()> {
        self.db.append_many_local_at(time, messages)
    }

    pub fn create_message_frame(
        &mut self,
        time: Option<NoatunTime>,
        message: MSG,
    ) -> Result<MessageFrame<MSG>> {
        self.db.create_message_frame(time, message)
    }

    /// Append a single message to the database.
    ///
    /// Note, this fails if the message cannot be added to the backing store but succeeds
    /// otherwise. Notably, it succeeds even if applying the message to the database
    /// causes a panic. Any such panic will be caught, and an entry will be emitted to the log.
    pub fn append_single(&mut self, message: &MessageFrame<MSG>, local: bool) -> Result<()> {
        self.db.append_single(message, local)
    }

    pub fn append_local_opt(
        &mut self,
        time: Option<NoatunTime>,
        message: MSG,
    ) -> Result<MessageHeader> {
        self.db.append_local_opt(time, message)
    }

    /// For messages before the cutoff-time, all parents are removed.
    pub fn append_many<'b>(
        &mut self,
        messages: impl Iterator<Item = &'b MessageFrame<MSG>>,
        local: bool,
        allow_cutoff_advance: bool,
    ) -> Result<()> {
        self.db.append_many(messages, local, allow_cutoff_advance)
    }
}
impl<MSG: Message> Drop for Database<MSG> {
    fn drop(&mut self) {
        if let Err(err) = self.sync_all() {
            error!("Error while dropping Database: {:?}", err);
        }
    }
}

impl<MSG: Message + 'static> Database<MSG> {
    pub fn begin_session_mut(&mut self) -> Result<DatabaseSessionMut<'_, MSG>> {
        self.mark_dirty()?;
        Ok(DatabaseSessionMut { db: self })
    }

    pub fn sync_all(&mut self) -> Result<()> {
        self.message_store.sync_all()?;
        self.context.sync_all()?;
        self.mark_fully_clean()?;
        Ok(())
    }

    pub fn begin_session(&self) -> Result<DatabaseSession<'_, MSG>> {
        self.assert_not_dirty()?;
        Ok(DatabaseSession { db: self })
    }

    fn assert_not_dirty(&self) -> Result<()> {
        if self.context.is_dirty() {
            bail!("Database is in a corrupted state. Call Database::recovery, or restart the application, to recover");
        }
        Ok(())
    }

    #[inline]
    fn mark_clean(&mut self) {
        self.context.mark_hot_clean()
    }

    #[inline]
    fn mark_fully_clean(&mut self) -> Result<()> {
        self.context.mark_fully_clean()?;
        Ok(())
    }

    fn mark_dirty(&mut self) -> Result<()> {
        if !self.context.mark_dirty()? {
            // Recovery needed
            self.do_recovery()?;
        }
        Ok(())
    }

    #[inline(never)]
    pub(crate) fn do_recovery(&mut self) -> Result<()> {
        let now = self.noatun_now();
        Self::recover_impl(
            &mut self.context,
            &mut self.message_store,
            &DatabaseSettings {
                mock_time: Some(now),
                projection_time_limit: self.projection_time_limit,
                auto_prune: self.auto_delete,
                ..DatabaseSettings::default()
            },
        )?;
        self.do_apply_missing()?;
        Ok(())
    }

    /// Recover database if in corrupted state.
    /// This method is very fast in the case where the database is not corrupted.
    fn recover(&mut self) -> Result<()> {
        if self.context.is_dirty() {
            self.do_recovery()?;
            self.mark_clean();
        }
        Ok(())
    }

    /// Explicitly check if enough time has passed to be able to auto_delete some messages.
    ///
    /// Messages that are older than the cutoff period are assumed to have reached all nodes.
    /// Specifically, it is assumed that no message will ever arrive with a time stamp before
    /// the cutoff time.
    ///
    /// This means that messages that have no current visible effects can be considered
    /// definitely stale and can be removed. This method will delete messages if possible.
    fn maybe_advance_cutoff(&mut self) -> Result<()> {
        if !self.auto_delete {
            return Ok(());
        }
        let now = self.noatun_now();
        let nominal_cutoff_time = self.message_store.nominal_cutoff_time(now);
        let current_cutoff = self.message_store.current_cutoff_hash()?;
        if nominal_cutoff_time > current_cutoff.before_time {
            self.advance_cutoff_impl(nominal_cutoff_time)?;
        }
        Ok(())
    }

    /// This disables filesystem write back. Write-back will still occur, but a power-cut
    /// or unclean operating system shut down can cause newly written messages to be lost,
    /// even though the call that added them returned an Ok result.
    fn disable_filesystem_sync(&mut self) -> Result<()> {
        self.message_store.disable_filesystem_sync();
        self.context.disable_filesystem_sync();
        Ok(())
    }
    fn advance_cutoff_impl(&mut self, new_cutoff: CutOffTime) -> Result<()> {
        if !self.auto_delete {
            return Ok(());
        }
        self.message_store
            .advance_cutoff(new_cutoff, &mut self.context)
    }

    fn advance_cutoff(&mut self, new_cutoff: CutOffTime) -> Result<()> {
        self.advance_cutoff_impl(new_cutoff)
    }

    // Only intended for internal tests
    #[cfg(test)]
    fn force_rewind(&mut self, index: SequenceNr) {
        self.context.rewind(index)
    }

    fn contains_message(&self, message_id: MessageId) -> Result<bool> {
        self.message_store.contains_message(message_id)
    }

    fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)> + '_> {
        self.message_store.get_upstream_of(message_id)
    }

    fn load_message(&self, message_id: MessageId) -> Result<MessageFrame<MSG>> {
        self.message_store.load_message(message_id)
    }

    fn get_update_heads(&self) -> &[MessageId] {
        self.message_store.get_update_heads()
    }

    fn get_messages_at_or_after(&self, message: MessageId, count: usize) -> Result<Vec<MessageId>> {
        self.message_store.get_messages_after(message, count)
    }

    fn is_acceptable_cutoff_hash(&self, hash: CutOffHashPos) -> Result<Acceptability> {
        self.message_store
            .is_acceptable_cutoff_hash(hash, self.noatun_now())
    }
    fn current_cutoff_state(&self) -> Result<CutOffHashPos> {
        self.message_store.current_cutoff_hash()
    }
    fn current_cutoff_time(&self) -> Result<NoatunTime> {
        self.message_store.current_cutoff_time()
    }
    fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.message_store.get_all_message_ids()
    }
    fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        self.message_store.get_message_children(msg)
    }

    /// Retrieve all messages in the database.
    fn get_all_messages(&self) -> Result<impl Iterator<Item = MessageFrame<MSG>> + use<'_, MSG>> {
        self.message_store.get_all_messages()
    }
    fn get_all_messages_with_children(&self) -> Result<Vec<(MessageFrame<MSG>, Vec<MessageId>)>> {
        self.message_store.get_all_messages_with_children()
    }

    fn with_root_preview<R>(
        &mut self,
        time: DateTime<Utc>,
        preview: impl Iterator<Item = MSG>,
        f: impl FnOnce(&MSG::Root) -> R,
    ) -> Result<R> {
        let current = self.context.next_seqnr();

        self.context.set_next_seqnr(current.successor());
        let root_ptr = self.context.get_root_ptr::<<MSG::Root as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context, false);
        let mut root = unsafe { root_ptr.access_mut::<MSG::Root>() };
        self.message_store
            .apply_preview(time, root.as_mut(), preview)?;
        let ret = f(&*root);
        drop(guard);
        self.message_store.rewind(&mut self.context, current)?;

        Ok(ret)
    }

    /// Note, this method offer very little overhead, but this means it also does not validate
    /// the database before executing. This should always be safe, but it means that recovery
    /// will not occur if the database has been corrupted by a previous operation.
    ///
    /// In normal operation, the database is never corrupted. However, if it somehow is
    /// (by a thread being killed, for example), this method could produce a root object that
    /// is in a state of a message being half-applied, for example.
    pub(crate) fn with_root<R>(&self, f: impl FnOnce(&MSG::Root) -> R) -> R {
        let root_ptr = self.context.get_root_ptr::<<MSG::Root as Object>::Ptr>();
        let _guard = ContextGuard::new(&self.context);
        let root = unsafe { root_ptr.access::<MSG::Root>() };
        f(root)
    }

    pub fn noatun_now(&self) -> NoatunTime {
        self.time_override.unwrap_or_else(NoatunTime::now)
    }

    #[cfg(test)]
    fn with_root_mut<R>(&mut self, f: impl FnOnce(Pin<&mut MSG::Root>) -> R) -> Result<R> {
        let root_ptr = self.context.get_root_ptr::<<MSG::Root as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context, false);
        let root = unsafe { root_ptr.access_mut::<MSG::Root>() };
        let t = f(root);
        drop(guard);

        Ok(t)
    }

    fn set_projection_time_limit(&mut self, limit: NoatunTime) -> Result<()> {
        let index = self.message_store.get_index_of_time(limit)?;
        self.message_store
            .rewind(&mut self.context, SequenceNr::from_index(index))?;

        self.projection_time_limit = Some(limit);

        self.do_apply_missing()?;
        Ok(())
    }

    /// Force a complete rewind and re-application of all messages.
    ///
    /// You should never need this, but it remains publicly visible to ease in
    /// debugging. Since this re-runs all message-applies, it can be used during debugging
    /// of [`crate::Message::apply`].
    fn reproject(&mut self) -> Result<()> {
        self.reproject_from(SequenceNr::from_index(0))?;
        Ok(())
    }
    /// Returns earliest seq deleted (if any)
    fn reproject_from(&mut self, index: SequenceNr) -> Result<Option<SequenceNr>> {
        self.message_store.rewind(&mut self.context, index)?;

        self.do_apply_missing()
    }

    fn do_apply_missing(&mut self) -> Result<Option<SequenceNr>> {
        let root_ptr = self.context.get_root_ptr::<<MSG::Root as Object>::Ptr>();

        let mut earliest = None;

        while let Some(cur_earliest_deleted) = self.message_store.apply_missing_messages(
            unsafe { root_ptr.access_ctx_mut::<MSG::Root>(&mut self.context) },
            &mut self.context,
            self.projection_time_limit,
            self.auto_delete,
        )? {
            earliest = Some(
                earliest
                    .unwrap_or(SequenceNr::default())
                    .max(cur_earliest_deleted),
            );
            info!(
                "Post-apply deletion carried out, rewinding to {}",
                cur_earliest_deleted
            );
            self.message_store
                .rewind(&mut self.context, cur_earliest_deleted)?;
        }
        Ok(earliest)
    }

    #[inline(never)]
    fn recover_impl(
        context: &mut DatabaseContextData,
        message_store: &mut Projector<MSG>,
        settings: &DatabaseSettings,
    ) -> Result<()> {
        context.clear()?;

        message_store.recover(settings.mock_time.unwrap_or_else(NoatunTime::now))?;
        let mmap_ptr = context.start_ptr();
        let guard = ContextGuardMut::new(context, true);
        let root_obj_ref = unsafe { NoatunContext.allocate::<MSG::Root>() };
        let root_ptr = DatabaseContextData::index_of_rel(mmap_ptr, &*root_obj_ref);
        drop(guard);

        context.set_root_ptr(root_ptr.as_generic());
        context.set_next_seqnr(SequenceNr::from_index(0));

        Ok(())
    }

    /// Remove all cache files, retaining only the actual data files.
    /// This should never be needed, but can possibly serve a purpose if
    /// archiving noatun files, since the cache files can always be recreated from the
    /// data files.
    pub fn remove_caches(path: impl AsRef<Path>) -> Result<()> {
        Self::remove_db_files_impl(path, true)
    }
    /// Remove all database files for a database at the given path
    ///
    /// Succeeds if the database was removed, or if it didn't even exist.
    pub fn remove_db_files(path: impl AsRef<Path>) -> Result<()> {
        Self::remove_db_files_impl(path, false)
    }
    /// Remove all database files for a database at the given path
    ///
    /// Succeeds if the database was removed, or if it didn't even exist.
    fn remove_db_files_impl(path: impl AsRef<Path>, only_caches: bool) -> Result<()> {
        let path: PathBuf = path.as_ref().to_path_buf();
        fn remove_if_exists(path: impl AsRef<Path>) -> Result<()> {
            if std::fs::metadata(path.as_ref()).is_ok() {
                std::fs::remove_file(path)?;
            }
            Ok(())
        }

        remove_if_exists(path.join("index.bin"))?;
        remove_if_exists(path.join("maindb.bin"))?;
        remove_if_exists(path.join("undo.bin"))?;
        remove_if_exists(path.join("undo.bin"))?;
        remove_if_exists(path.join("update_head.bin"))?;

        if !only_caches {
            remove_if_exists(path.join("data0.bin"))?;
            remove_if_exists(path.join("data1.bin"))?;
        }

        Ok(())
    }
    /// Note: You can set max_file_size to something very large, like 100_000_000_000.
    /// The max-size is reserved in the process' address space, but not actually allocated
    /// until needed.
    ///
    /// params - Can be anything. Will be provided at initialization time
    pub fn create_new(
        path: impl AsRef<Path>,
        mode: OpenMode,
        settings: DatabaseSettings,
    ) -> Result<Database<MSG>> {
        Self::create(
            if mode.overwrite_existing() {
                Target::CreateNewOrOverwrite(path.as_ref().to_path_buf())
            } else {
                Target::CreateNew(path.as_ref().to_path_buf())
            },
            settings,
        )
    }
    pub fn open(path: impl AsRef<Path>, settings: DatabaseSettings) -> Result<Database<MSG>> {
        Self::create(Target::OpenExisting(path.as_ref().to_path_buf()), settings)
    }

    /// Remove the given message.
    ///
    /// If force == false, don't delete message if it has been transmitted
    fn remove_message(&mut self, message_id: MessageId, force: bool) -> Result<()> {
        if let Some(seq) = self.message_store.remove_message(message_id, force)? {
            self.reproject_from(seq)?;
        }

        Ok(())
    }

    /// Local is true if this message has been locally created. I.e, it isn't a message that
    /// has been received from some other node.
    fn append_single(&mut self, message: &MessageFrame<MSG>, local: bool) -> Result<()> {
        self.append_many(std::iter::once(message), local, true)
    }

    /// Set the current time to the given value.
    /// This does not update the system time, it only affects the time for Noatun.
    ///
    /// This may advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    fn set_mock_time(&mut self, time: NoatunTime) -> Result<()> {
        self.time_override = Some(time);
        self.maybe_advance_cutoff()?;
        Ok(())
    }

    /// Returns true if the message still exists.
    /// If this returns false, the message has been deleted, and *MUST* not *BE* transmitted.
    fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        self.message_store.mark_transmitted(message_id)
    }

    #[inline]
    fn append_local(&mut self, message: MSG) -> Result<MessageHeader> {
        self.append_local_opt(None, message)
    }

    /// This method returns 0 on error.
    fn count_messages(&self) -> usize {
        self.message_store.count_messages()
    }

    fn compact(&mut self) -> Result<()> {
        self.message_store.compact()
    }

    fn append_local_at(&mut self, time: NoatunTime, message: MSG) -> Result<MessageHeader> {
        self.append_local_opt(Some(time), message)
    }

    fn append_many_local_at(
        &mut self,
        time: NoatunTime,
        messages: impl Iterator<Item = MSG>,
    ) -> Result<()> {
        self.append_local_many_opt(Some(time), messages)
    }

    fn create_message_frame(
        &mut self,
        time: Option<NoatunTime>,
        message: MSG,
    ) -> Result<MessageFrame<MSG>> {
        self.create_message_frame_impl(time, message, self.message_store.current_cutoff_time()?)
    }

    fn create_message_frame_impl(
        &mut self,
        time: Option<NoatunTime>,
        message: MSG,
        cutoff_time: NoatunTime,
    ) -> Result<MessageFrame<MSG>> {
        let time = time.unwrap_or_else(|| self.noatun_now());
        let new_id;

        if let Some(prev_local) = self.prev_local {
            if time == prev_local.timestamp() {
                new_id = prev_local.successor();
            } else {
                new_id = MessageId::generate_for_time(time)?;
            }
        } else {
            new_id = MessageId::generate_for_time(time)?;
        }
        self.prev_local = Some(new_id);

        let t = MessageFrame::new(
            new_id,
            if new_id.timestamp() >= cutoff_time {
                self.get_update_heads()
                    .iter()
                    .copied()
                    .filter(|x| *x < new_id)
                    .collect()
            } else {
                vec![]
            },
            message,
        );

        Ok(t)
    }

    fn append_local_opt(
        &mut self,
        time: Option<NoatunTime>,
        message: MSG,
    ) -> Result<MessageHeader> {
        let cutoff_time = self.message_store.current_cutoff_time()?;
        let t = self.create_message_frame_impl(time, message, cutoff_time)?;
        let header = t.header.clone();
        self.append_many_impl(std::iter::once(&t), true, true)?;
        Ok(header)
    }

    // Returns first message header
    fn append_local_many_opt(
        &mut self,
        time: Option<NoatunTime>,
        messages: impl Iterator<Item = MSG>,
    ) -> Result<()> {
        let cutoff_time = self.message_store.current_cutoff_time()?;

        let time = time.unwrap_or_else(|| self.noatun_now());
        let mut temp = Vec::new();

        let mut new_id;
        if let Some(prev_local) = self.prev_local {
            if time == prev_local.timestamp() {
                new_id = prev_local.successor();
            } else {
                new_id = MessageId::generate_for_time(time)?;
            }
        } else {
            new_id = MessageId::generate_for_time(time)?;
        }

        let mut parents: Vec<_> = self
            .get_update_heads()
            .iter()
            .copied()
            .filter(|x| *x < new_id)
            .collect();

        for message in messages {
            self.prev_local = Some(new_id);

            let t = MessageFrame::new(
                new_id,
                if new_id.timestamp() >= cutoff_time {
                    parents.clone()
                } else {
                    vec![]
                },
                message,
            );

            temp.push(t);

            parents.clear();
            parents.push(new_id);
            new_id = new_id.successor();
        }

        self.append_many_impl(temp.iter(), true, true)?;
        Ok(())
    }

    /// For messages before the cutoff-time, all parents are removed.
    fn append_many<'a>(
        &mut self,
        messages: impl Iterator<Item = &'a MessageFrame<MSG>>,
        local: bool,
        allow_cutoff_advance: bool,
    ) -> Result<()> {
        self.append_many_impl(messages, local, allow_cutoff_advance)
    }

    /// For messages before the cutoff-time, all parents are removed.
    fn append_many_impl<'a>(
        &mut self,
        messages: impl Iterator<Item = &'a MessageFrame<MSG>>,
        local: bool,
        allow_cutoff_advance: bool,
    ) -> Result<()> {
        if allow_cutoff_advance {
            self.maybe_advance_cutoff()?;
        }

        self.message_store
            .push_messages(&mut self.context, messages, local)?;

        trace!("apply_missing_messages");

        self.do_apply_missing()?;
        Ok(())
    }
    /// Create a database residing entirely in memory.
    /// This is mostly useful for tests
    pub fn create_in_memory(max_size: usize, settings: DatabaseSettings) -> Result<Database<MSG>> {
        let mut disk = InMemoryDisk::default();
        let target = Target::CreateNew(PathBuf::default());
        let mut ctx = DatabaseContextData::new(&mut disk, &target, max_size)
            .context("creating database in memory")?;
        let mut message_store =
            Projector::new(&mut disk, &target, max_size, settings.cutoff_interval)?;

        Self::recover_impl(&mut ctx, &mut message_store, &settings)?;
        ctx.mark_hot_clean();

        let mut db = Database {
            prev_local: None,
            context: ctx,
            message_store,
            time_override: settings.mock_time,
            projection_time_limit: settings.projection_time_limit,
            load_status: LoadingStatus::NewDatabase,
            auto_delete: settings.auto_prune,
        };
        db.do_apply_missing()?;

        Ok(db)
    }

    fn create(target: Target, settings: DatabaseSettings) -> Result<Database<MSG>> {
        let mut disk = StandardDisk;

        let mut ctx = DatabaseContextData::new(&mut disk, &target, settings.max_file_size)
            .context("opening database")?;

        let is_dirty = ctx.is_dirty();

        let mut message_store = Projector::new(
            &mut disk,
            &target,
            settings.max_file_size,
            settings.cutoff_interval,
        )?;
        let load_status;

        if is_dirty {
            Self::recover_impl(
                &mut ctx,
                &mut message_store,
                &DatabaseSettings {
                    mock_time: settings.mock_time,
                    projection_time_limit: settings.projection_time_limit,
                    auto_prune: settings.auto_prune,
                    ..Default::default()
                },
            )?;
            ctx.mark_hot_clean();
            if !message_store.loaded_existing_db() {
                load_status = LoadingStatus::NewDatabase;
            } else {
                load_status = LoadingStatus::RecoveryPerformed;
            }
        } else {
            if !message_store.loaded_existing_db() {
                load_status = LoadingStatus::NewDatabase;
            } else {
                load_status = LoadingStatus::CleanLoad;
            }
        }

        let mut db = Database {
            prev_local: None,
            context: ctx,
            message_store,
            time_override: settings.mock_time,
            projection_time_limit: settings.projection_time_limit,
            load_status,
            auto_delete: settings.auto_prune,
        };
        db.do_apply_missing()?;
        Ok(db)
    }
    pub fn load_status(&self) -> LoadingStatus {
        self.load_status
    }
}
