//! This module contains the main user-facing database type: [`Database`].
use crate::cutoff::{Acceptability, CutOffDuration, CutOffHashPos, CutOffTime};
use crate::disk_abstraction::{InMemoryDisk, StandardDisk};
use crate::projector::Projector;
use crate::sequence_nr::SequenceNr;
use crate::{
    calculate_schema_hash, ContextGuard, ContextGuardMut, DatabaseContextData, Message,
    MessageFrame, MessageHeader, MessageId, NoatunContext, NoatunTime, Object, Pointer, Target,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use metrics::{counter, describe_counter, Unit};
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::pin::Pin;
use tracing::{error, info, trace, warn};

/// The result of attempting to load a database
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoadingStatus {
    /// A new database was created
    NewDatabase,
    /// An existing database was loaded cleanly
    CleanLoad,
    /// An existing database was loaded, but recovery had to be performed.
    ///
    /// Recovery is needed if the database was not previously closed gracefully.
    RecoveryPerformed,
}

/// Main database object.
///
/// This object implements the main Noatun database functionality.
///
/// Use [`Database::create_new`] to create a database.
///
/// Use [`Database::begin_session_mut`] to start a mutable session to write to the database, using
/// [`DatabaseSessionMut::append_single`] to add a message.
///
/// Use [`Database::begin_session`] to start a read-only session to gain access to the database
/// data using [`DatabaseSession::with_root`].
///
///
#[cfg_attr(
    feature = "tokio",
    doc = "For replication, see [`crate::communication::DatabaseCommunication`]."
)]
///
/// # Consistency
///
/// If thread execution is halted while a mutable Database-operation is running,
/// the Database will enter a "corrupted" state. Clearing this state requires mutable access,
/// and this state can thus not be recovered from within any of the methods accepting `&self`.
///
/// Any access to a method using `&mut self` (and [`Self::recover`] in particular) will
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

    /// The maximum propagation delay in the network.
    ///
    /// Note, this value must take into account delays caused by nodes being offline.
    ///
    /// For example, it may be stipulated that every node in the system must be connected
    /// to some central server at least every 2 weeks. The maximum propagation delay in the
    /// system will then be 4 weeks (worst case: data is written to node A two weeks before
    /// connection, node B is connected just prior to A connecting).
    pub cutoff_interval: CutOffDuration,

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

    /// The size allocated to database size on disks for an empty database.
    ///
    /// The database files will always consume this much space, at least, but may grow.
    /// Increasing this value may increase initial performance, since the files don't
    /// have to be resized as many times.
    pub initial_file_size: usize,

    /// If set to true, data-files will be automatically compacted
    pub auto_compact_enabled: bool,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        DatabaseSettings {
            mock_time: None,
            projection_time_limit: None,
            auto_prune: true,
            max_file_size: 1_000_000_000,
            cutoff_interval: CutOffDuration::from_minutes(15),
            initial_file_size: 4096,
            auto_compact_enabled: true,
        }
    }
}

/// A mutable database session.
///
/// This allows adding new messages to the database.
///
/// Create mutable sessions from an instance of [`Database`], using [`Database::begin_session_mut`] .
pub struct DatabaseSessionMut<'a, MSG: Message> {
    db: &'a mut Database<MSG>,
}

/// A readonly database session.
///
/// Readonly sessions allow reading the materialized view, as well as the messages.
///
/// Create sessions from an instance of [`Database`], using [`Database::begin_session`] .
pub struct DatabaseSession<'a, MSG: Message> {
    db: &'a Database<MSG>,
}

impl<MSG: Message> Drop for DatabaseSessionMut<'_, MSG> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            _ = self.db.mark_hot_clean();
        }
    }
}

/// All stored information about a particular message
pub struct MessageInfo<MSG: Message> {
    /// The current sequence number of this message.
    ///
    /// Sequence numbers change as the database evolves, is compacted etc.
    pub seq: SequenceNr,
    /// The number of pieces of data that this message wrote, that are still
    /// present in the database materialized view
    pub live: u32,
    /// Flags for this message:
    ///
    /// "T": Tainted
    ///
    /// "S": Wrote tombstones
    ///
    /// "N": Non-opaque
    ///
    /// # Tainted
    /// A message that overwrote part of the data written by the current message,
    /// had observed data from the materialized view before doing the overwrite. Since the
    /// materialized view could change if old messages arrives, this overwrite is not guaranteed
    /// to happen.
    ///
    /// # Wrote tombstones
    /// The message deleted data. Such data has no ownership. To protect the message from being
    /// pruned, the message is marked as a tombstone. Tombstones are never deleted until
    /// the cutoff interval elapses, to avoid deleted entries to linger on nodes that were
    /// offline during the deletion.
    ///
    /// # Wrote non-opaque
    /// The message wrote non-opaque data. Such data can be observed by later messages. Even if
    /// the non-opaque data is later overwritten, some other message may arrive that reads the
    /// to-be-overwritten data, and writes it to some second field. This means the overwritten
    /// data could yet be salvaged, and the current message must not be pruned before the cutoff
    /// period has elapsed.
    pub flags: &'static str,
    /// The message frame, with id, parents and payload.
    pub frame: MessageFrame<MSG>,
    /// Owning messages for all data that was read by this message
    pub reads: Vec<SequenceNr>,
    /// All messages whose data was (at least partially) overwritten by this message.
    pub writes: Vec<SequenceNr>,
}

impl<MSG: Message + 'static> DatabaseSession<'_, MSG> {
    /// Returns true if this database contains the given messsage.
    pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
        self.db.contains_message(message_id)
    }

    /// From the provided list, remove all message ids that have a timestamp
    /// before the cutoff period.
    pub fn remove_cutoff_parents(&self, parents: &mut Vec<MessageId>) {
        if let Ok(cutoff) = self.db.current_cutoff_time() {
            parents.retain(|x| x.timestamp() >= cutoff);
        }
    }

    /// Get 'query count' number of messages upstream of each given message.
    ///
    /// The messages upstream of a given message, is the transitive closure given by
    /// the parent relation. That is, all messages that can be reached by following
    /// parent pointers, recursively. In genealogy, it would be all ancestors.
    pub fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)> + '_> {
        self.db.get_upstream_of(message_id)
    }

    /// Load the given message from the store.
    pub fn load_message(&self, message_id: MessageId) -> Result<MessageFrame<MSG>> {
        self.db.load_message(message_id)
    }

    /// Get all current update heads
    ///
    /// Update heads are messages that are not parents to any other message
    pub fn get_update_heads(&self) -> &[MessageId] {
        self.db.get_update_heads()
    }

    /// Get all messages that sort at, or after the given message
    pub fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        self.db.get_messages_at_or_after(message, count)
    }

    /// Return the current cutoff hash and time
    ///
    /// The cutoff time is the time such that all messages created before this time must
    /// have reached all nodes in the network.
    pub fn current_cutoff_state(&self) -> Result<CutOffHashPos> {
        self.db.current_cutoff_state()
    }
    /// Return the current cutoff time
    ///
    /// The cutoff time is the time such that all messages created before this time must
    /// have reached all nodes in the network.
    pub fn current_cutoff_time(&self) -> Result<NoatunTime> {
        self.db.current_cutoff_time()
    }

    /// Get all message ids in the database
    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.db.get_all_message_ids()
    }

    /// Get all children of the given message
    pub fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
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
    /// Retrieve all messages in the system in a Vec
    pub fn get_all_messages_meta_vec(&self) -> Result<Vec<MessageInfo<MSG>>> {
        Ok(self.db.get_all_messages_meta()?.collect())
    }

    /// Get all messages in the database, and their children.
    pub fn get_all_messages_with_children(
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
    /// This method has very little overhead, but this means it also does not validate
    /// the database before executing. This should always be safe, but it means that recovery
    /// will not occur if the database has been corrupted by a previous operation.
    ///
    /// In normal operation, the database is never corrupted. However, if it somehow is
    /// (by a thread being killed, for example), this method could produce a root object
    /// in a state of a message being half-applied, for example.
    ///
    /// This does not advance the cutoff frontier. See [`DatabaseSessionMut::maybe_advance_cutoff`].
    pub fn with_root<R>(&self, f: impl FnOnce(&MSG::Root) -> R) -> R {
        self.db.with_root(f)
    }

    /// Return the current time, as known by this noatun database instance.
    ///
    /// This value is affected by the [`DatabaseSessionMut::set_mock_time`] call.
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
    /// Returns true if this database contains the given message
    pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
        self.db.contains_message(message_id)
    }

    /// Sync any written messages to the disk, and wait for the disk io
    /// to complete. Once this method returns Ok, all messages have been persisted
    /// to disk.
    ///
    /// This will not sync the materialized view to disk, but doing so is not necessary
    /// since it will be rebuilt automatically in case of either operating system or application
    /// crashes.
    pub fn commit(self) -> Result<()> {
        self.db.sync_outstanding()
    }

    /// Trim the given vector, removing all items that have a timestamp
    /// prior to the cutoff time.
    ///
    /// This is a helper intended to prune the parent list of a message.
    /// Message parents can never have a timestamp before the cutoff time, such
    /// parents aren't tracked by noatun.
    ///
    /// All messages that were created before the cutoff time are expected
    /// to have had time to propagate to every node in the network.
    pub fn remove_cutoff_parents(&self, parents: &mut Vec<MessageId>) {
        if let Ok(cutoff) = self.db.current_cutoff_time() {
            parents.retain(|x| x.timestamp() >= cutoff);
        }
    }

    /// Return every message upstream of the given messages, with the given recursion depth.
    ///
    /// Upstream messages are ancestors of a message, based on their list of parents.
    pub(crate) fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)> + '_> {
        self.db.get_upstream_of(message_id)
    }

    /// Load the given message from the database
    pub fn load_message(&self, message_id: MessageId) -> Result<MessageFrame<MSG>> {
        self.db.load_message(message_id)
    }

    /// Get all update heads of this database.
    ///
    /// Update heads are messages that are not the parent of any other message.
    pub fn get_update_heads(&self) -> &[MessageId] {
        self.db.get_update_heads()
    }

    /// Get all messages in the database with a timestamp at or after the given
    /// message. Returns at most 'count' messages.
    pub fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        self.db.get_messages_at_or_after(message, count)
    }

    pub(crate) fn is_acceptable_cutoff_hash(&self, hash: CutOffHashPos) -> Result<Acceptability> {
        self.db.is_acceptable_cutoff_hash(hash)
    }
    #[cfg(test)]
    pub(crate) fn current_cutoff_state(&self) -> Result<CutOffHashPos> {
        self.db.current_cutoff_state()
    }
    /// Return the current cutoff time
    ///
    /// All messages created before the cutoff time are expected to have had enough
    /// time to propagate to every node in the network.
    pub fn current_cutoff_time(&self) -> Result<NoatunTime> {
        self.db.current_cutoff_time()
    }
    /// Get all message ids in the database
    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.db.get_all_message_ids()
    }
    /// Get the children of the given message
    pub(crate) fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        self.db.get_message_children(msg)
    }
    /// Get all messages in the database
    pub fn get_all_messages(
        &self,
    ) -> Result<impl Iterator<Item = MessageFrame<MSG>> + use<'_, MSG>> {
        self.db.get_all_messages()
    }
    /// Get all messages in the database, in a Vec
    pub fn get_all_messages_vec(&self) -> Result<Vec<MessageFrame<MSG>>> {
        Ok(self.db.get_all_messages()?.collect())
    }
    /// Get all messages and their children
    pub fn get_all_messages_with_children(
        &self,
    ) -> Result<Vec<(MessageFrame<MSG>, Vec<MessageId>)>> {
        self.db.get_all_messages_with_children()
    }

    /// Note, this method has very little overhead, but this means it also does not validate
    /// the database before executing. This should always be safe, but it means that recovery
    /// will not occur if the database has been corrupted by a previous operation.
    ///
    /// In normal operation, the database is never corrupted. However, if it somehow is
    /// (by a thread being killed, for example), this method could produce a root object that
    /// is in a state of a message being half-applied, for example.
    ///
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    pub fn with_root<R>(&self, f: impl FnOnce(&MSG::Root) -> R) -> R {
        self.db.with_root(f)
    }

    /// Return the current time, as known by this noatun database instance.
    ///
    /// This time is affected by calls to [`DatabaseSessionMut::set_mock_time`].
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

    /// Apply the messages given by `preview`, then call `f` with a reference to
    /// the resulting materialized view.
    ///
    /// The changes are undone afterwards.
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

    /// Limit the projection of messages on this database to messages before or at 'limit'.
    ///
    /// This can be used to 'travel in time', and observe the database state at an historical
    /// time, with some limitations.
    ///
    /// Specifically, if early pruning has occurred, not all historical context will be available.
    ///
    /// To achieve complete history, it is possible to implement [`Message::persistence`] and return
    /// [`crate::Persistence::AtLeastUntilCutoff`], and then set a very large cutoff_interval (see
    /// field 'cutoff_interval' in [`DatabaseSettings`] ).
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

    /// Compact the message index of this database.
    ///
    /// Unless compaction is disabled by config, you should never need to call this method
    pub fn compact_index(&mut self) -> Result<()> {
        self.db.compact_index()
    }

    /// Set the current time to the given value.
    /// This does not update the system time, it only affects the time for Noatun.
    ///
    /// This may advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    pub fn set_mock_time(&mut self, time: NoatunTime) -> Result<()> {
        self.db.set_mock_time(time)?;
        Ok(())
    }

    /// Set the mock time, but do not advance the cutoff time yet
    ///
    /// The cutoff time is advanced automatically when adding messages and in
    /// certain other cases.
    ///
    /// This method only makes sense for some very specific testing/troubleshooting.
    pub fn set_mock_time_no_advance(&mut self, time: NoatunTime) -> Result<()> {
        self.db.set_mock_time_no_advance(time)?;
        Ok(())
    }

    /// Returns true if the message still exists.
    /// If this returns false, the message has been deleted, and *MUST* not *BE* transmitted.
    ///
    /// Note, this function thus does not fail if the message to mark as transmitted
    /// does not exist, it just returns false.
    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        self.db.mark_transmitted(message_id)
    }

    /// Add a new local message to the database.
    ///
    /// Local messages are messages that are known to not have been observed by any other node.
    ///
    /// This means they can be pruned aggressively, if overwritten by later changes.
    #[inline]
    pub fn append_local(&mut self, message: MSG) -> Result<MessageHeader> {
        self.db.append_local(message)
    }

    /// Add a message that might already have been observed by other nodes.
    #[inline]
    pub fn append_nonlocal(&mut self, message: MSG) -> Result<MessageHeader> {
        self.db.append_nonlocal(message)
    }

    /// Compact the database
    ///
    /// This should never be needed, unless automatic compaction has been disabled by config.
    pub fn compact(&mut self) -> Result<()> {
        self.db.compact()
    }

    /// Add a new local message at the given time.
    ///
    /// Local messages are messages that are known to not have been observed by any other node.
    ///
    /// Backdating messages can be useful, to ensure they are applied to the database before
    /// a certain time.
    pub fn append_local_at(&mut self, time: NoatunTime, message: MSG) -> Result<MessageHeader> {
        self.db.append_local_at(time, message)
    }

    /// Append many local messages at the given time.
    ///
    /// Local messages are messages that are known to not have been observed by any other node.
    ///
    /// Backdating messages can be useful, to ensure they are applied to the database before
    /// a certain time.
    pub fn append_many_local_at(
        &mut self,
        time: NoatunTime,
        messages: impl Iterator<Item = MSG>,
    ) -> Result<()> {
        self.db.append_many_local_at(time, messages)
    }

    /// Create a message frame from the given information.
    ///
    /// If time is None, the databases current time is used.
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

    /// Create a new local message.
    ///
    /// Local messages are messages that are known to not have been observed by any other node.
    ///
    /// Backdating messages can be useful, to ensure they are applied to the database before
    /// a certain time. Provide a time value to override databse time.
    pub fn append_local_opt(
        &mut self,
        time: Option<NoatunTime>,
        message: MSG,
    ) -> Result<MessageHeader> {
        self.db.append_opt(time, message, true)
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
    /// Begin a write transaction
    ///
    /// Each transaction has a significant overhead because of the cost of
    /// syncing disks.
    ///
    /// It is therefore useful to combine as many operations as possible
    /// into each transaction.
    pub fn begin_session_mut(&mut self) -> Result<DatabaseSessionMut<'_, MSG>> {
        self.mark_dirty()?;
        Ok(DatabaseSessionMut { db: self })
    }

    /// Sync all writes to disk
    ///
    /// This will update both file data and file metadata
    pub fn sync_all(&mut self) -> Result<()> {
        self.message_store.sync_all()?;
        self.context.sync_all()?;
        self.mark_fully_clean()?;
        Ok(())
    }

    /// Begin a readonly database session.
    ///
    ///
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

    fn sync_outstanding(&mut self) -> Result<()> {
        self.message_store.sync_outstanding()
    }

    fn mark_hot_clean(&mut self) -> Result<()> {
        match self.message_store.sync_outstanding() {
            Ok(()) => {
                self.context.mark_hot_clean();
                Ok(())
            }
            Err(err) => {
                warn!("Error while syncing outstanding messages: {:?}", err);
                Err(err)
            }
        }
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
    pub fn recover(&mut self) -> Result<()> {
        if self.context.is_dirty() {
            self.do_recovery()?;
            self.mark_hot_clean()?;
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

    /// Set noatun to abort on any panicking message.
    ///
    /// The default is to catch panics and log an error.
    ///
    /// Aborting can be useful when debugging, as it can sometimes
    /// be useful to stop _immediately_ at the first error.
    pub fn set_abort_on_panic(&mut self) {
        self.message_store.set_abort_on_panic();
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
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn get_all_messages(&self) -> Result<impl Iterator<Item = MessageFrame<MSG>> + use<'_, MSG>> {
        self.message_store.get_all_messages()
    }
    fn get_all_messages_meta(
        &self,
    ) -> Result<impl Iterator<Item = MessageInfo<MSG>> + use<'_, MSG>> {
        self.message_store.get_all_messages_meta(&self.context)
    }

    fn get_all_messages_with_children(&self) -> Result<Vec<(MessageFrame<MSG>, Vec<MessageId>)>> {
        self.message_store.get_all_messages_with_children()
    }

    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
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
        // Safety: root_ptr is valid
        let mut root = unsafe { root_ptr.access_mut::<MSG::Root>() };
        self.message_store
            .apply_preview(time, root.as_mut(), preview)?;
        let ret = f(&*root);
        drop(guard);
        self.message_store.rewind(&mut self.context, current)?;

        Ok(ret)
    }

    /// Note, this method has very little overhead, but this means it also does not validate
    /// the database before executing. This should always be safe, but it means that recovery
    /// will not occur if the database has been corrupted by a previous operation.
    ///
    /// In normal operation, the database is never corrupted. However, if it somehow is
    /// (by a thread being killed, for example), this method could produce a root object that
    /// is in a state of a message being half-applied.
    ///
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    pub(crate) fn with_root<R>(&self, f: impl FnOnce(&MSG::Root) -> R) -> R {
        let root_ptr = self.context.get_root_ptr::<<MSG::Root as Object>::Ptr>();
        let _guard = ContextGuard::new(&self.context);
        // Safety: The root ptr is valid
        let root = unsafe { root_ptr.access::<MSG::Root>() };
        f(root)
    }

    /// Return this database's current time.
    ///
    /// If time has not been explicitly overridden by calling [`DatabaseSessionMut::set_mock_time`],
    /// this will return current system time.
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
    ///
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn reproject(&mut self) -> Result<()> {
        self.reproject_from(SequenceNr::from_index(0))?;
        Ok(())
    }
    /// Returns earliest seq deleted (if any)
    fn reproject_from(&mut self, index: SequenceNr) -> Result<Option<SequenceNr>> {
        self.message_store.rewind(&mut self.context, index)?;

        self.do_apply_missing()
    }
    fn compact_index(&mut self) -> Result<()> {
        self.message_store.compact_index()?;
        Ok(())
    }
    fn do_apply_missing(&mut self) -> Result<Option<SequenceNr>> {
        let root_ptr = self.context.get_root_ptr::<<MSG::Root as Object>::Ptr>();

        let mut earliest = None;

        while let Some(cur_earliest_deleted) = self.message_store.apply_missing_messages(
            // Safety: root_ptr is valid
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
        context.clear(calculate_schema_hash::<MSG::Root>())?;

        message_store.recover(settings.mock_time.unwrap_or_else(NoatunTime::now))?;
        let mmap_ptr = context.start_ptr();
        let guard = ContextGuardMut::new(context, true);
        // Safety: We tie the lifetime to this function
        let root_obj_ref = unsafe { NoatunContext.allocate::<MSG::Root>() };
        let root_ptr = DatabaseContextData::index_of_rel(mmap_ptr, &*root_obj_ref);
        drop(guard);

        context.set_root_ptr(root_ptr.as_generic());
        context.set_next_seqnr(SequenceNr::from_index(0));

        Ok(())
    }

    /// Remove all cache files, retaining only the actual data files.
    ///
    /// This should never be needed, but can possibly serve a purpose if
    /// archiving noatun files, since the cache files can always be recreated from the
    /// data files.
    pub fn remove_caches(path: impl AsRef<Path>) -> Result<()> {
        Self::remove_db_files_impl(path, true)
    }
    /// Remove all database files for a database at the given path
    ///
    /// Succeeds if the database was removed, or if it didn't even exist.
    /// This is mostly useful to clean up integration tests.
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

    /// Remove the given message.
    ///
    /// If force == false, don't delete message if it has been transmitted
    ///
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn remove_message(&mut self, message_id: MessageId, force: bool) -> Result<()> {
        if let Some(seq) = self.message_store.remove_message(message_id, force)? {
            self.reproject_from(seq)?;
        }

        Ok(())
    }

    /// Local is true if this message has been locally created. I.e, it isn't a message that
    /// has been received from some other node.
    ///
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn append_single(&mut self, message: &MessageFrame<MSG>, local: bool) -> Result<()> {
        self.append_many(std::iter::once(message), local, true)
    }

    /// Set the current time to the given value.
    /// This does not update the system time, it only affects the time for Noatun.
    ///
    /// This may advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    ///
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn set_mock_time(&mut self, time: NoatunTime) -> Result<()> {
        self.time_override = Some(time);
        self.maybe_advance_cutoff()?;
        Ok(())
    }

    /// Set the current time to the given value.
    ///
    /// This does not advance the cutoff frontier. See [`Self::maybe_advance_cutoff`].
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn set_mock_time_no_advance(&mut self, time: NoatunTime) -> Result<()> {
        self.time_override = Some(time);
        Ok(())
    }

    /// Returns true if the message still exists.
    /// If this returns false, the message has been deleted, and *MUST* not *BE* transmitted.
    fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        self.message_store.mark_transmitted(message_id)
    }

    #[inline]
    fn append_local(&mut self, message: MSG) -> Result<MessageHeader> {
        self.append_opt(None, message, true)
    }

    #[inline]
    fn append_nonlocal(&mut self, message: MSG) -> Result<MessageHeader> {
        self.append_opt(None, message, false)
    }

    /// Return the number of messages in the system.
    /// This method returns 0 on error.
    pub fn count_messages(&self) -> usize {
        self.message_store.count_messages()
    }

    fn compact(&mut self) -> Result<()> {
        self.message_store.compact()
    }

    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn append_local_at(&mut self, time: NoatunTime, message: MSG) -> Result<MessageHeader> {
        self.append_opt(Some(time), message, true)
    }

    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn append_many_local_at(
        &mut self,
        time: NoatunTime,
        messages: impl Iterator<Item = MSG>,
    ) -> Result<()> {
        self.append_local_many_opt(Some(time), messages)
    }

    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
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

    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn append_opt(
        &mut self,
        time: Option<NoatunTime>,
        message: MSG,
        local: bool,
    ) -> Result<MessageHeader> {
        let cutoff_time = self.message_store.current_cutoff_time()?;
        let t = self.create_message_frame_impl(time, message, cutoff_time)?;
        let header = t.header.clone();
        self.append_many_impl(std::iter::once(&t), local, true)?;
        Ok(header)
    }

    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
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
    ///
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
    fn append_many<'a>(
        &mut self,
        messages: impl Iterator<Item = &'a MessageFrame<MSG>>,
        local: bool,
        allow_cutoff_advance: bool,
    ) -> Result<()> {
        self.append_many_impl(messages, local, allow_cutoff_advance)
    }

    /// For messages before the cutoff-time, all parents are removed.
    ///
    /// This method is not public, please call `Self::begin_session` or `Self::begin_session_mut`
    /// and use a method on the returned session instead.
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

        trace!(next=?self.context.next_seqnr(),"apply_missing_messages");

        self.do_apply_missing()?;
        Ok(())
    }

    /// Create a database residing entirely in memory.
    /// This is mostly useful for tests
    pub fn create_in_memory(max_size: usize, settings: DatabaseSettings) -> Result<Database<MSG>> {
        let mut disk = InMemoryDisk::default();
        let target = Target::CreateNew(PathBuf::default());
        let mut ctx = DatabaseContextData::new(
            &mut disk,
            &target,
            settings.initial_file_size,
            max_size,
            calculate_schema_hash::<MSG::Root>(),
        )
        .context("creating database in memory")?;
        let mut message_store = Projector::new(
            &mut disk,
            &target,
            settings.initial_file_size,
            max_size,
            settings.cutoff_interval,
            settings.auto_compact_enabled,
        )?;

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

        let mut ctx = DatabaseContextData::new(
            &mut disk,
            &target,
            settings.initial_file_size,
            settings.max_file_size,
            calculate_schema_hash::<MSG::Root>(),
        )
        .context("opening database")?;

        let main_db_dirty = counter!("main_db_dirty");
        describe_counter!(
            "main_db_dirty",
            Unit::Count,
            "Main DB was dirty, and had to be rebuilt at start"
        );
        let main_db_schema_change = counter!("main_db_schema_change");
        describe_counter!(
            "main_db_schema_change",
            Unit::Count,
            "Main DB schema changed"
        );

        let ctx_dirty = ctx.is_dirty();
        let wrong_version = ctx.is_wrong_version(calculate_schema_hash::<MSG::Root>());
        if ctx_dirty {
            main_db_dirty.increment(1);
        }
        if wrong_version {
            main_db_schema_change.increment(1);
        }
        let is_dirty = ctx_dirty || wrong_version;

        let mut message_store = Projector::new(
            &mut disk,
            &target,
            settings.initial_file_size,
            settings.max_file_size,
            settings.cutoff_interval,
            settings.auto_compact_enabled,
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

    /// Get the loading status of this database.
    ///
    /// This gives insight into whether recovery occurred, for example.
    pub fn load_status(&self) -> LoadingStatus {
        self.load_status
    }
}
