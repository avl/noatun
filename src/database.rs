use crate::cutoff::{Acceptability, CutOffDuration, CutOffHashPos, CutOffTime};
use crate::disk_abstraction::{InMemoryDisk, StandardDisk};
use crate::projector::Projector;
use crate::sequence_nr::SequenceNr;
use crate::{
    Application, ContextGuard, ContextGuardMut, DatabaseContextData, MessageFrame, MessageHeader,
    MessageId, NoatunContext, NoatunTime, Object, Pointer, Target,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tracing::info;

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
pub struct Database<Base: Application> {
    context: DatabaseContextData,
    message_store: Projector<Base>,
    // Most recently generated local id, or all zeroes.
    // Future local id's will always be greater than this.
    prev_local: Option<MessageId>,
    time_override: Option<NoatunTime>,
    projection_time_limit: Option<NoatunTime>,
    params: Base::Params,
    load_status: LoadingStatus,
    auto_delete: bool,
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
    pub auto_delete: bool,
}
impl Default for DatabaseSettings {
    fn default() -> Self {
        DatabaseSettings {
            mock_time: None,
            projection_time_limit: None,
            auto_delete: true,
        }
    }
}

impl<APP: Application> Database<APP> {
    fn assert_not_dirty(&self) -> Result<()> {
        if self.context.is_dirty() {
            bail!("Database is in a corrupted state. Call Self::recovery, or restart the application, to recover");
        }
        Ok(())
    }

    #[inline]
    fn mark_clean(&mut self) -> Result<()> {
        self.context.mark_clean()
    }
    fn mark_dirty(&mut self) -> Result<()> {
        if !self.context.mark_dirty()? {
            // Recovery needed
            self.do_recovery()?;
        }
        Ok(())
    }
    #[inline(never)]
    fn do_recovery(&mut self) -> Result<()> {
        let now = self.noatun_now();
        Self::recover_impl(
            &mut self.context,
            &mut self.message_store,
            &DatabaseSettings {
                mock_time: Some(now),
                projection_time_limit: self.projection_time_limit,
                auto_delete: self.auto_delete,
            },
            &self.params,
        )
    }

    /// Recover database if in corrupted state.
    /// This method is very fast in the case where the database is not corrupted.
    pub fn recover(&mut self) -> Result<()> {
        if self.context.is_dirty() {
            self.do_recovery()?;
            self.mark_clean()?;
        }
        Ok(())
    }

    /// Explicitly check if enough time has passed to be able to auto_delete some messages.
    ///
    /// Messages that are older than the cutoff period, are assumed to have reached all nodes.
    /// Specifically, it is assumed that no message will ever arrive with a time stamp before
    /// the cutoff time.
    ///
    /// This means that messages that have no current visible effects, can be considered
    /// definitely stale and can be removed. This method will delete messages if possible.
    pub(crate) fn maybe_advance_cutoff(&mut self) -> Result<()> {
        if !self.auto_delete {
            return Ok(());
        }
        // TODO: Do we need to check for dirty here? Probably not, but then we should
        // stop checking for dirty in things like append_many. It should be enough to
        // check on construction, right?
        let now = self.noatun_now();
        let nominal_cutoff_time = self.message_store.nominal_cutoff_time(now);
        let current_cutoff = self.message_store.current_cutoff_hash()?;
        if nominal_cutoff_time > current_cutoff.before_time {
            self.advance_cutoff(nominal_cutoff_time)?;
        }
        Ok(())
    }

    /// This disables filesystem write back. Write-back will still occur, but a power-cut
    /// or unclean operating system shut down can cause newly written messages to be lost.
    pub fn disable_filesystem_sync(&mut self) -> Result<()> {
        self.recover()?;
        self.message_store.disable_filesystem_sync();
        self.context.disable_filesystem_sync();
        Ok(())
    }

    pub(crate) fn advance_cutoff(&mut self, new_cutoff: CutOffTime) -> Result<()> {
        if !self.auto_delete {
            return Ok(());
        }
        self.message_store
            .advance_cutoff(new_cutoff, &mut self.context)
    }

    pub(crate) fn force_rewind(&mut self, index: SequenceNr) {
        self.context.rewind(index)
    }

    pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
        self.assert_not_dirty()?;
        self.message_store.contains_message(message_id)
    }

    pub(crate) fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)> + '_> {
        self.message_store.get_upstream_of(message_id)
    }

    pub fn load_message(&self, message_id: MessageId) -> Result<MessageFrame<APP::Message>> {
        self.assert_not_dirty()?;
        self.message_store.load_message(message_id)
    }

    pub(crate) fn get_update_heads(&self) -> &[MessageId] {
        self.message_store.get_update_heads()
    }

    pub(crate) fn get_messages_at_or_after(
        &self,
        message: MessageId,
        count: usize,
    ) -> Result<Vec<MessageId>> {
        self.message_store.get_messages_after(message, count)
    }

    pub(crate) fn is_acceptable_cutoff_hash(&self, hash: CutOffHashPos) -> Result<Acceptability> {
        self.message_store.is_acceptable_cutoff_hash(hash)
    }
    pub(crate) fn current_cutoff_state(&self) -> Result<CutOffHashPos> {
        self.message_store.current_cutoff_hash()
    }
    pub(crate) fn current_cutoff_time(&self) -> Result<NoatunTime> {
        self.message_store.current_cutoff_time()
    }
    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.assert_not_dirty()?;
        self.message_store.get_all_message_ids()
    }
    pub(crate) fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        self.message_store.get_message_children(msg)
    }
    pub fn get_all_messages(&self) -> Result<Vec<MessageFrame<APP::Message>>> {
        self.assert_not_dirty()?;
        self.message_store.get_all_messages()
    }
    pub(crate) fn get_all_messages_with_children(
        &self,
    ) -> Result<Vec<(MessageFrame<APP::Message>, Vec<MessageId>)>> {
        self.message_store.get_all_messages_with_children()
    }

    pub fn with_root_preview<R>(
        &mut self,
        time: DateTime<Utc>,
        preview: impl Iterator<Item = APP::Message>,
        f: impl FnOnce(&APP) -> R,
    ) -> Result<R> {
        self.mark_dirty()?;

        let current = self.context.next_seqnr();

        self.context.set_next_seqnr(current.successor());
        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let mut root = unsafe { root_ptr.access_mut::<APP>() };
        self.message_store
            .apply_preview(time, root.as_mut(), preview)?;
        let ret = f(&*root);
        drop(guard);
        self.message_store.rewind(&mut self.context, current)?;

        self.mark_clean()?;
        Ok(ret)
    }

    /// Note, this method offer very little overhead, but this means it also does not validate
    /// the database before executing. This should always be safe, but it means that recovery
    /// will not occur if the database has been corrupted by a previous operation.
    ///
    /// In normal operation, the database is never corrupted. However, if it somehow is
    /// (by a thread being killed, for example), this method could produce a root object that
    /// is in a state of a message being half-applied, for example.
    pub fn with_root<R>(&self, f: impl FnOnce(&APP) -> R) -> R {
        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let _guard = ContextGuard::new(&self.context);
        let root = unsafe { root_ptr.access::<APP>() };
        f(root)
    }

    pub(crate) fn noatun_now(&self) -> NoatunTime {
        self.time_override.unwrap_or_else(NoatunTime::now)
    }

    pub(crate) fn with_root_mut<R>(&mut self, f: impl FnOnce(Pin<&mut APP>) -> R) -> Result<R> {
        let now = self.noatun_now();
        if !self.context.mark_dirty()? {
            // Recovery needed
            Self::recover_impl(
                &mut self.context,
                &mut self.message_store,
                &DatabaseSettings {
                    mock_time: Some(now),
                    projection_time_limit: self.projection_time_limit,
                    auto_delete: self.auto_delete,
                },
                &self.params,
            )?;
        }

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { root_ptr.access_mut::<APP>() };
        let t = f(root);
        drop(guard);
        self.context.mark_clean()?;

        Ok(t)
    }

    pub fn set_projection_time_limit(&mut self, limit: NoatunTime) -> Result<()> {
        self.mark_dirty()?;

        let index = self.message_store.get_index_of_time(limit)?;
        self.message_store
            .rewind(&mut self.context, SequenceNr::from_index(index))?;

        self.projection_time_limit = Some(limit);

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();

        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { root_ptr.access_mut::<APP>() };
        drop(guard);

        self.message_store.apply_missing_messages(
            &mut self.context,
            unsafe { root.get_unchecked_mut() },
            self.projection_time_limit,
            self.auto_delete,
        )?;

        self.mark_clean()?;
        Ok(())
    }

    /// Force a complete rewind and re-application of all messages.
    ///
    /// You should never need this, but it remains publicly visible to ease in
    /// debugging. Since this re-runs all message-applies, it can be used during debugging
    /// of [`crate::Message::apply`].
    pub fn reproject(&mut self) -> Result<()> {
        self.mark_dirty()?;
        self.reproject_from(SequenceNr::from_index(0))?;
        self.mark_clean()?;
        Ok(())
    }
    /// Returns earliest seq deleted (if any)
    fn reproject_from(&mut self, index: SequenceNr) -> Result<Option<SequenceNr>> {
        self.message_store.rewind(&mut self.context, index)?;

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { root_ptr.access_mut::<APP>() };
        drop(guard);

        let any_deletes = self.message_store.apply_missing_messages(
            &mut self.context,
            unsafe { root.get_unchecked_mut() },
            self.projection_time_limit,
            self.auto_delete,
        )?;

        Ok(any_deletes)
    }

    #[inline(never)]
    fn recover_impl(
        context: &mut DatabaseContextData,
        message_store: &mut Projector<APP>,
        settings: &DatabaseSettings,
        params: &APP::Params,
    ) -> Result<()> {
        context.clear()?;

        message_store.recover(settings.mock_time.unwrap_or_else(NoatunTime::now))?;
        let mmap_ptr = context.start_ptr();
        let guard = ContextGuardMut::new(context);
        let mut root_obj_ref = unsafe { NoatunContext.allocate::<APP>() };
        APP::initialize_root(root_obj_ref.as_mut(), params);
        let root_ptr = DatabaseContextData::index_of_rel(mmap_ptr, &*root_obj_ref);
        drop(guard);

        context.set_root_ptr(root_ptr.as_generic());
        context.set_next_seqnr(SequenceNr::from_index(0));

        // Safety:
        // Recover is only called when the db is not used
        let guard = ContextGuardMut::new(context);
        let root = unsafe { root_ptr.access_mut::<APP>() };
        drop(guard);
        //let root = context.access_pod(root_ptr);
        message_store.apply_missing_messages(
            context,
            unsafe { root.get_unchecked_mut() },
            settings.projection_time_limit,
            settings.auto_delete,
        )?;

        Ok(())
    }

    /// Remove all cache files, retaining only the actual data files.
    /// This should never be needed, but can possibly serve a purpose if
    /// archiving noatun files, since the cache files can always be recreated from the
    /// data files.
    pub fn remove_caches(path: impl AsRef<Path>) -> Result<()> {
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
        remove_if_exists(path.join("update_head.bin"))?;

        Ok(())
    }

    /// Note: You can set max_file_size to something very large, like 100_000_000_000.
    /// The max-size is reserved in the process' address space, but not actually allocated
    /// until needed.
    pub fn create_new(
        path: impl AsRef<Path>,
        overwrite_existing: bool,
        max_file_size: usize,
        cutoff_interval: CutOffDuration,
        settings: DatabaseSettings,
        params: APP::Params,
    ) -> Result<Database<APP>> {
        Self::create(
            if overwrite_existing {
                Target::CreateNewOrOverwrite(path.as_ref().to_path_buf())
            } else {
                Target::CreateNew(path.as_ref().to_path_buf())
            },
            max_file_size,
            cutoff_interval,
            settings,
            params,
        )
    }
    pub fn open(
        path: impl AsRef<Path>,
        max_file_size: usize,
        cutoff_interval: CutOffDuration,
        settings: DatabaseSettings,
        params: APP::Params,
    ) -> Result<Database<APP>> {
        Self::create(
            Target::OpenExisting(path.as_ref().to_path_buf()),
            max_file_size,
            cutoff_interval,
            settings,
            params,
        )
    }

    /// Local is true if this message has been locally created. I.e, it isn't a message that
    /// has been received from some other node.
    pub fn append_single(
        &mut self,
        message: &MessageFrame<APP::Message>,
        local: bool,
    ) -> Result<()> {
        self.append_many(std::iter::once(message), local, true)
    }

    /// Set the current time to the given value.
    /// This does not update the system time, it only affects the time for Noatun.
    ///
    /// This does not trigger any rewinding of the database, current time is only used
    /// when automatically advancing the cutoff frontier. This only happens during recovery
    /// (initialization) and when adding more messages.
    pub fn set_mock_time(&mut self, time: NoatunTime) -> Result<()> {
        self.recover()?;
        self.time_override = Some(time);
        Ok(())
    }

    /// Returns true if the message still exists.
    /// If this returns false, the message has been deleted, and *MUST* not *BE* transmitted.
    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        self.recover()?;
        self.message_store.mark_transmitted(message_id)
    }

    #[inline]
    pub fn append_local(&mut self, message: APP::Message) -> Result<MessageHeader> {
        self.append_local_opt(None, message)
    }

    pub fn count_messages(&self) -> usize {
        self.message_store.count_messages()
    }

    pub(crate) fn compact(&mut self) -> Result<()> {
        self.recover()?;
        self.message_store.compact()
    }
    pub fn append_local_at(
        &mut self,
        time: NoatunTime,
        message: APP::Message,
    ) -> Result<MessageHeader> {
        self.append_local_opt(Some(time), message)
    }

    pub fn create_message_frame(
        &mut self,
        time: Option<NoatunTime>,
        message: APP::Message,
    ) -> Result<MessageFrame<APP::Message>> {
        self.create_message_frame_impl(time, message, self.message_store.current_cutoff_time()?)
    }

    fn create_message_frame_impl(
        &mut self,
        time: Option<NoatunTime>,
        message: APP::Message,
        cutoff_time: NoatunTime,
    ) -> Result<MessageFrame<APP::Message>> {
        let time = time.unwrap_or_else(|| self.noatun_now());
        let new_id;

        if let Some(prev_local) = self.prev_local {
            if time == prev_local.timestamp() {
                //TODO: Fix all cases of u64 timestamps. We should probably just use i64 instead
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
    pub fn append_local_opt(
        &mut self,
        time: Option<NoatunTime>,
        message: APP::Message,
    ) -> Result<MessageHeader> {
        let t = self.create_message_frame(time, message)?;
        let header = t.header.clone();
        self.append_single(&t, true)?;
        Ok(header)
    }

    /// For messages before the cutoff-time, all parents are removed.
    pub fn append_many<'a>(
        &mut self,
        messages: impl Iterator<Item = &'a MessageFrame<APP::Message>>,
        local: bool,
        allow_cutoff_advance: bool,
    ) -> Result<()> {
        self.mark_dirty()?;

        if allow_cutoff_advance {
            self.maybe_advance_cutoff()?;
        }

        self.message_store
            .push_messages(&mut self.context, messages, local)?;

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { root_ptr.access_mut::<APP>() };
        drop(guard);
        info!("apply_missing_messages");
        let mut earliest_deleted = self.message_store.apply_missing_messages(
            &mut self.context,
            unsafe { root.get_unchecked_mut() },
            self.projection_time_limit,
            self.auto_delete,
        )?;

        while let Some(cur_earliest_deleted) = earliest_deleted {
            info!("Post-apply deletion carried out");
            // TODO: If this loop works, see where else it's needed
            //println!("Post add reproject bc delete: {:?}", cur_earliest_deleted);
            earliest_deleted = self.reproject_from(cur_earliest_deleted)?;
        }

        self.mark_clean()?;
        Ok(())
    }

    /// Create a database residing entirely in memory.
    /// This is mostly useful for tests
    pub fn create_in_memory(
        max_size: usize,
        cutoff_interval: CutOffDuration,
        settings: DatabaseSettings,
        params: APP::Params,
    ) -> Result<Database<APP>> {
        let mut disk = InMemoryDisk::default();
        let target = Target::CreateNew(PathBuf::default());
        let mut ctx = DatabaseContextData::new(&mut disk, &target, max_size)
            .context("creating database in memory")?;
        let mut message_store = Projector::new(&mut disk, &target, max_size, cutoff_interval)?;

        Self::recover_impl(&mut ctx, &mut message_store, &settings, &params)?;
        ctx.mark_clean()?;

        Ok(Database {
            prev_local: None,
            context: ctx,
            message_store,
            time_override: settings.mock_time,
            projection_time_limit: settings.projection_time_limit,
            params,
            load_status: LoadingStatus::NewDatabase,
            auto_delete: settings.auto_delete,
        })
    }

    fn create(
        target: Target,
        max_file_size: usize,
        cutoff_interval: CutOffDuration,
        settings: DatabaseSettings,
        params: APP::Params,
    ) -> Result<Database<APP>> {
        let mut disk = StandardDisk;

        let mut ctx = DatabaseContextData::new(&mut disk, &target, max_file_size)
            .context("opening database")?;

        let is_dirty = ctx.is_dirty();

        let mut message_store = Projector::new(&mut disk, &target, max_file_size, cutoff_interval)?;
        let load_status;
        //let update_heads = disk.open_file(&target, "update_heads", 0, 128 * 1024 * 1024)?;
        println!("Load, is dirty: {:?}", is_dirty);
        if is_dirty {
            Self::recover_impl(
                &mut ctx,
                &mut message_store,
                &DatabaseSettings {
                    mock_time: settings.mock_time,
                    projection_time_limit: settings.projection_time_limit,
                    auto_delete: settings.auto_delete,
                },
                &params,
            )?;
            ctx.mark_clean()?;
            if !message_store.loaded_existing_db() {
                load_status = LoadingStatus::NewDatabase;
            } else {
                load_status = LoadingStatus::RecoveryPerformed;
                //println!("Load status: {:?}", load_status);
            }
        } else {
            if !message_store.loaded_existing_db() {
                load_status = LoadingStatus::NewDatabase;
            } else {
                load_status = LoadingStatus::CleanLoad;
            }
        }
        //println!("Load status: {:?}", load_status);
        Ok(Database {
            params,
            prev_local: None,
            context: ctx,
            message_store,
            time_override: None,
            projection_time_limit: settings.projection_time_limit,
            load_status,
            auto_delete: settings.auto_delete,
        })
    }
    pub fn load_status(&self) -> LoadingStatus {
        self.load_status
    }
}
