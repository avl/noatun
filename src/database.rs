use crate::cutoff::{Acceptability, CutOffDuration, CutOffHashPos, CutOffTime};
use crate::disk_abstraction::{InMemoryDisk, StandardDisk};
use crate::projector::Projector;
use crate::sequence_nr::SequenceNr;
use crate::{
    Application, ContextGuard, ContextGuardMut, DatabaseContextData, Message, MessageHeader,
    MessageId, NoatunTime, Object, Pointer, Target,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::pin::Pin;

#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub enum LoadingStatus {
    NewDatabase,
    CleanLoad,
    RecoveryPerformed
}

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
}

impl<APP: Application> Database<APP> {


    /// TODO: Document
    pub fn maybe_advance_cutoff(&mut self) -> Result<()> {
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
    pub fn disable_filesystem_sync(&mut self)  {
        self.message_store.disable_filesystem_sync();
        self.context.disable_filesystem_sync();
    }

    pub fn advance_cutoff(&mut self, new_cutoff: CutOffTime) -> Result<()> {
        self.message_store
            .advance_cutoff(new_cutoff, &mut self.context)
    }

    pub(crate) fn force_rewind(&mut self, index: SequenceNr) {
        self.context.rewind(index)
    }

    pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
        self.message_store.contains_message(message_id)
    }

    pub fn get_upstream_of(
        &self,
        message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
    ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)> + '_> {
        self.message_store.get_upstream_of(message_id)
    }

    pub fn load_message(&self, message_id: MessageId) -> Result<Message<APP::Message>> {
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

    pub fn is_acceptable_cutoff_hash(&self, hash: CutOffHashPos) -> Result<Acceptability> {
        self.message_store.is_acceptable_cutoff_hash(hash)
    }
    pub fn current_cutoff_state(&self) -> Result<CutOffHashPos> {
        self.message_store.current_cutoff_hash()
    }

    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.message_store.get_all_message_ids()
    }
    pub fn get_message_children(&self, msg: MessageId) -> Result<Vec<MessageId>> {
        self.message_store.get_message_children(msg)
    }
    pub fn get_all_messages(&self) -> Result<Vec<Message<APP::Message>>> {
        self.message_store.get_all_messages()
    }
    pub fn get_all_messages_with_children(
        &self,
    ) -> Result<Vec<(Message<APP::Message>, Vec<MessageId>)>> {
        self.message_store.get_all_messages_with_children()
    }

    pub fn with_root_preview<R>(
        &mut self,
        time: DateTime<Utc>,
        preview: impl Iterator<Item = APP::Message>,
        f: impl FnOnce(&APP) -> R,
    ) -> Result<R> {
        let now = self.noatun_now();
        if !self.context.mark_dirty()? {
            // Recovery needed
            Self::recover(
                &mut self.context,
                &mut self.message_store,
                now,
                self.projection_time_limit,
                &self.params,
            )?;
        }

        let current = self.context.next_seqnr();

        self.context.set_next_seqnr(current.successor());
        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let mut root = unsafe { <APP as Object>::access_mut(root_ptr) };
        self.message_store
            .apply_preview(time, root.as_mut(), preview)?;
        let ret = f(&*root);
        drop(guard);
        self.message_store
            .rewind(&mut self.context, current.index())?;
        self.context.mark_clean()?;
        Ok(ret)
    }

    pub fn with_root<R>(&self, f: impl FnOnce(&APP) -> R) -> R {
        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let _guard = ContextGuard::new(&self.context);
        let root = unsafe { <APP as Object>::access(root_ptr) };
        f(root)
    }

    pub(crate) fn noatun_now(&self) -> NoatunTime {
        self.time_override.unwrap_or_else(NoatunTime::now)
    }

    pub(crate) fn with_root_mut<R>(&mut self, f: impl FnOnce(Pin<&mut APP>) -> R) -> Result<R> {
        let now = self.noatun_now();
        if !self.context.mark_dirty()? {
            // Recovery needed
            Self::recover(
                &mut self.context,
                &mut self.message_store,
                now,
                self.projection_time_limit,
                &self.params,
            )?;
        }

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { <APP as Object>::access_mut(root_ptr) };
        let t = f(root);
        drop(guard);
        self.context.mark_clean()?;

        Ok(t)
    }

    pub fn set_projection_time_limit(&mut self, limit: NoatunTime) -> Result<()> {
        let now = self.noatun_now();
        // TODO: Remove duplication. There are many methods that have exact this
        // code
        if !self.context.mark_dirty()? {
            // Recovery needed
            Self::recover(
                &mut self.context,
                &mut self.message_store,
                now,
                self.projection_time_limit,
                &self.params,
            )?;
        }

        let index = self.message_store.get_index_of_time(limit)?;
        self.message_store.rewind(&mut self.context, index)?;

        self.projection_time_limit = Some(limit);

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();

        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { <APP as Object>::access_mut(root_ptr) };
        drop(guard);

        self.message_store.apply_missing_messages(
            &mut self.context,
            unsafe { root.get_unchecked_mut() },
            self.projection_time_limit,
        )?;

        self.context.mark_clean()?;
        Ok(())
    }

    pub fn reproject(&mut self) -> Result<()> {
        let now = self.noatun_now();
        // TODO: Reduce code duplication - mark_dirty etc exists in many methods
        if !self.context.mark_dirty()? {
            // Recovery needed
            Self::recover(
                &mut self.context,
                &mut self.message_store,
                now,
                self.projection_time_limit,
                &self.params,
            )?;
        }

        self.message_store.rewind(&mut self.context, 0)?;

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { <APP as Object>::access_mut(root_ptr) };
        drop(guard);

        self.message_store.apply_missing_messages(
            &mut self.context,
            unsafe { root.get_unchecked_mut() },
            self.projection_time_limit,
        )?;

        self.context.mark_clean()?;
        Ok(())
    }

    fn recover(
        context: &mut DatabaseContextData,
        message_store: &mut Projector<APP>,
        time_now: NoatunTime,
        projection_time_limit: Option<NoatunTime>,
        params: &APP::Params,
    ) -> Result<()> {
        context.clear()?;

        message_store.recover(time_now)?;
        let mmap_ptr = context.start_ptr();
        let guard = ContextGuardMut::new(context);
        let root_obj_ref = APP::initialize_root(params);
        let root_ptr = DatabaseContextData::index_of_rel(mmap_ptr, &*root_obj_ref);
        drop(guard);

        context.set_root_ptr(root_ptr.as_generic());
        context.set_next_seqnr(SequenceNr::from_index(0));

        // Safety:
        // Recover is only called when the db is not used
        let guard = ContextGuardMut::new(context);
        let root = unsafe { <APP as Object>::access_mut(root_ptr) };
        drop(guard);
        //let root = context.access_pod(root_ptr);
        message_store.apply_missing_messages(
            context,
            unsafe { root.get_unchecked_mut() },
            projection_time_limit,
        )?;

        Ok(())
    }

    pub fn remove_caches(path: impl AsRef<Path>) -> Result<()> {

        let path : PathBuf = path.as_ref().to_path_buf();
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

    /// Note: You can set max_file_size to something very large, like 100_000_000_000
    pub fn create_new(
        path: impl AsRef<Path>,
        overwrite_existing: bool,
        max_file_size: usize,
        cutoff_interval: CutOffDuration,
        projection_time_limit: Option<NoatunTime>,
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
            projection_time_limit,
            params,
        )
    }
    pub fn open(
        path: impl AsRef<Path>,
        max_file_size: usize,
        cutoff_interval: CutOffDuration,
        projection_time_limit: Option<NoatunTime>,
        params: APP::Params,
    ) -> Result<Database<APP>> {
        Self::create(
            Target::OpenExisting(path.as_ref().to_path_buf()),
            max_file_size,
            cutoff_interval,
            projection_time_limit,
            params,
        )
    }

    pub fn append_single(&mut self, message: Message<APP::Message>, local: bool) -> Result<()> {
        self.append_many(std::iter::once(message), local, true)
    }

    /// Set the current time to the given value.
    /// This does not update the system time, it only affects the time for Noatun.
    pub fn set_mock_time(&mut self, time: NoatunTime) {
        self.time_override = Some(time);
    }

    /// Returns true if the message still exists.
    /// If this returns false, the message has been deleted, and *MUST* not *BE* transmitted.
    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<bool> {
        self.message_store.mark_transmitted(message_id)
    }

    pub fn append_local(&mut self, message: APP::Message) -> Result<MessageHeader> {
        self.append_local_opt(None, message)
    }

    pub fn count_messages(&self) -> usize {
        self.message_store.count_messages()
    }
    pub fn compact(&mut self) -> Result<()> {
        self.message_store.compact()
    }
    pub fn append_local_at(
        &mut self,
        time: NoatunTime,
        message: APP::Message,
    ) -> Result<MessageHeader> {
        self.append_local_opt(Some(time), message)
    }
    pub fn append_local_opt(
        &mut self,
        time: Option<NoatunTime>,
        message: APP::Message,
    ) -> Result<MessageHeader> {
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
        /*println!(
            "At {:?}/#{} Appending {:?}",
            time,
            self.context.next_seqnr(),
            message
        );*/

        let t = Message::new(
            new_id,
            self.get_update_heads()
                .iter()
                .copied()
                .filter(|x| *x < new_id)
                .collect(),
            message,
        );
        //TODO: Find a way to avoid the following clone!
        let header = t.header.clone();
        self.append_single(t, true)?;
        Ok(header)
    }

    /// For messages before the cutoff-time, all parents are removed.
    pub fn append_many(
        &mut self,
        messages: impl Iterator<Item = Message<APP::Message>>,
        local: bool,
        allow_cutoff_advance: bool,
    ) -> Result<()> {
        let now = self.noatun_now();
        if !self.context.mark_dirty()? {
            // Recovery needed
            Self::recover(
                &mut self.context,
                &mut self.message_store,
                now,
                self.projection_time_limit,
                &self.params,
            )?;
        }

        if allow_cutoff_advance {
            self.maybe_advance_cutoff()?;
        }

        self.message_store
            .push_messages(&mut self.context, messages, local)?;



        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { <APP as Object>::access_mut(root_ptr) };
        drop(guard);

        self.message_store.apply_missing_messages(
            &mut self.context,
            unsafe { root.get_unchecked_mut() },
            self.projection_time_limit,
        )?;
        self.context.mark_clean()?;
        Ok(())
    }

    /// Create a database residing entirely in memory.
    /// This is mostly useful for tests
    // TODO: Use builder pattern?
    pub fn create_in_memory(
        max_size: usize,
        cutoff_interval: CutOffDuration,
        mock_time: Option<NoatunTime>,
        projection_time_limit: Option<NoatunTime>,
        params: APP::Params,
    ) -> Result<Database<APP>> {
        let mut disk = InMemoryDisk::default();
        let target = Target::CreateNew(PathBuf::default());
        let mut ctx = DatabaseContextData::new(&mut disk, &target, max_size)
            .context("creating database in memory")?;
        let mut message_store = Projector::new(&mut disk, &target, max_size, cutoff_interval)?;

        Self::recover(
            &mut ctx,
            &mut message_store,
            mock_time.unwrap_or_else(NoatunTime::now),
            projection_time_limit,
            &params,
        )?;
        ctx.mark_clean()?;

        Ok(Database {
            prev_local: None,
            context: ctx,
            message_store,
            time_override: mock_time,
            projection_time_limit,
            params,
            load_status: LoadingStatus::NewDatabase,
        })
    }

    fn create(
        target: Target,
        max_file_size: usize,
        cutoff_interval: CutOffDuration,
        projection_time_limit: Option<NoatunTime>,
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
            Self::recover(
                &mut ctx,
                &mut message_store,
                NoatunTime::now(),
                projection_time_limit,
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
            projection_time_limit,
            load_status,
        })
    }
    pub fn load_status(&self) -> LoadingStatus {
        self.load_status
    }
}
