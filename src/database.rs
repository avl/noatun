use crate::cutoff::CutoffHash;
use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
use crate::disk_access::FileAccessor;
use crate::message_store::IndexEntry;
use crate::projector::Projector;
use crate::sequence_nr::SequenceNr;
use crate::update_head_tracker::UpdateHeadTracker;
use crate::{
    Application, ContextGuard, ContextGuardMut, DatabaseContextData, Message, MessageComponent,
    MessageHeader, MessageId, MessagePayload, MultiInstanceThreadBlocker, Object, Pointer, Target,
    CONTEXT
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime};

pub struct Database<Base: Application> {
    context: DatabaseContextData,
    message_store: Projector<Base>,
    // Most recently generated local id, or all zeroes.
    // Future local id's will always be greater than this.
    prev_local: Option<MessageId>,
    time_override: Option<DateTime<Utc>>,
    projection_time_limit: Option<DateTime<Utc>>,
    params: Base::Params,
}

//TODO: Make the modules in this file be distinct files

impl<APP: Application> Database<APP> {
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

    pub fn is_acceptable_cutoff_hash(&self, hash: CutoffHash) -> Result<bool> {
        self.message_store.is_acceptable_cutoff_hash(hash)
    }
    pub fn nominal_cutoffhash(&self) -> Result<CutoffHash> {
        self.message_store.nominal_cutoffhash()
    }

    pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
        self.message_store.get_all_message_ids()
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
        let now = self.now();
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
        self.message_store.apply_preview(time, root.as_mut(), preview)?;
        let ret = f(&*root);
        drop(guard);
        self.message_store
            .rewind(&mut self.context, current.index())?;
        self.context.mark_clean()?;
        Ok(ret)
    }

    pub fn with_root<R>(&self, f: impl FnOnce(&APP) -> R) -> R {
        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuard::new(&self.context);
        let root = unsafe { <APP as Object>::access(root_ptr) };
        f(root)
    }

    pub(crate) fn now(&self) -> chrono::DateTime<Utc> {
        self.time_override.unwrap_or_else(Utc::now)
    }

    pub(crate) fn with_root_mut<R>(&mut self, f: impl FnOnce(Pin<&mut APP>) -> R) -> Result<R> {
        let now = self.now();
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

    pub fn set_projection_time_limit(&mut self, limit: DateTime<Utc>) -> Result<()> {
        let now = self.now();
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
            unsafe{root.get_unchecked_mut()},
            now,
            self.projection_time_limit,
        )?;

        self.context.mark_clean();
        Ok(())
    }

    pub fn reproject(&mut self) -> Result<()> {
        let now = self.now();
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
            unsafe{root.get_unchecked_mut()},
            now,
            self.projection_time_limit,
        )?;

        self.context.mark_clean()?;
        Ok(())
    }

    fn recover(
        context: &mut DatabaseContextData,
        message_store: &mut Projector<APP>,
        time_now: chrono::DateTime<Utc>,
        projection_time_limit: Option<DateTime<Utc>>,
        params: &APP::Params,
    ) -> Result<()> {
        context.clear()?;

        message_store.recover();
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
        message_store.apply_missing_messages(context, unsafe{root.get_unchecked_mut()}, time_now, projection_time_limit)?;

        Ok(())
    }

    /// Note: You can set max_file_size to something very large, like 100_000_000_000
    pub fn create_new(
        path: impl AsRef<Path>,
        overwrite_existing: bool,
        max_file_size: usize,
        cutoff_interval: Duration,
        projection_time_limit: Option<DateTime<Utc>>,
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
        cutoff_interval: Duration,
        projection_time_limit: Option<DateTime<Utc>>,
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

    // TODO: We should separate the message_id from the Message-type, and let
    // Noatun provide message_id. It should be provided when adding the message, so that
    // we can be sure that all local messages are such that they haven't been observed
    // previously
    // TODO: Maybe change the signature of this, and some other public methods, to accept
    // a raw message payload, and hide messageId-generation from user!
    pub fn append_single(&mut self, message: Message<APP::Message>, local: bool) -> Result<()> {
        self.append_many(std::iter::once(message), local)
    }

    /// Set the current time to the given value.
    /// This does not update the system time, it only affects the time for Noatun.
    pub fn set_mock_time(&mut self, time: DateTime<Utc>) {
        self.time_override = Some(time);
    }

    pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<()> {
        self.message_store.mark_transmitted(message_id)
    }


    pub fn append_local(&mut self, message: APP::Message) -> Result<MessageHeader> {
        self.append_local_opt(None, message)
    }
    pub fn append_local_at(&mut self, time: DateTime<Utc>, message: APP::Message) -> Result<MessageHeader> {
        self.append_local_opt(Some(time), message)
    }
    pub fn append_local_opt(&mut self, time: Option<DateTime<Utc>>, message: APP::Message) -> Result<MessageHeader> {
        let time = time.unwrap_or_else(||self.now());
        let mut new_id;


        if let Some(prev_local) = self.prev_local {
            if time.timestamp_millis() as u64  == prev_local.timestamp() { //TODO: Fix all cases of u64 timestamps. We should probably just use i64 instead
                new_id = prev_local.successor();
            } else {
                new_id  = MessageId::generate_for_time(time)?;
            }
        } else {
            new_id  = MessageId::generate_for_time(time)?;
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

    pub fn append_many(
        &mut self,
        messages: impl Iterator<Item = Message<APP::Message>>,
        local: bool,
    ) -> Result<()> {
        let now = self.now();
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

        self.message_store
            .push_messages(&mut self.context, messages, local);

        let root_ptr = self.context.get_root_ptr::<<APP as Object>::Ptr>();
        let guard = ContextGuardMut::new(&mut self.context);
        let root = unsafe { <APP as Object>::access_mut(root_ptr) };
        drop(guard);

        self.message_store.apply_missing_messages(
            &mut self.context,
            unsafe{root.get_unchecked_mut()},
            now,
            self.projection_time_limit,
        )?;
        self.context.mark_clean();
        Ok(())
    }

    /// Create a database residing entirely in memory.
    /// This is mostly useful for tests
    // TODO: Use builder pattern?
    pub fn create_in_memory(
        max_size: usize,
        cutoff_interval: Duration,
        mock_time: Option<chrono::DateTime<Utc>>,
        projection_time_limit: Option<DateTime<Utc>>,
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
            mock_time.unwrap_or_else(Utc::now),
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
        })
    }

    fn create(
        target: Target,
        max_file_size: usize,
        cutoff_interval: Duration,
        projection_time_limit: Option<DateTime<Utc>>,
        params: APP::Params,
    ) -> Result<Database<APP>> {

        let mut disk = StandardDisk;

        let mut ctx = DatabaseContextData::new(&mut disk, &target, max_file_size)
            .context("opening database")?;

        let is_dirty = ctx.is_dirty();

        let mut message_store = Projector::new(&mut disk, &target, max_file_size, cutoff_interval)?;
        let mut update_heads = disk.open_file(&target, "update_heads", 0, 128 * 1024 * 1024)?;
        if is_dirty {
            Self::recover(
                &mut ctx,
                &mut message_store,
                Utc::now(),
                projection_time_limit,
                &params,
            )?;
            ctx.mark_clean()?;
        }
        Ok(Database {
            params,
            prev_local: None,
            context: ctx,
            message_store,
            time_override: None,
            projection_time_limit,
        })
    }
}
