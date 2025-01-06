#![feature(test)]
#![allow(unused)]
#![allow(dead_code)]

extern crate test;

use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
use crate::message_store::OnDiskMessageStore;
use crate::platform_specific::{FileMapping, get_boot_time};
use anyhow::{Context, Result, bail};
pub use projection_store::DatabaseContext;
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use fs2::FileExt;
use indexmap::IndexMap;
use memmap2::MmapMut;
use savefile_derive::Savefile;
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::cell::{Cell, OnceCell};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::{transmute, transmute_copy};
use std::ops::{Deref, Range};
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::slice::SliceIndex;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use crate::projector::Projector;
use crate::sha2_helper::sha2;
pub use database::Database;


mod projection_store;
mod disk_abstraction;
mod message_store;
mod undo_store;

struct MessageComponent<const ID: u32, T> {
    value: Option<T>,
}

pub(crate) mod platform_specific;

pub(crate) mod disk_access;
mod sha2_helper;
mod boot_checksum;

#[derive(
    Pod,
    Zeroable,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Savefile,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
pub struct MessageId {
    data: [u32; 4],
}

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time_ms = ((self.data[0] as u64) << 16) + ((self.data[1]) >> 16) as u64;

        let unix_timestamp = (time_ms / 1000) as i64;
        let timestamp_ms = time_ms % 1000;

        let time = OffsetDateTime::from_unix_timestamp(unix_timestamp).unwrap();

        let time_str = time.format(&Rfc3339).unwrap(); //All values representable should be formattable here
        write!(
            f,
            "{:?}-{:x}-{:x}-{:x}",
            time_str,
            self.data[1] & 0xffff,
            self.data[2],
            self.data[3]
        )
    }
}

impl Debug for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{:x}_{:x}_{:x}",
            self.data[0], self.data[1], self.data[2]
        )
    }
}

impl MessageId {
    pub fn min(self, other: MessageId) -> MessageId {
        if self < other { self } else { other }
    }
    pub fn is_zero(&self) -> bool {
        self.data[0] == 0 && self.data[1] == 0
    }
    pub fn zero() -> MessageId {
        MessageId { data: [0, 0, 0, 0] }
    }
    pub fn new_debug(nr: u32) -> Self {
        Self {
            data: [nr, 0, 0, 0],
        }
    }
}

#[derive(Pod, Zeroable, Copy, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
// 0 is an invalid sequence number, used to represent 'not a number'
pub struct SequenceNr(u32);

impl Display for SequenceNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "#INVALID")
        } else {
            write!(f, "#{}", self.0 - 1)
        }
    }
}
impl Debug for SequenceNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "#INVALID")
        } else {
            write!(f, "#{}", self.0 - 1)
        }
    }
}

impl SequenceNr {
    pub const INVALID: SequenceNr = SequenceNr(0);
    pub fn is_invalid(self) -> bool {
        self.0 == 0
    }
    pub fn is_valid(self) -> bool {
        self.0 != 0
    }
    pub fn successor(self) -> SequenceNr {
        SequenceNr(self.0 + 1)
    }
    pub fn from_index(index: usize) -> SequenceNr {
        if index >= (u32::MAX - 1) as usize {
            panic!("More than 2^32 elements created. Not supported by noatun");
        }
        SequenceNr(index as u32 + 1)
    }
    pub fn index(self) -> usize {
        if self.0 == 0 {
            panic!("0 SequenceNr does not have an index")
        }
        self.0 as usize - 1
    }
    pub fn try_index(self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        Some(self.0 as usize - 1)
    }
}

pub trait Message: Debug {
    type Root: Object;
    fn id(&self) -> MessageId;
    fn parents(&self) -> impl ExactSizeIterator<Item = MessageId>;
    fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root);

    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;
    fn serialize<W: Write>(&self, writer: W) -> Result<()>;
}

/// A state-less object, mostly useful for testing
pub struct DummyUnitObject;

impl Object for DummyUnitObject {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        &DummyUnitObject
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        // # SAFETY
        // Any dangling pointer is a valid pointer to a zero-sized type
        unsafe { &mut *(std::ptr::dangling_mut() as *mut Self) }
    }
}

mod projector {
    use std::marker::PhantomData;
    use crate::{Application, DatabaseContext, Message, SequenceNr, Target};
    use crate::disk_abstraction::Disk;
    use crate::message_store::OnDiskMessageStore;
    use anyhow::Result;

    // TODO: This type should probably not be public
    pub struct Projector<APP: Application> {
        messages: OnDiskMessageStore<APP::Message>,
        phantom_data: PhantomData<(*const APP::Root, APP::Message)>,
    }

    impl<APP: Application> Projector<APP> {
        pub fn new<D: Disk>(s: &mut D, target: &Target, max_size: usize) -> Result<Projector<APP>> {
            Ok(Projector {
                messages: OnDiskMessageStore::new(s, target, max_size)?,
                phantom_data: PhantomData,
            })
        }
        /// Returns true if the message did not exist and was inserted
        fn push_message(
            &mut self,
            context: &mut DatabaseContext,
            message: APP::Message,
        ) -> Result<bool> {
            if let Some(insert_point) = self.messages.append_single(message)? {
                self.rewind(context, insert_point)?;
                Ok(true)
            } else {
                Ok(false)
            }
        }

        /// Returns true if any of the messages were not previously present
        pub(crate) fn push_messages(
            &mut self,
            context: &mut DatabaseContext,
            message: impl Iterator<Item=APP::Message>,
        ) -> Result<bool> {
            let mut messages: Vec<APP::Message> = message.collect();
            messages.sort_unstable_by_key(|x| x.id());

            if let Some(insert_point) = self.messages.append_many_sorted(messages.into_iter())? {
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

        fn rewind(&mut self, context: &mut DatabaseContext, point: usize) -> Result<()> {
            context.rewind(SequenceNr::from_index(point));
            Ok(())
        }

        fn apply_single_message(
            context: &mut DatabaseContext,
            root: &mut APP::Root,
            msg: &APP::Message,
            seqnr: SequenceNr,
        ) {
            msg.apply(context, root); //TODO: Handle panics in apply gracefully
            context.set_next_seqnr(seqnr.successor()); //TODO: Don't record a snapshot for _every_ message.
            context.finalize_message(seqnr);
        }

        pub(crate) fn apply_missing_messages(
            &mut self,
            root: &mut APP::Root,
            context: &mut DatabaseContext,
        ) -> Result<()> {
            for (seq, msg) in self
                .messages
                .query_by_index(context.next_seqnr().try_index().unwrap())?
            {
                let seqnr = SequenceNr::from_index(seq);
                Self::apply_single_message(context, root, &msg, seqnr);
            }
            //let next_index = self.messages.next_index()?;
            //context.set_next_seqnr(SequenceNr::from_index(next_index));
            let must_remove = context.finalize_transaction(&mut self.messages)?;
            for index in must_remove {
                println!("Removing message {:?}", index);
                self.messages.mark_deleted_by_index(index.index());
                //*self.messages.get_index_mut(index.index()).unwrap().1 = None;
            }
            Ok(())
        }
    }

}

pub enum GenPtr {
    Thin(ThinPtr),
    Fat(FatPtr),
}

pub trait Pointer: Copy + Debug + 'static {
    fn start(self) -> usize;
    fn create<T>(addr: &T, buffer_start: *const u8) -> Self;
    fn as_generic(&self) -> GenPtr;
}

pub trait Object {
    /// This is meant to be either ThinPtr for sized objects, or
    /// FatPtr for dynamically sized objects. Other types are likely to not make sense.
    type Ptr: Pointer;
    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self;
    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self;
}

pub trait FixedSizeObject: Object<Ptr = ThinPtr> + Sized + Pod {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}

impl<T: Object<Ptr = ThinPtr> + Sized + Copy + Pod> FixedSizeObject for T {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}

impl Object for usize {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

impl Object for u32 {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
impl Object for u8 {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
pub trait Application {
    type Root: Object;
    type Message: Message<Root = Self::Root>;

    fn initialize_root(ctx: &mut DatabaseContext) -> <Self::Root as Object>::Ptr;
}


#[derive(Copy, Clone)]
#[repr(C)]
pub struct DatabaseCell<T: Copy> {
    value: T,
    registrar: SequenceNr,
}

impl<T: Copy> Deref for DatabaseCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Copy, Clone, Debug)]
pub struct FatPtr {
    start: usize,
    len: usize,
}
impl FatPtr {
    pub fn from(start: usize, len: usize) -> FatPtr {
        FatPtr { start, len }
    }
}
#[derive(Copy, Clone, Debug)]
pub struct ThinPtr(pub usize);

unsafe impl Zeroable for FatPtr {}

unsafe impl Pod for FatPtr {}
unsafe impl Zeroable for ThinPtr {}

unsafe impl Pod for ThinPtr {}

impl Pointer for ThinPtr {
    fn start(self) -> usize {
        self.0
    }

    fn create<T>(addr: &T, buffer_start: *const u8) -> Self {
        let index = (addr as *const T as *const u8 as usize) - (buffer_start as usize);
        ThinPtr(index)
    }

    fn as_generic(&self) -> GenPtr {
        GenPtr::Thin(*self)
    }
}

impl Pointer for FatPtr {
    fn start(self) -> usize {
        self.start
    }
    fn create<T>(addr: &T, buffer_start: *const u8) -> Self {
        let addr: *const T = addr as *const T;
        let dummy: (*const u8, usize);
        assert_eq!(
            std::mem::size_of::<*const T>(),
            2 * std::mem::size_of::<usize>()
        );
        dummy = unsafe { transmute_copy(&addr) };
        FatPtr {
            start: ((dummy.0 as usize) - (buffer_start as usize)),
            len: dummy.1,
        }
    }
    fn as_generic(&self) -> GenPtr {
        GenPtr::Fat(*self)
    }
}

impl Object for [u8] {
    type Ptr = FatPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_mut(index) }
    }
}

unsafe impl<T> Zeroable for DatabaseCell<T> where T: Pod {}

unsafe impl<T> Pod for DatabaseCell<T> where T: Pod {}
impl<T: Pod> DatabaseCell<T> {
    pub fn get(&self, context: &DatabaseContext) -> T {
        context.observe_registrar(self.registrar);
        self.value
    }
    pub fn set<'a>(&'a mut self, context: &'a mut DatabaseContext, new_value: T) {
        let index = context.index_of(self);
        //context.write(index, bytes_of(&new_value));
        context.write_pod(new_value, &mut self.value);
        context.update_registrar(&mut self.registrar);
    }
}

#[repr(C)]
pub struct DatabaseVec<T> {
    length: usize,
    capacity: usize,
    data: usize,
    length_registrar: SequenceNr,
    phantom_data: PhantomData<T>,
}

unsafe impl<T> Zeroable for DatabaseVec<T> {}

impl<T> Copy for DatabaseVec<T> {}

impl<T> Clone for DatabaseVec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T> Pod for DatabaseVec<T> where T: 'static {}

impl<T> DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    pub fn new(ctx: &DatabaseContext) -> &mut DatabaseVec<T> {
        unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
    }
    pub fn len(&self, ctx: &mut DatabaseContext) -> usize {
        ctx.observe_registrar(self.length_registrar);
        self.length
    }
    pub fn get(&self, ctx: &DatabaseContext, index: usize) -> &T {
        let offset = self.data + index * T::SIZE;
        unsafe { T::access(ctx, ThinPtr(offset)) }
    }
    pub fn get_mut(&mut self, ctx: &mut DatabaseContext, index: usize) -> &mut T {
        let offset = self.data + index * T::SIZE;
        let t = unsafe { T::access_mut(ctx, ThinPtr(offset)) };
        t
    }

    fn realloc_add(&mut self, ctx: &mut DatabaseContext, new_capacity: usize) {
        let dest = ctx.allocate_raw(new_capacity * T::SIZE, T::ALIGN);
        let dest_index = ctx.index_of_ptr(dest);

        if self.length > 0 {
            let old_ptr = FatPtr::from(self.data, T::SIZE * self.length);
            println!("Copy old");
            ctx.copy(old_ptr, dest_index.0);
        }

        let new_len = self.length + 1;

        println!("Write-pod with db.vec");

        ctx.write_pod(
            DatabaseVec {
                length: new_len,
                capacity: new_capacity,
                data: dest_index.0,
                length_registrar: SequenceNr::default(),
                phantom_data: Default::default(),
            },
            self,
        )
    }

    pub fn push_new<'a>(&'a mut self, ctx: &mut DatabaseContext) -> &'a mut T {
        if self.length >= self.capacity {
            println!("Realloc-leg");
            self.realloc_add(ctx, (self.capacity + 1) * 2);
        } else {
            println!("no-Realloc-leg");
            ctx.write_pod(self.length + 1, &mut self.length);
        }
        ctx.update_registrar(&mut self.length_registrar);

        self.get_mut(ctx, self.length - 1)
    }
}

impl<T> Object for DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { &*(context.start_ptr().wrapping_add(index.0) as *const DatabaseVec<T>) }
    }
    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

#[repr(transparent)]
struct DatabaseObjectHandle<T: Object> {
    object_index: T::Ptr,
    phantom: PhantomData<T>,
}

unsafe impl<T: Object> Zeroable for DatabaseObjectHandle<T> {}

impl<T: Object> Copy for DatabaseObjectHandle<T> {}

impl<T: Object> Clone for DatabaseObjectHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T: Object + 'static> Pod for DatabaseObjectHandle<T> {}
impl<T: Object> Object for DatabaseObjectHandle<T> {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe {
            &*(context.start_ptr().wrapping_add(index.0) as *const _
                as *const DatabaseObjectHandle<T>)
        }
    }
    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe {
            &mut *(context.start_ptr().wrapping_add(index.0) as *const _
                as *mut DatabaseObjectHandle<T>)
        }
    }
}

impl<T: Object> DatabaseObjectHandle<T> {
    pub fn get<'a>(&'a self, context: &DatabaseContext) -> &'a T {
        unsafe { T::access(context, self.object_index) }
    }
    pub fn get_mut<'a>(&'a self, context: &mut DatabaseContext) -> &'a mut T {
        unsafe { T::access_mut(context, self.object_index) }
    }
    pub fn new(context: &DatabaseContext, value: T) -> &mut Self
    where
        T: Object<Ptr = ThinPtr>,
        T: Pod,
    {
        let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
        let target = unsafe { context.allocate_pod::<T>() };
        *target = value;
        this.object_index = context.index_of(target);
        this
    }
}

impl<T: Pod + Object> DatabaseCell<T> {
    pub fn new(context: &DatabaseContext) -> &mut Self {
        let memory = unsafe { context.allocate_pod::<T>() };
        unsafe { &mut *(memory as *mut _ as *mut DatabaseCell<T>) }
    }
}

impl<T: Pod> Object for DatabaseCell<T> {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
thread_local! {
    static MULTI_INSTANCE_BLOCKER: Cell<bool> = Cell::new(false);
}

#[derive(Clone)]
pub enum Target {
    OpenExisting(PathBuf),
    CreateNewOrOverwrite(PathBuf),
    CreateNew(PathBuf),
}
impl Target {
    pub fn path(&self) -> &Path {
        let (Target::CreateNew(x) | Target::CreateNewOrOverwrite(x) | Target::OpenExisting(x)) =
            self;
        &*x
    }
    pub fn create(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_) | Target::CreateNew(_))
    }
    pub fn overwrite(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_))
    }
}

pub mod database {
    use std::path::{Path, PathBuf};
    use crate::{Application, DatabaseContext, Object, Pointer, SequenceNr, Target, MULTI_INSTANCE_BLOCKER};
    use crate::projector::Projector;
    use anyhow::{Context, Result};
    use crate::disk_abstraction::{InMemoryDisk, StandardDisk};

    pub struct Database<Base: Application> {
        context: DatabaseContext,
        message_store: Projector<Base>,
        app: Base,
    }

    impl<APP: Application> Database<APP> {

        pub(crate) fn force_rewind(&mut self, index: SequenceNr) {
            self.context.rewind(index)
        }

        pub fn get_root(&self) -> (&APP::Root, &DatabaseContext) {
            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access(&self.context, root_ptr) };
            (root, &self.context)
        }
        // TODO: This method should probably not be public (changes should only happen through messages)
        pub fn with_root_mut<R>(
            &mut self,
            f: impl FnOnce(&mut APP::Root, &mut DatabaseContext, &mut Projector<APP>) -> R,
        ) -> Result<R> {
            if !self.context.mark_dirty()? {
                // Recovery needed
                Self::recover(&mut self.app, &mut self.context, &mut self.message_store)?;
            }

            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access_mut(&mut self.context, root_ptr) };

            let t = f(root, &mut self.context, &mut self.message_store);

            self.context.mark_clean();
            Ok(t)
        }

        fn recover(
            app: &mut APP,
            context: &mut DatabaseContext,
            message_store: &mut Projector<APP>,
        ) -> Result<()> {
            context.clear()?;

            let root_ptr = APP::initialize_root(context);
            context.set_root_ptr(root_ptr.as_generic());

            context.set_next_seqnr(SequenceNr::from_index(0));

            let root = <APP::Root as Object>::access_mut(context, root_ptr);
            message_store.apply_missing_messages(root, context)?;

            Ok(())
        }

        /// Note: You can set max_file_size to something very large, like 100_000_000_000
        pub fn create_new(
            path: impl AsRef<Path>,
            app: APP,
            overwrite_existing: bool,
            max_file_size: usize,
        ) -> Result<Database<APP>> {
            Self::create(
                app,
                if overwrite_existing {
                    Target::CreateNewOrOverwrite(path.as_ref().to_path_buf())
                } else {
                    Target::CreateNew(path.as_ref().to_path_buf())
                },
                max_file_size,
            )
        }
        pub fn open(path: impl AsRef<Path>, app: APP, max_file_size: usize) -> Result<Database<APP>> {
            Self::create(app, Target::OpenExisting(path.as_ref().to_path_buf()), max_file_size)
        }

        pub fn append_single(&mut self, message: APP::Message) -> Result<()> {
            self.append_many(std::iter::once(message))
        }

        pub fn append_many(&mut self, messages: impl Iterator<Item=APP::Message>) -> Result<()> {
            self.with_root_mut(|root, context, message_store| {
                message_store.push_messages(context, messages)?;
                message_store.apply_missing_messages(root, context)?;

                Ok(())
            })?
        }

        /// Create a database residing entirely in memory.
        /// This is mostly useful for tests
        pub fn create_in_memory(mut app: APP, max_size: usize) -> Result<Database<APP>> {
            let mut disk = InMemoryDisk::default();
            let target = Target::CreateNew(PathBuf::default());
            let mut ctx =
                DatabaseContext::new(&mut disk, &target, max_size).context("creating database in memory")?;
            let mut message_store = Projector::new(&mut disk, &target, max_size)?;
            Ok(Database {
                context: ctx,
                app,
                message_store,
            })
        }

        fn create(mut app: APP, target: Target, max_file_size: usize) -> Result<Database<APP>> {
            if MULTI_INSTANCE_BLOCKER.get() {
                if !std::env::var("NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE").is_ok() {
                    panic!(
                        "Noatun: Multiple active DB-roots in the same thread are not allowed.\
                        You can disable this diagnostic by setting env-var NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE.\
                        Note, unsoundness can occur if DatabaseContext from one instance is used by data \
                        for other."
                    );
                }
            }
            MULTI_INSTANCE_BLOCKER.set(true);

            let mut ctx =
                DatabaseContext::new(&mut StandardDisk, &target, max_file_size).context("opening database")?;

            let is_dirty = ctx.is_dirty();

            let mut message_store = Projector::new(&mut StandardDisk, &target, max_file_size)?;
            if is_dirty {
                Self::recover(&mut app, &mut ctx, &mut message_store)?;
                ctx.mark_clean()?;
            }
            Ok(Database {
                context: ctx,
                app,
                message_store,
            })
        }
    }
    impl<APP: Application> Drop for Database<APP> {
        fn drop(&mut self) {
            MULTI_INSTANCE_BLOCKER.set(false);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection_store::MainDbHeader;
    use crate::disk_access::DiskAccessor;
    use byteorder::{LittleEndian, WriteBytesExt};
    use database::Database;
    use savefile::{load_noschema, save_noschema};
    use savefile_derive::Savefile;
    use sha2::{Digest, Sha256};
    use std::io::{Cursor, SeekFrom};
    use test::Bencher;

    #[test]
    fn test_mmap_big() {
        let mut mmap = DiskAccessor::new(
            &Target::CreateNewOrOverwrite("test/mmap_test_big".into()),
            "mmap",
            0,
            1024 * 1024*1024,
        );
        //use std::io::Read;
        //let _ =  std::io::stdin().read(&mut [0u8]).unwrap();
    }

    #[test]
    fn test_mmap_helper() {
        let mut mmap = DiskAccessor::new(
            &Target::CreateNewOrOverwrite("test/mmap_test1".into()),
            "mmap",
            0,
            16 * 1024 * 1024,
        )
        .unwrap();
        mmap.write_u32::<LittleEndian>(0x2b).unwrap();
        use byteorder::ReadBytesExt;
        use std::io::Read;
        use std::io::Seek;
        mmap.seek(SeekFrom::Start(12)).unwrap();
        mmap.write_u64::<LittleEndian>(0x2c).unwrap();
        mmap.seek(SeekFrom::Start(12)).unwrap();
        let initial_ptr = mmap.get_ptr();
        assert_eq!(mmap.read_u64::<LittleEndian>().unwrap(), 0x2c);

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        mmap.write_u8(1).unwrap();
        assert_eq!(initial_ptr, mmap.get_ptr());

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        assert_eq!(mmap.read_u8().unwrap(), 1);

        mmap.flush_all().unwrap();

        println!("Truncate");
        mmap.truncate(0).unwrap();
        mmap.seek(SeekFrom::Start(0)).unwrap();

        let mut buf = [0];
        let got = mmap.read(&mut buf).unwrap();
        assert_eq!(got, 0);
        mmap.write_u8(42).unwrap();
        mmap.write_u8(42).unwrap();
        mmap.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(mmap.read_u8().unwrap(), 42);
    }

    struct DummyMessage<T> {
        phantom_data: PhantomData<T>,
    }
    impl<T> Debug for DummyMessage<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "DummyMessage")
        }
    }

    impl<T: Object> Message for DummyMessage<T> {
        type Root = T;

        fn id(&self) -> MessageId {
            todo!()
        }

        fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
            std::iter::empty()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            todo!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            todo!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            todo!()
        }
    }

    #[derive(Clone, Copy, Zeroable, Pod)]
    #[repr(C)]
    struct CounterObject {
        counter: DatabaseCell<u32>,
        counter2: DatabaseCell<u32>,
    }

    impl Object for CounterObject {
        type Ptr = ThinPtr;

        fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl CounterObject {
        fn set_counter(&mut self, ctx: &mut DatabaseContext, value1: u32, value2: u32) {
            self.counter.set(ctx, value1);
            self.counter2.set(ctx, value2);
        }
        fn new(ctx: &DatabaseContext) -> &mut CounterObject {
            ctx.allocate_pod()
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root = CounterObject;
        type Message = CounterMessage;

        fn initialize_root(mut ctx: &mut DatabaseContext) -> ThinPtr {
            let new_obj = CounterObject::new(&ctx);
            //assert_eq!(ctx.index_of(&new_obj.counter), 0);
            ctx.index_of(new_obj)
            //new_obj
        }
    }

    #[derive(Debug)]
    struct IncrementMessage {
        increment_by: u32,
    }

    impl Message for IncrementMessage {
        type Root = CounterObject;

        fn id(&self) -> MessageId {
            MessageId::new_debug(self.increment_by)
        }

        fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
            std::iter::empty()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            todo!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            todo!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            todo!()
        }
    }

    #[test]
    fn test1() {
        let mut db: Database<CounterApplication> =
            Database::create_new("test/test1.bin", CounterApplication, true, 1000).unwrap();


        db.with_root_mut(|counter, context, _message_store| {
            assert_eq!(counter.counter.get(context), 0);
            counter.counter.set(context, 42);
            counter.counter2.set(context, 43);
            counter.counter.set(context, 44);

            assert_eq!(*counter.counter, 44);
            assert_eq!(counter.counter.get(context), 44);
            assert_eq!(counter.counter2.get(context), 43);
        });
    }

    #[derive(Debug, Savefile)]
    struct CounterMessage {
        id: MessageId,
        parent: Option<MessageId>,
        inc1: i32,
        set1: u32,
    }
    impl Message for CounterMessage {
        type Root = CounterObject;
        fn id(&self) -> MessageId {
            self.id
        }

        fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
            self.parent.into_iter()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut CounterObject) {
            println!("Applying {:?}", self);
            if self.inc1 != 0 {
                root.counter.set(
                    context,
                    root.counter.get(context).saturating_add_signed(self.inc1),
                );
            } else {
                root.counter.set(context, self.set1);
            }
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            Ok(load_noschema(&mut Cursor::new(buf), 1)?)
        }

        fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
            Ok(save_noschema(&mut writer, 1, self)?)
        }
    }

    #[test]
    fn test_msg_store_real() {
        let mut db: Database<CounterApplication> =
            Database::create_new("test/msg_store.bin", CounterApplication, true, 10000).unwrap();
        //        let mut messages = MessageStore::new(StandardDisk, &Target::CreateNewOrOverwrite("test/msg_store.bin".into())).unwrap();

        db.append_single(CounterMessage {
            parent: None,
            id: MessageId::new_debug(0x100),
            inc1: 2,
            set1: 0,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x100)),
            id: MessageId::new_debug(0x101),
            inc1: 0,
            set1: 42,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x101)),
            id: MessageId::new_debug(0x102),
            inc1: 1,
            set1: 0,
        })
        .unwrap();

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context, _| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_msg_store_inmem_miri() {
        let mut db: Database<CounterApplication> =
            Database::create_in_memory(CounterApplication, 10000).unwrap();


        db.append_single(CounterMessage {
            parent: None,
            id: MessageId::new_debug(0x100),
            inc1: 2,
            set1: 0,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x100)),
            id: MessageId::new_debug(0x101),
            inc1: 0,
            set1: 42,
        })
        .unwrap();
        db.append_single(CounterMessage {
            parent: Some(MessageId::new_debug(0x101)),
            id: MessageId::new_debug(0x102),
            inc1: 1,
            set1: 0,
        })
        .unwrap();

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context, _| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_handle() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<u32>;
            type Message = DummyMessage<DatabaseObjectHandle<u32>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> ThinPtr {
                let obj = DatabaseObjectHandle::new(&ctx, 43u32);

                ctx.index_of(obj)
            }
        }

        let mut db: Database<HandleApplication> =
            Database::create_new("test/test_handle.bin", HandleApplication, true, 1000).unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(*handle.get(context), 43);
    }

    #[test]
    fn test_vec0() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> ThinPtr {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                ctx.index_of(obj)
            }

            type Message = DummyMessage<DatabaseVec<CounterObject>>;
        }

        let mut db: Database<CounterVecApplication> =
            Database::create_new("test/test_vec0", CounterVecApplication, true, 10000).unwrap();
        db.with_root_mut(|counter_vec, context, _| {
            assert_eq!(counter_vec.len(context), 0);

            println!("1");
            let new_element = counter_vec.push_new(context);
            let new_element = counter_vec.get_mut(context, 0);
            println!("2");

            new_element.counter.set(context, 47);
            let new_element = counter_vec.push_new(context);
            new_element.counter.set(context, 48);
            println!("3");

            assert_eq!(counter_vec.len(context), 2);

            let item = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);

            println!("4");

            for _ in 0..10 {
                let new_element = counter_vec.push_new(context);
                println!("5");
            }
            println!("6");

            let item = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
        });
    }

    #[test]
    fn test_vec_undo() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> ThinPtr {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                let index = ctx.index_of(obj);

                // This assert might not hold in the future, if we redesign things.
                // It's a bit white-box, consider removing.
                assert_eq!(index.start(), size_of::<MainDbHeader>());
                index
            }

            type Message = DummyMessage<DatabaseVec<CounterObject>>;
        }

        let mut db: Database<CounterVecApplication> =
            Database::create_new("test/vec_undo", CounterVecApplication, true, 10000).unwrap();

        {
            db.with_root_mut(|counter_vec, context, _| {
                context.set_next_seqnr(SequenceNr::from_index(1));
                assert_eq!(counter_vec.len(context), 0);
                println!("Pre-push");
                let new_element = counter_vec.push_new(context);
                new_element.counter.set(context, 47);
                new_element.counter2.set(context, 48);
                println!("Pre-set-time");
                context.set_next_seqnr(SequenceNr::from_index(2));
                assert_eq!(counter_vec.len(context), 1);
                context.set_next_seqnr(SequenceNr::from_index(3));
            });
        }

        {
            db.with_root_mut(|counter_vec, context, _| {
                let counter = counter_vec.get_mut(context, 0);
                counter.counter.set(context, 50);
                context.rewind(SequenceNr::from_index(2));
                assert_eq!(counter.counter.get(context), 47);
            });
        }

        println!("Rewind!");
        db.force_rewind(SequenceNr::from_index(1));

        {
            db.with_root_mut(|counter_vec, context, _| {
                assert_eq!(counter_vec.len(context), 0);
            });
        }
    }

    #[bench]
    fn bench_sha256(b: &mut Bencher) {
        // write input message

        // read hash digest and consume hasher

        b.iter(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"hello world");
            hasher.finalize()
        });
    }
}
