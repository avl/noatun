#![feature(test)]
#![allow(unused)]
#![allow(dead_code)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::comparison_chain)]

extern crate test;

use crate::data_types::DatabaseCell;
use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
use crate::message_store::OnDiskMessageStore;
use crate::platform_specific::{FileMapping, get_boot_time};
use crate::projector::Projector;
use crate::sha2_helper::sha2;
use anyhow::{Context, Result, bail};
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
pub use database::Database;
use fs2::FileExt;
use indexmap::IndexMap;
use memmap2::MmapMut;
pub use projection_store::DatabaseContext;
#[cfg(feature = "savefile")]
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

mod disk_abstraction;
mod message_store;
mod projection_store;
mod undo_store;

struct MessageComponent<const ID: u32, T> {
    value: Option<T>,
}

pub(crate) mod platform_specific;

mod boot_checksum;
pub(crate) mod disk_access;
mod sha2_helper;

#[derive(
    Pod, Zeroable, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[cfg_attr(feature = "savefile", derive(Savefile))]
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
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct DummyUnitObject;

impl Object for DummyUnitObject {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        &DummyUnitObject
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        // # SAFETY
        // Any dangling pointer is a valid pointer to a zero-sized type
        unsafe { &mut *(std::ptr::dangling_mut()) }
    }
}


mod update_head_tracker {
    use crate::disk_abstraction::Disk;
    use crate::disk_access::FileAccessor;
    use crate::MessageId;
    use anyhow::Result;

    pub(crate) struct UpdateHeadTracker {
        file: FileAccessor
    }

    impl UpdateHeadTracker {

        pub(crate) fn add_new_update_head(&mut self,
                                          new_message_id: MessageId,
                                          subsumed: &[MessageId],
                                          ) -> anyhow::Result<()> {
            let mapping = self.file.map_mut();
            let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);
            let mut i = 0;
            let mut file_len = id_mapping.len();
            let mut maplen = id_mapping.len();
            while i < maplen {
                if subsumed.contains(&id_mapping[i]) {
                    if i != maplen-1 {
                        id_mapping.swap(i, maplen-1);
                    }
                    maplen -= 1;
                } else {
                    i += 1;
                }
            }
            if maplen == file_len {
                self.file.grow((file_len +1)*size_of::<MessageId>())?;
                file_len = file_len +1;
            }

            let mapping = self.file.map_mut();
            let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);

            id_mapping[maplen] = new_message_id.clone();
            maplen += 1;

            if maplen < file_len {
                self.file.fast_truncate(maplen);
            }
            Ok(())
        }

        pub(crate) fn get_update_heads(&self) -> &[MessageId] {
            bytemuck::cast_slice(self.file.map())
        }
        pub(crate) fn new<D:Disk>(disk: &mut D, target: &crate::Target) -> Result<UpdateHeadTracker> {
            Ok(Self {
                file: disk.open_file(target,"update_head", 0, 10*1024*1024)?,
            })
        }
    }
}

mod update_tail_tracker {
    use crate::disk_abstraction::Disk;
    use crate::disk_access::FileAccessor;
    use crate::message_store::OnDiskMessageStore;
    use crate::{Message, MessageId, Target};
    use anyhow::Result;


    pub(crate) struct UpdateTailTracker {
        file: FileAccessor
    }

    /// Contains messages that cannot be applied to main store because we don't know
    /// their ancestors
    pub(crate) struct Quarantine<M:Message> {
        quarantine: OnDiskMessageStore<M>
    }
    impl<M:Message> Quarantine<M> {
        pub(crate) fn add_new_update_tail(&mut self,
                                          message: &M,
                                          existing: &OnDiskMessageStore<M>) -> Result<()> {
            for item in message.parents() {
                if !existing.contains_message(item)? {

                }

            }
            todo!()
        }

        pub(crate) fn new<D:Disk>(disk: &mut D, target: &Target, max_file_size: usize) -> anyhow::Result<Self> {
            let mut sub = target.append("tail");
            Ok(Self {
                quarantine: OnDiskMessageStore::new(disk, &sub, max_file_size)?,
            })
        }
    }

}

mod projector {
    use crate::disk_abstraction::Disk;
    use crate::message_store::OnDiskMessageStore;
    use crate::{Application, Database, DatabaseContext, Message, MessageId, SequenceNr, Target};
    use anyhow::Result;
    use std::marker::PhantomData;
    use crate::disk_access::FileAccessor;
    use crate::update_head_tracker::UpdateHeadTracker;

    pub(crate) struct Projector<APP: Application> {
        messages: OnDiskMessageStore<APP::Message>,
        head_tracker: UpdateHeadTracker,
        phantom_data: PhantomData<(*const APP::Root, APP::Message)>,
    }

    impl<APP: Application> Projector<APP> {

        pub fn get_upstream_of(&self, message_id: &[MessageId], count: usize) -> Result<impl Iterator<Item=APP::Message>> {
            self.messages.get_upstream_of(message_id, count)
        }

        pub(crate) fn get_update_heads(&self) -> &[MessageId] {
            self.head_tracker.get_update_heads()
        }

        pub(crate) fn contains_message(&self, id: MessageId) -> Result<bool> {
            self.messages.contains_message(id)
        }

        pub(crate) fn load_message(&self, id: MessageId) -> Result<APP::Message> {
            Ok(self.messages.read_message(id)?
                .ok_or_else(|| anyhow::anyhow!("Message not found"))?
            )
        }

        pub fn recover(&mut self) -> Result<()> {
            self.messages.recover(
                                  |id,parents|self.head_tracker.add_new_update_head(id,parents)
            )
        }

        pub(crate) fn new<D: Disk>(
            s: &mut D,
            target: &Target,
            max_size: usize,
        ) -> Result<Projector<APP>> {
            Ok(Projector {
                messages: OnDiskMessageStore::new(s, target, max_size)?,
                head_tracker: UpdateHeadTracker::new(s, target)?,
                phantom_data: PhantomData,
            })
        }
        /// Returns true if the message did not exist and was inserted
        fn push_message(
            &mut self,
            context: &mut DatabaseContext,
            message: APP::Message,
        ) -> Result<bool> {
            self.push_sorted_messages(context, std::iter::once(message))
        }

        /// Returns true if any of the messages were not previously present
        pub(crate) fn push_messages(
            &mut self,
            context: &mut DatabaseContext,
            message: impl Iterator<Item = APP::Message>,
        ) -> Result<bool> {
            let mut messages: Vec<APP::Message> = message.collect();
            messages.sort_unstable_by_key(|x| x.id());

            self.push_sorted_messages(context, messages.into_iter())
        }
        pub(crate) fn push_sorted_messages(
            &mut self,
            context: &mut DatabaseContext,
            messages: impl ExactSizeIterator<Item = APP::Message>,
        ) -> Result<bool> {
            if let Some(insert_point) = self.messages.append_many_sorted(messages, |id,parents|self.head_tracker.add_new_update_head(id,parents))? {

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
    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self;
    fn as_generic(&self) -> GenPtr;
}

pub trait Object {
    /// This is meant to be either ThinPtr for sized objects, or
    /// FatPtr for dynamically sized objects. Other types are likely to not make sense.
    type Ptr: Pointer;
    /// # Safety
    /// The caller must ensure that the accessed object is not aliased with a mutable
    /// reference to the same object.
    // TODO: Don't expose these methods. Too hard to use correctly!
    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self;
    /// # Safety
    /// The caller must ensure that the accessed object is not aliased with any other
    /// reference to the same object.
    // TODO: Don't expose these methods. Too hard to use correctly!
    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self;
}


#[derive(Clone,Debug,Copy,Pod,Zeroable)]
#[repr(transparent)]
pub struct PodObject<T:Pod> {
    pub pod: T,
}

impl<T:Pod> Object for PodObject<T> {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}


pub trait FixedSizeObject: Object<Ptr = ThinPtr> + Sized + Pod {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}

impl<T: Object<Ptr = ThinPtr> + Sized + Copy + Pod> FixedSizeObject for T {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}

pub trait Application {
    type Root: Object + ?Sized;
    type Message: Message<Root = Self::Root>;

    fn initialize_root(ctx: &mut DatabaseContext) -> <Self::Root as Object>::Ptr;
}

#[derive(Copy, Clone, Debug)]
pub struct FatPtr {
    start: usize,
    /// Size in bytes
    len: usize,
}
impl FatPtr {
    /// Start index, and size in bytes
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

    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self {
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
    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self {

        assert_eq!(
            std::mem::size_of::<*const T>(),
            2 * std::mem::size_of::<usize>()
        );

        FatPtr {
            start: ((addr as *const T as *const u8 as usize) - (buffer_start as usize)),
            len: size_of_val(addr),
        }
    }
    fn as_generic(&self) -> GenPtr {
        GenPtr::Fat(*self)
    }
}

impl<T: FixedSizeObject> Object for [T] {
    type Ptr = FatPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_slice(index) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_slice_mut(index) }
    }
}

pub mod data_types {
    use crate::{DatabaseContext, FatPtr, FixedSizeObject, Object, Pointer, SequenceNr, ThinPtr};
    use bytemuck::{Pod, Zeroable};
    use std::marker::PhantomData;
    use std::mem::transmute_copy;
    use std::ops::Deref;
    use sha2::digest::typenum::Zero;

    #[derive(Copy, Clone)]
    #[repr(C)]
    pub struct DatabaseCell<T: Copy> {
        value: T,
        registrar: SequenceNr,
    }

    //TODO: Document. Also rename this or DatabaseCell, the names should harmonize.
    #[derive(Copy, Clone)]
    #[repr(C)]
    pub struct OpaqueCell<T: Copy> {
        value: T,
        registrar: SequenceNr,
    }

    // TODO: The below (and same for DatabaseCell) are probably not actually sound.
    // There could be padding needed. We could avoid this by making sure SequenceNr
    // has alignment 1.
    unsafe impl<T:Pod> Zeroable for OpaqueCell<T> {}
    unsafe impl<T:Pod> Pod for OpaqueCell<T> {}


    pub trait DatabaseCellArrayExt<T:Pod> {
        fn observe(&self, context: &DatabaseContext) -> Vec<T>;
    }
    impl<T:Pod> DatabaseCellArrayExt<T> for &[DatabaseCell<T>] {
        fn observe(&self, context: &DatabaseContext) -> Vec<T> {
            self.iter().map(|x|x.get(context)).collect()
        }
    }


    impl<T: Copy> Deref for DatabaseCell<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.value
        }
    }

    unsafe impl<T> Zeroable for DatabaseCell<T> where T: Pod {}

    unsafe impl<T> Pod for DatabaseCell<T> where T: Pod {}
    impl<T: Pod> DatabaseCell<T> {
        pub fn get(&self, context: &DatabaseContext) -> T {
            //context.observe_registrar(self.registrar);
            self.value
        }
        pub fn get_ref(&self, context: &DatabaseContext) -> &T {
            //context.observe_registrar(self.registrar);
            &self.value
        }
        pub fn set<'a>(&'a mut self, context: &'a DatabaseContext, new_value: T) {
            let index = context.index_of(self);
            //context.write(index, bytes_of(&new_value));
            context.write_pod(new_value, &mut self.value);
            context.update_registrar(&mut self.registrar, false);
        }
    }

    impl<T:Pod> Object for OpaqueCell<T> {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl<T: Pod> OpaqueCell<T> {
        pub fn set<'a>(&'a mut self, context: &'a DatabaseContext, new_value: T) {
            let index = context.index_of(self);
            //context.write(index, bytes_of(&new_value));
            context.write_pod(new_value, &mut self.value);
            context.update_registrar(&mut self.registrar, true);
        }
    }

    impl<T: Pod> DatabaseCell<T> {
        #[allow(clippy::mut_from_ref)]
        pub fn allocate(context: &DatabaseContext) -> &mut Self {
            let memory = unsafe { context.allocate_pod::<DatabaseCell<T>>() };
            unsafe { &mut *(memory as *mut _ as *mut DatabaseCell<T>) }
        }
        pub fn new(value: T) -> DatabaseCell<T> {
            DatabaseCell {
                value,
                registrar: Default::default(),
            }
        }
    }

    impl<T: Pod> Object for DatabaseCell<T> {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
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
    compile_error!("Do we really want to scrap the whole observer-system? Can it be salvaged?
    It appears not.
    ")

    unsafe impl<T> Pod for DatabaseVec<T> where T: 'static {}

    impl<T> DatabaseVec<T>
    where
        T: FixedSizeObject + 'static,
    {
        #[allow(clippy::mut_from_ref)]
        pub fn new(ctx: &DatabaseContext) -> &mut DatabaseVec<T> {
            unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
        }
        pub fn len(&self, ctx: &DatabaseContext) -> usize {
            //ctx.observe_registrar(self.length_registrar);
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
        pub fn write(&mut self, ctx: &mut DatabaseContext, index: usize, val: T) {
            let offset = self.data + index * T::SIZE;
            unsafe {
                *T::access_mut(ctx, ThinPtr(offset)) = val;
            };
        }

        fn realloc_add(&mut self, ctx: &mut DatabaseContext, new_capacity: usize) {
            let dest = ctx.allocate_raw(new_capacity * T::SIZE, T::ALIGN);
            let dest_index = ctx.index_of_ptr(dest);

            if self.length > 0 {
                let old_ptr = FatPtr::from(self.data, T::SIZE * self.length);
                ctx.copy(old_ptr, dest_index.0);
            }

            let new_len = self.length + 1;

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

        pub fn push_zeroed(&mut self, context: &mut DatabaseContext) -> &mut T {
            self.push(context, T::zeroed());
            self.get_mut(context, self.length - 1)
        }
        pub fn push<'a>(&'a mut self, ctx: &mut DatabaseContext, t: T) {
            if self.length >= self.capacity {
                self.realloc_add(ctx, (self.capacity + 1) * 2);
            } else {
                ctx.write_pod(self.length + 1, &mut self.length);
            }
            ctx.update_registrar(&mut self.length_registrar, false);

            self.write(ctx, self.length - 1, t)
        }
    }

    impl<T> Object for DatabaseVec<T>
    where
        T: FixedSizeObject + 'static,
    {
        type Ptr = ThinPtr;
        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }
        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    #[repr(transparent)]
    pub struct DatabaseObjectHandle<T: Object + ?Sized> {
        object_index: T::Ptr,
        phantom: PhantomData<T>,
    }

    unsafe impl<T: Object + ?Sized> Zeroable for DatabaseObjectHandle<T> {}

    impl<T: Object + ?Sized> Copy for DatabaseObjectHandle<T> {}

    impl<T: Object + ?Sized> Clone for DatabaseObjectHandle<T> {
        fn clone(&self) -> Self {
            *self
        }
    }

    unsafe impl<T: Object + ?Sized + 'static> Pod for DatabaseObjectHandle<T> {}
    impl<T: Object + ?Sized + 'static> Object for DatabaseObjectHandle<T> {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }
        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl<T: Object + ?Sized> DatabaseObjectHandle<T> {
        pub fn get(&self, context: &DatabaseContext) -> &T {
            unsafe { T::access(context, self.object_index) }
        }
        pub fn get_mut(&mut self, context: &mut DatabaseContext) -> &mut T {
            unsafe { T::access_mut(context, self.object_index) }
        }

        pub fn new(value: T::Ptr) -> Self {
            Self {
                object_index: value,
                phantom: Default::default(),
            }
        }

        #[allow(clippy::mut_from_ref)]
        pub fn allocate(context: &DatabaseContext, value: T) -> &mut Self
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

        #[allow(clippy::mut_from_ref)]
        pub fn allocate_unsized<'a>(context: &'a DatabaseContext, value: &T) -> &'a mut Self
        where
            T: Object<Ptr = FatPtr> + 'static,
        {
            let size_bytes = std::mem::size_of_val(value);
            let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
            let target_dst_ptr =
                unsafe { context.allocate_raw(size_bytes, std::mem::align_of_val(value)) };

            let target_src_ptr = value as *const T as *const u8;

            let (src_ptr, src_metadata): (*const u8, usize) = unsafe { transmute_copy(&value) };

            unsafe { std::ptr::copy(target_src_ptr, target_dst_ptr, size_bytes) };
            let thin_index = context.index_of_ptr(target_dst_ptr);

            this.object_index = FatPtr::from(thin_index.start(), size_bytes);
            this
        }
    }
}

thread_local! {
    pub(crate) static MULTI_INSTANCE_BLOCKER: Cell<bool> = const { Cell::new(false) };
}

#[derive(Clone)]
pub enum Target {
    OpenExisting(PathBuf),
    CreateNewOrOverwrite(PathBuf),
    CreateNew(PathBuf),
}
impl Target {
    fn path_buf(&mut self) -> &mut PathBuf {
        let (Target::CreateNew(x) | Target::CreateNewOrOverwrite(x) | Target::OpenExisting(x)) =
            self;
        x
    }
    #[must_use]
    pub fn append(&self, path: &str) -> Target {
        let mut temp = self.clone();
        *temp.path_buf() = self.path().join(path);
        temp
    }
    pub fn path(&self) -> &Path {
        let (Target::CreateNew(x) | Target::CreateNewOrOverwrite(x) | Target::OpenExisting(x)) =
            self;
        x
    }
    pub fn create(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_) | Target::CreateNew(_))
    }
    pub fn overwrite(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_))
    }
}

pub mod database {
    use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
    use crate::projector::Projector;
    use crate::{Application, DatabaseContext, MULTI_INSTANCE_BLOCKER, Object, Pointer, SequenceNr, Target, MessageId, MessageComponent, Message};
    use anyhow::{Context, Result};
    use std::path::{Path, PathBuf};
    use crate::disk_access::FileAccessor;
    use crate::update_head_tracker::UpdateHeadTracker;

    pub struct Database<Base: Application> {
        context: DatabaseContext,
        message_store: Projector<Base>,
        app: Base,
    }

    //TODO: Make the modules in this file be distinct files

    impl<APP: Application> Database<APP> {
        pub(crate) fn force_rewind(&mut self, index: SequenceNr) {
            self.context.rewind(index)
        }

        pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
            self.message_store.contains_message(message_id)
        }

        pub fn get_upstream_of(&self, message_id: &[MessageId], count: usize) -> Result<impl Iterator<Item=APP::Message>> {
            self.message_store.get_upstream_of(message_id, count)
        }

        pub fn load_message(&self, message_id: MessageId) -> Result<APP::Message> {
            self.message_store.load_message(message_id)
        }

        pub(crate) fn get_update_heads(&self) -> &[MessageId] {
            self.message_store.get_update_heads()
        }

        pub fn get_root(&self) -> (&APP::Root, &DatabaseContext) {
            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access(&self.context, root_ptr) };
            //let root = self.context.access_pod(root_ptr);
            (root, &self.context)
        }

        pub(crate) fn with_root_mut<R>(
            &mut self,
            f: impl FnOnce(&mut APP::Root, &mut DatabaseContext) -> R,
        ) -> Result<R> {
            if !self.context.mark_dirty()? {
                // Recovery needed
                Self::recover(&mut self.app, &mut self.context, &mut self.message_store)?;
            }

            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access_mut(&mut self.context, root_ptr) };

            let t = f(root, &mut self.context);

            self.context.mark_clean();
            Ok(t)
        }

        fn recover(
            app: &mut APP,
            context: &mut DatabaseContext,
            message_store: &mut Projector<APP>,
        ) -> Result<()> {
            context.clear()?;

            message_store.recover();
            let root_ptr = APP::initialize_root(context);
            context.set_root_ptr(root_ptr.as_generic());

            context.set_next_seqnr(SequenceNr::from_index(0));

            // Safety:
            // Recover is only called when the db is not used
            let root = unsafe { <APP::Root as Object>::access_mut(context, root_ptr) };
            //let root = context.access_pod(root_ptr);
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
        pub fn open(
            path: impl AsRef<Path>,
            app: APP,
            max_file_size: usize,
        ) -> Result<Database<APP>> {
            Self::create(
                app,
                Target::OpenExisting(path.as_ref().to_path_buf()),
                max_file_size,
            )
        }

        pub fn append_single(&mut self, message: APP::Message) -> Result<()> {
            self.append_many(std::iter::once(message))
        }

        pub fn append_many(&mut self, messages: impl Iterator<Item = APP::Message>) -> Result<()> {
            if !self.context.mark_dirty()? {
                // Recovery needed
                Self::recover(&mut self.app, &mut self.context, &mut self.message_store)?;
            }

            self.message_store
                .push_messages(&mut self.context, messages)?;

            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access_mut(&mut self.context, root_ptr) };

            self.message_store
                .apply_missing_messages(root, &mut self.context)?;

            self.context.mark_clean();
            Ok(())
        }

        fn set_multi_instance_block() {
            if MULTI_INSTANCE_BLOCKER.get() {
                if std::env::var("NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE").is_err() {
                    panic!(
                        "Noatun: Multiple active DB-roots in the same thread are not allowed.\
                        You can disable this diagnostic by setting env-var NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE.\
                        Note, unsoundness can occur if DatabaseContext from one instance is used by data \
                        for other."
                    );
                }
            }
            MULTI_INSTANCE_BLOCKER.set(true);
        }

        /// Create a database residing entirely in memory.
        /// This is mostly useful for tests
        pub fn create_in_memory(mut app: APP, max_size: usize) -> Result<Database<APP>> {
            Self::set_multi_instance_block();
            let mut disk = InMemoryDisk::default();
            let target = Target::CreateNew(PathBuf::default());
            let mut ctx = DatabaseContext::new(&mut disk, &target, max_size)
                .context("creating database in memory")?;
            let mut message_store = Projector::new(&mut disk, &target, max_size)?;

            Self::recover(&mut app, &mut ctx, &mut message_store)?;
            ctx.mark_clean()?;

            Ok(Database {
                context: ctx,
                app,
                message_store
            })
        }

        fn create(mut app: APP, target: Target, max_file_size: usize) -> Result<Database<APP>> {
            Self::set_multi_instance_block();
            let mut disk = StandardDisk;

            let mut ctx = DatabaseContext::new(&mut disk, &target, max_file_size)
                .context("opening database")?;

            let is_dirty = ctx.is_dirty();

            let mut message_store = Projector::new(&mut disk, &target, max_file_size)?;
            let mut update_heads = disk.open_file(&target, "update_heads", 0, 128*1024*1024)?;
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


pub mod distributor {
    use std::collections::HashSet;
    use indexmap::{IndexMap, IndexSet};
    use libc::send;
    use savefile_derive::Savefile;
    use crate::{Application, Database, Message, MessageId};
    use anyhow::Result;

    // Principle
    // The node that is 'most ahead' (highest MessageId) has responsibility.
    // If knows all the heads of other node, just sends perfect updates.
    // Otherwise:
    // Must request messages until it has complete picture

    #[derive(Debug, Savefile)]
    pub struct SerializedMessage {
        data: Vec<u8>
    }
    impl SerializedMessage {
        pub fn to_message<M:Message>(&self, data: &[u8]) -> Result<M> {
            M::deserialize(data)
        }
        pub fn new<M:Message>(m: M) -> Result<SerializedMessage> {
            let mut data = vec![];
            m.serialize(&mut data)?;
            Ok(SerializedMessage {
                data
            })
        }
    }


    #[derive(Debug, Savefile)]
    pub struct MessageSubGraphNode {
        id: MessageId,
        parents: Vec<MessageId>
    }

    #[derive(Debug, Savefile)]
    pub enum DistributorMessage {
        /// Report all update heads for the sender
        ReportHeads(Vec<MessageId>),
        /// Report a cut in the source node message graph
        RequestUpstream {
            query: Vec<MessageId>,
            /// How many levels to ascend from message
            count: usize,
        },
        UpstreamResponse {
            query: Vec<MessageId>,
            count: usize,
            messages: Vec<MessageSubGraphNode>,
        },
        /// Command the recipient to send all descendants of the given message
        SendMessageAndAllDescendants {
            message_id: Vec<MessageId>,
        },
        Message(SerializedMessage)
    }

    pub struct Distributor {

    }

    impl Distributor {
        pub fn get_heads<APP:Application>(&self, database: &Database<APP>) -> DistributorMessage {
            DistributorMessage::ReportHeads(
                database.get_update_heads().into_iter().copied().collect()
            )
        }

        pub fn receive_message<APP:Application>(&self, database: &mut Database<APP>, message: DistributorMessage) -> Result<Vec<DistributorMessage>> {
            let mut output = vec![];
            match message {
                DistributorMessage::ReportHeads(messages) => {
                    for message in messages {
                        if database.contains_message(message)? {
                            continue;
                        }
                        output.push(DistributorMessage::RequestUpstream {
                            query: vec![message],
                            count: 4,
                        })
                    }
                }
                DistributorMessage::RequestUpstream { query, count } => {
                    let mut response: IndexMap<MessageId, APP::Message> = IndexMap::new();
                    let messages: Vec<MessageSubGraphNode> = database.get_upstream_of(&query, count)?.map(|msg|{
                        MessageSubGraphNode {
                            id: msg.id(),
                            parents: msg.parents().collect()
                        }
                    }).collect();
                    output.push(DistributorMessage::UpstreamResponse {
                        query,
                        count,
                        messages,
                    })
                }
                DistributorMessage::UpstreamResponse { query, count, messages } => {
                    let mut unknowns: HashSet<MessageId> = HashSet::new();
                    let mut send_cmds = vec![];
                    for msg in messages {
                        if database.contains_message(msg.id)? {
                            continue; //We already have this one
                        }
                        let mut err = Ok(());
                        let have_all_parents = msg.parents.iter().all(|x|
                                                                          database.contains_message(*x).map_err(|e| {err=Err(e);}).is_ok());
                        err?;
                        if have_all_parents
                         {
                            // We have all the parents, a perfect msg to request!
                            send_cmds.push(msg.id);
                            continue;
                        }
                        unknowns.extend(msg.parents);
                    }
                    if send_cmds.is_empty() == false {
                        output.push(DistributorMessage::SendMessageAndAllDescendants { message_id: send_cmds});
                    }
                    if unknowns.is_empty() == false {
                        output.push(DistributorMessage::RequestUpstream {query: unknowns.into_iter().collect(), count: 2*count});
                    }
                }
                DistributorMessage::SendMessageAndAllDescendants { message_id } => {
                    for msg in message_id.iter().map(|x|database.load_message(*x)) {
                        let msg = msg?;
                        output.push(DistributorMessage::Message(SerializedMessage::new(msg)?));
                    }
                }
                DistributorMessage::Message(msg) => {
                    let message = msg.to_message(&msg.data)?;
                    database.append_many(std::iter::once(message))?;
                }
            }

            Ok(output)
        }


    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_access::FileAccessor;
    use crate::projection_store::MainDbHeader;
    use byteorder::{LittleEndian, WriteBytesExt};
    use data_types::DatabaseCell;
    use data_types::DatabaseObjectHandle;
    use data_types::DatabaseVec;
    use database::Database;
    use savefile::{load_noschema, save_noschema};
    use savefile_derive::Savefile;
    use sha2::{Digest, Sha256};
    use std::io::{Cursor, SeekFrom};
    use test::Bencher;
    use crate::data_types::DatabaseCellArrayExt;
    use crate::distributor::DistributorMessage;

    #[test]
    fn test_mmap_big() {
        let mut mmap = FileAccessor::new(
            &Target::CreateNewOrOverwrite("test/mmap_test_big".into()),
            "mmap",
            0,
            1024 * 1024 * 1024,
        );
        //use std::io::Read;
        //let _ =  std::io::stdin().read(&mut [0u8]).unwrap();
    }

    #[test]
    fn test_mmap_helper() {
        let mut mmap = FileAccessor::new(
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
        let initial_ptr = mmap.map_mut_ptr();
        assert_eq!(mmap.read_u64::<LittleEndian>().unwrap(), 0x2c);

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        mmap.write_u8(1).unwrap();
        assert_eq!(initial_ptr, mmap.map_mut_ptr());

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        assert_eq!(mmap.read_u8().unwrap(), 1);

        mmap.flush_all().unwrap();

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
            unimplemented!()
        }

        fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
            std::iter::empty()
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            unimplemented!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            unimplemented!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            unimplemented!()
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

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
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
            unimplemented!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            unimplemented!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            unimplemented!()
        }
    }

    #[test]
    fn test1() {
        let mut db: Database<CounterApplication> =
            Database::create_new("test/test1.bin", CounterApplication, true, 1000).unwrap();

        db.with_root_mut(|counter, context| {
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

        println!("Update heads: {:?}", db.get_update_heads());
        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context| {
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

        db.with_root_mut(|root, context| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_distributor() {
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

        let mut d = distributor::Distributor {};

        println!("Heads: {:?}", d.get_heads(&db));

        let r = d.receive_message(&mut db, DistributorMessage::RequestUpstream {
            query: vec![MessageId::new_debug(0x102)],
            count: 2
        }).unwrap();
        println!("Clarify: {:?}", r);

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context| {
            assert_eq!(root.counter.get(context), 43);
        });
    }


    #[test]
    fn test_handle() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<DatabaseCell<u32>>;
            type Message = DummyMessage<DatabaseObjectHandle<DatabaseCell<u32>>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> ThinPtr {
                let obj = DatabaseObjectHandle::allocate(&ctx, DatabaseCell::new(43u32));

                ctx.index_of(obj)
            }
        }

        let mut db: Database<HandleApplication> =
            Database::create_new("test/test_handle.bin", HandleApplication, true, 1000).unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(handle.get(context).get(context), 43);
    }

    #[test]
    fn test_handle_to_unsized_miri() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<[DatabaseCell<u8>]>;
            type Message = DummyMessage<DatabaseObjectHandle<[DatabaseCell<u8>]>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> ThinPtr {
                let obj = DatabaseObjectHandle::allocate_unsized(&ctx, [
                    43u8, 45
                ].map(|x|DatabaseCell::new(x)).as_slice());

                ctx.index_of(obj)
            }
        }

        let mut db: Database<HandleApplication> =
            Database::create_in_memory(HandleApplication, 1000).unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(handle.get(context).observe(context), &[43, 45]);
    }

    #[test]
    fn test_handle_miri() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<DatabaseCell<u32>>;
            type Message = DummyMessage<DatabaseObjectHandle<DatabaseCell<u32>>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> ThinPtr {
                let obj = DatabaseObjectHandle::allocate(&ctx, DatabaseCell::new(43u32));

                ctx.index_of(obj)
            }
        }

        let mut db: Database<HandleApplication> =
            Database::create_in_memory(HandleApplication, 1000).unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(handle.get(context).get(context), 43);

        db.with_root_mut(|root, context| {
            let a1 = root.get_mut(context);
            assert_eq!(a1.get(context), 43);
        });
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
        db.with_root_mut(|counter_vec, context| {
            assert_eq!(counter_vec.len(context), 0);

            let new_element = counter_vec.push_zeroed(context);
            let new_element = counter_vec.get_mut(context, 0);

            new_element.counter.set(context, 47);
            let new_element = counter_vec.push_zeroed(context);
            new_element.counter.set(context, 48);

            assert_eq!(counter_vec.len(context), 2);

            let item = counter_vec.get_mut(context, 1);
            //let item2 = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
            //assert_eq!(*item2.counter, 48);

            for _ in 0..10 {
                let new_element = counter_vec.push_zeroed(context);
            }

            let item = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
        });
    }

    #[test]
    fn test_vec_miri0() {
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
            Database::create_in_memory(CounterVecApplication, 10000).unwrap();
        db.with_root_mut(|counter_vec, context| {
            assert_eq!(counter_vec.len(context), 0);

            let new_element = counter_vec.push_zeroed(context);
            let new_element = counter_vec.get_mut(context, 0);

            new_element.counter.set(context, 47);
            let new_element = counter_vec.push_zeroed(context);
            new_element.counter.set(context, 48);

            assert_eq!(counter_vec.len(context), 2);

            let item = counter_vec.get_mut(context, 1);
            //let item2 = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
            //assert_eq!(*item2.counter, 48);

            for _ in 0..10 {
                let new_element = counter_vec.push_zeroed(context);
            }

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
            db.with_root_mut(|counter_vec, context| {
                context.set_next_seqnr(SequenceNr::from_index(1));
                assert_eq!(counter_vec.len(context), 0);

                let new_element = counter_vec.push_zeroed(context);
                new_element.counter.set(context, 47);
                new_element.counter2.set(context, 48);

                context.set_next_seqnr(SequenceNr::from_index(2));
                assert_eq!(counter_vec.len(context), 1);
                context.set_next_seqnr(SequenceNr::from_index(3));
            });
        }

        {
            db.with_root_mut(|counter_vec, context| {
                let counter = counter_vec.get_mut(context, 0);
                counter.counter.set(context, 50);
                context.rewind(SequenceNr::from_index(2));
                assert_eq!(counter.counter.get(context), 47);
            });
        }

        db.force_rewind(SequenceNr::from_index(1));

        {
            db.with_root_mut(|counter_vec, context| {
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
