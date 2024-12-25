#![feature(test)]
#![allow(unused)]
#![allow(dead_code)]

extern crate test;

use anyhow::Result;
pub use buffer::DatabaseContext;
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use indexmap::IndexMap;
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::{transmute, transmute_copy};
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::slice::SliceIndex;
use std::time::{Duration, SystemTime};
use fs2::FileExt;
use memmap2::MmapMut;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use crate::disk_abstraction::Disk;
use crate::on_disk_message_store::OnDiskMessageStore;

pub(crate) mod backing_store;

pub(crate) mod buffer;
mod on_disk_message_store;
pub(crate) mod undo_store;
mod disk_abstraction;

struct MessageComponent<const ID: u32, T> {
    value: Option<T>,
}

fn sha2(bytes: &[u8]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    // write input message
    hasher.update(bytes);

    // read hash digest and consume hasher
    hasher.finalize_fixed()[0..16].try_into().unwrap()
}

#[derive(Pod, Zeroable, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
        if self < other {
            self
        } else {
            other
        }
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

pub trait Message {
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

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        &DummyUnitObject
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        // # SAFETY
        // Any dangling pointer is a valid pointer to a zero-sized type
        unsafe { &mut * (std::ptr::dangling_mut() as *mut Self) }
    }
}

/*
struct MessageStore<APP: Application, M: Message<Root = APP::Root>> {
    messages: IndexMap<MessageId, Option<M>>,
    phantom_data: PhantomData<*const APP::Root>,
}


impl<App: Application, M: Message<Root = App::Root>, S:Disk> MessageStore<M, S>
where
    M: Debug,
{
    pub fn new(s:&mut S, target: &Target) -> OnDiskMessageStore<M, S> {
        OnDiskMessageStore::new(s, target)
    }
    fn push_message(&mut self, context: &mut DatabaseContext, message: M) {
        let new_time = message.id();
        let Err(insert_point) = self
            .messages
            .binary_search_by_key(&new_time, |msg_id, _| *msg_id)
        else {
            // Binary search returns Ok when it finds the element exactly.
            // If it does, the message was already in the store, in which case we just successfully
            // do nothing.
            return;
        };
        self.rewind(context, insert_point);
    }
    fn rewind(&mut self, context: &mut DatabaseContext, point: usize) {
        context.rewind(SequenceNr::from_index(point));
        let mut index = 0;
        // TODO: This is inefficient
        self.messages.retain(|_, value| {
            if index < point {
                index += 1;
                return true;
            }
            !value.is_none()
        });
    }
    fn apply_single_message(
        context: &mut DatabaseContext,
        root: &mut App::Root,
        msg: &M,
        seqnr: SequenceNr,
    ) {
        context.set_next_seqnr(seqnr); //TODO: Don't record a snapshot for _every_ message.
        msg.apply(context, root); //TODO: Handle panics in apply gracefully
        context.finalize_message();
    }
    fn apply_missing_messages(&mut self, database: &mut Database<App>) {
        let (root, context) = database.get_root();
        for (seq, (message_id, msg)) in self
            .messages
            .iter()
            .enumerate()
            .skip(context.next_seqnr().try_index().unwrap_or(0))
        {
            let Some(msg) = msg else {
                continue;
            };
            let seqnr = SequenceNr::from_index(seq);
            Self::apply_single_message(context, root, &msg, seqnr);
        }
        context.set_next_seqnr(SequenceNr::from_index(self.messages.len()));
        let must_remove = context.finalize_transaction(&self.messages);
        for index in must_remove {
            println!("Removing message {:?}", index);
            *self.messages.get_index_mut(index.index()).unwrap().1 = None;
        }
        context.set_next_seqnr(SequenceNr::INVALID);
    }
}
*/
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
    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self;
    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self;
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

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

impl Object for u32 {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
impl Object for u8 {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
pub trait Application {
    type Root: Object;
    fn initialize_root(ctx: &mut DatabaseContext) -> &mut Self::Root;
    fn get_root<'a>(
        &'a mut self,
        ctx: &mut DatabaseContext,
        root_ptr: <Self::Root as Object>::Ptr,
    ) -> &'a mut Self::Root;
}

pub struct Database<Base> {
    context: DatabaseContext,
    app: Base,
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

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access(index) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
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
    pub fn new<'a>(ctx: &mut DatabaseContext) -> &'a mut DatabaseVec<T> {
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

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { &*(context.start_ptr().wrapping_add(index.0) as *const DatabaseVec<T>) }
    }
    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
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

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe {
            &*(context.start_ptr().wrapping_add(index.0) as *const _
                as *const DatabaseObjectHandle<T>)
        }
    }
    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
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
    pub fn new<'a>(context: &mut DatabaseContext, value: T) -> &'a mut Self
    where
        T: Object<Ptr = ThinPtr>,
        T: Pod,
    {
        let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
        let target = unsafe { context.allocate_pod::<T>() };
        *target = value;
        this.object_index = context.index_of_ptr(target as *const _);
        this
    }
}

impl<T: Pod + Object> DatabaseCell<T> {
    pub fn new<'a>(context: &mut DatabaseContext) -> &'a mut Self {
        let memory = unsafe { context.allocate_pod::<T>() };
        unsafe { &mut *(memory as *mut _ as *mut DatabaseCell<T>) }
    }
}

impl<T: Pod> Object for DatabaseCell<T> {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
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
    CreateNew(PathBuf)
}
impl Target {
    pub fn path(&self) -> &Path {
        let (Target::CreateNew(x)|Target::CreateNewOrOverwrite(x)|Target::OpenExisting(x)) = self;
        &*x
    }
    pub fn create(&self) -> bool {
        matches!(self,  Target::CreateNewOrOverwrite(_)|Target::CreateNew(_))
    }
    pub fn overwrite(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_))
    }
}
impl<APP: Application> Database<APP> {
    pub fn get_root<'a>(&'a mut self) -> (&'a mut APP::Root, &'a mut DatabaseContext) {
        let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
        let root = APP::get_root(&mut self.app, &mut self.context, root_ptr);
        (root, &mut self.context)
    }

    pub fn create_new(path: impl AsRef<Path>, app: APP, overwrite_existing:bool) -> Result<Database<APP>> {
        Self::create(app,
                     if overwrite_existing
                        {Target::CreateNewOrOverwrite(path.as_ref().to_path_buf()) }
                     else
                     {
                        Target::CreateNew(path.as_ref().to_path_buf())
                     }
        )
    }
    pub fn open(path: impl AsRef<Path>, app: APP) -> Result<Database<APP>> {
        Self::create(app, Target::OpenExisting(path.as_ref().to_path_buf())
                     )
    }
    fn create(app: APP, target: Target) -> Result<Database<APP>> {
        if MULTI_INSTANCE_BLOCKER.get() {
            if !std::env::var("NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE").is_ok() {
                panic!(
                    "Noatun: Multiple active DB-roots in the same thread are not allowed.\
                        You can disable this warning by setting env-var NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE.\
                        Note, unsoundness can occur if DatabaseContext from one instance is used by data \
                        for other."
                );
            }
        }
        MULTI_INSTANCE_BLOCKER.set(true);

        let mut ctx = DatabaseContext::new(&target)?;
        let start_ptr = ctx.start_ptr();
        let root = APP::initialize_root(&mut ctx);
        let root_ptr = <APP::Root as Object>::Ptr::create(root, start_ptr);
        ctx.set_root_ptr(root_ptr.as_generic());
        Ok(Database { context: ctx, app })
    }
}
impl<APP> Drop for Database<APP> {
    fn drop(&mut self) {
        MULTI_INSTANCE_BLOCKER.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use test::Bencher;

    #[derive(Clone, Copy, Zeroable, Pod)]
    #[repr(C)]
    struct CounterObject {
        counter: DatabaseCell<u32>,
        counter2: DatabaseCell<u32>,
    }

    impl Object for CounterObject {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index.0) }
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
        fn new<'a, 'b: 'a>(ctx: &'b mut DatabaseContext) -> &'a mut CounterObject {
            let counter: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
            assert_eq!(ctx.index_of(counter).0, 0);
            let counter2: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
            assert_eq!(ctx.index_of(counter2).0, 4);
            unsafe { Self::access_mut(ctx, ThinPtr(0)) }
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root = CounterObject;

        fn initialize_root(ctx: &mut DatabaseContext) -> &mut CounterObject {
            let new_obj = CounterObject::new(ctx);
            //assert_eq!(ctx.index_of(&new_obj.counter), 0);
            new_obj
        }

        fn get_root<'a>(
            &'a mut self,
            ctx: &mut DatabaseContext,
            ptr: <CounterObject as Object>::Ptr,
        ) -> &'a mut Self::Root {
            unsafe { CounterObject::access_mut(ctx, ptr) }
        }
    }

    struct IncrementMessage {
        increment_by: u32,
    }

    #[test]
    fn test1() {
        let mut db: Database<CounterApplication> =
            Database::create_new("test/test1.bin", CounterApplication, true).unwrap();

        let context = &db.context;

        let (counter, context) = db.get_root();
        context.set_next_seqnr(SequenceNr::from_index(0));
        assert_eq!(counter.counter.get(context), 0);
        counter.counter.set(context, 42);
        counter.counter2.set(context, 43);
        counter.counter.set(context, 44);

        assert_eq!(*counter.counter, 44);
        assert_eq!(counter.counter.get(context), 44);
        assert_eq!(counter.counter2.get(context), 43);
    }

    #[derive(Debug)]
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
            todo!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            todo!()
        }
    }

    #[test]
    fn test_msg_store() {
        let mut db: Database<CounterApplication> =
            Database::create_new("test/msg_store.bin", CounterApplication, true).unwrap();
        let mut messages = MessageStore::new();
        messages.messages.insert(
            MessageId::new_debug(0x100),
            Some(CounterMessage {
                parent: None,
                id: MessageId::new_debug(0x100),
                inc1: 2,
                set1: 0,
            }),
        );
        messages.messages.insert(
            MessageId::new_debug(0x101),
            Some(CounterMessage {
                parent: Some(MessageId::new_debug(0x100)),
                id: MessageId::new_debug(0x101),
                inc1: 0,
                set1: 42,
            }),
        );
        messages.messages.insert(
            MessageId::new_debug(0x102),
            Some(CounterMessage {
                parent: Some(MessageId::new_debug(0x101)),
                id: MessageId::new_debug(0x102),
                inc1: 1,
                set1: 0,
            }),
        );

        messages.apply_missing_messages(&mut db);

        let (counter, context) = db.get_root();
        assert_eq!(counter.counter.get(context), 43);
    }

    #[test]
    fn test_handle() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<u32>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut DatabaseObjectHandle<u32> {
                DatabaseObjectHandle::new(ctx, 43u32)
            }

            fn get_root<'a>(
                &'a mut self,
                ctx: &mut DatabaseContext,
                ptr: <DatabaseObjectHandle<u32> as Object>::Ptr,
            ) -> &'a mut Self::Root {
                unsafe { DatabaseObjectHandle::access_mut(ctx, ptr) }
            }
        }

        let mut db: Database<HandleApplication> =
            Database::create_new("test/test_handle.bin", HandleApplication, true).unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(*handle.get(context), 43);
    }

    #[test]
    fn test_vec0() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut DatabaseVec<CounterObject> {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                obj
            }

            fn get_root<'a>(
                &'a mut self,
                ctx: &mut DatabaseContext,
                ptr: <DatabaseVec<CounterObject> as Object>::Ptr,
            ) -> &'a mut Self::Root {
                unsafe { DatabaseVec::access_mut(ctx, ThinPtr(0)) }
            }
        }

        let mut db: Database<CounterVecApplication> =
            Database::create_new("test/test_vec0", CounterVecApplication, true).unwrap();
        let (counter_vec, context) = db.get_root();
        context.set_next_seqnr(SequenceNr::from_index(0));
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
    }

    #[test]
    fn test_vec_undo() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut DatabaseVec<CounterObject> {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                obj
            }

            fn get_root<'a>(
                &'a mut self,
                ctx: &mut DatabaseContext,
                ptr: <DatabaseVec<CounterObject> as Object>::Ptr,
            ) -> &'a mut Self::Root {
                unsafe { DatabaseVec::access_mut(ctx, ThinPtr(0)) }
            }
        }

        let mut db: Database<CounterVecApplication> =
            Database::create_new("test/vec_undo", CounterVecApplication, true).unwrap();

        {
            let (counter_vec, context) = db.get_root();
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
        }

        {
            let (counter_vec, context) = db.get_root();
            let counter = counter_vec.get_mut(context, 0);
            counter.counter.set(context, 50);
            context.rewind(SequenceNr::from_index(2));
            assert_eq!(counter.counter.get(context), 47);
        }

        println!("Rewind!");
        db.context.rewind(SequenceNr::from_index(1));

        {
            let (counter_vec, context) = db.get_root();
            assert_eq!(counter_vec.len(context), 0);
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
