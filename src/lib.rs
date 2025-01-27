#![allow(unused)]
#![allow(dead_code)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::needless_question_mark)]
#![allow(clippy::bool_comparison)]
// At some point we might remove this and actually analyse each individual site that
// produces this warning.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::type_complexity)]
// Yeah, this is not ideal. This should be fixed.
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::let_and_return)]

use std::any::Any;
pub use crate::data_types::{DatabaseCell, DatabaseVec};
use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
use crate::message_store::OnDiskMessageStore;
use crate::platform_specific::{get_boot_time, FileMapping};
use crate::projector::Projector;
use crate::sequence_nr::SequenceNr;
use crate::sha2_helper::sha2;
use anyhow::{bail, Context, Result};
use bumpalo::Bump;
pub use bytemuck::{AnyBitPattern, Pod, Zeroable};
use chrono::{DateTime, SecondsFormat, Utc};
pub use database::Database;
use fs2::FileExt;
use indexmap::IndexMap;
use memmap2::MmapMut;
pub use projection_store::DatabaseContextData;
use rand::RngCore;
use savefile::Deserializer;
use savefile_derive::Savefile;
use serde::{Deserialize, Serialize};
pub use serde_derive;
use std::cell::{Cell, OnceCell};
use std::ffi::c_void;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::{transmute, transmute_copy};
use std::ops::{Add, Deref, Range};
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::{null, null_mut};
use std::{mem, slice};
use std::borrow::Borrow;
use std::slice::SliceIndex;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

mod disk_abstraction;
mod message_store;
mod projection_store;
mod undo_store;

pub mod prelude {}
#[cfg(feature = "tokio")]
pub mod communication;

pub mod distributor;
pub mod sequence_nr;

pub(crate) mod cutoff;
pub mod data_types;
pub mod database;
mod projector;
mod update_head_tracker;

struct MessageComponent<const ID: u32, T> {
    value: Option<T>,
}

pub(crate) mod platform_specific;

mod boot_checksum;
pub(crate) mod disk_access;
mod sha2_helper;

thread_local! {
    pub static CONTEXT: Cell<*mut DatabaseContextData> = const { Cell::new(null_mut()) };
}

#[derive(Clone, Copy)]
pub struct NoatunContext;

fn get_context_mut_ptr() -> *mut DatabaseContextData {
    let context_ptr = CONTEXT.get();
    if context_ptr.is_null() {
        panic!("No mutable NoatunContext is presently available on this thread");
    }
    context_ptr
}
fn get_context_ptr() -> *const DatabaseContextData {
    let context_ptr = CONTEXT.get();
    if context_ptr.is_null() {
        panic!("No NoatunContext is presently available on this thread");
    }
    context_ptr
}

/// This represents a type that has no detached representation.
/// Instances of this type cannot be created.
pub enum Undetachable {}

impl NoatunContext {
    pub fn start_ptr_mut(self) -> *mut u8 {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).start_ptr_mut() }
    }
    pub(crate) fn clear_unused_tracking(self) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).clear_unused_tracking() }
    }

    pub fn index_of<T: Object + ?Sized>(self, t: &T) -> T::Ptr {
        let context_ptr = get_context_ptr();
        unsafe { (*context_ptr).index_of(t) }
    }

    // TODO: This should almost certainly NOT exist here.
    // It's used in a very early test, from before we had the architecture down
    #[doc(hidden)]
    pub(crate) unsafe fn rewind(self, new_time: SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).rewind(new_time) }
    }

    pub fn set_next_seqnr(self, seqnr: SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).set_next_seqnr(seqnr) }
    }
    pub fn copy(&self, src: FatPtr, dest_index: ThinPtr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).copy(src, dest_index) }
    }
    pub fn copy_sized(&self, src: ThinPtr, dest_index: ThinPtr, size_bytes: usize) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).copy_sized(src, dest_index, size_bytes) }
    }
    pub fn copy_pod<T: Pod>(&self, src: &T, dst: &mut T) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).copy_pod(src, dst) }
    }

    pub fn index_of_ptr(&self, ptr: *const u8) -> ThinPtr {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*context_ptr).index_of_ptr(ptr) }
    }
    pub fn allocate_raw(&self, size: usize, align: usize) -> *mut u8 {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).allocate_raw(size, align) }
    }
    pub fn update_registrar(&self, registrar: &mut SequenceNr, value: bool) {
        let context_ptr = get_context_mut_ptr();
        unsafe {
            (*context_ptr).update_registrar(registrar, value);
        }
    }
    pub fn write_pod<T: Pod>(&self, value: T, dest: Pin<&mut T>) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_pod(value, dest) }
    }
    pub(crate) fn write_pod_internal<T: Pod>(&self, value: T, dest: &mut T) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_pod(value, Pin::new_unchecked(dest)) }
    }
    pub fn write_object<T: FixedSizeObject>(&self, value: T, dest: Pin<&mut T>) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_object(value, dest) }
    }
    pub fn write_pod_ptr<T: Pod>(&self, value: T, dest: *mut T) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_pod_ptr(value, dest) }
    }
    pub fn allocate_pod<'a, T: AnyBitPattern>(&self) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).allocate_pod() }
    }
    pub unsafe fn access_pod_mut<'a, T: Pod>(&mut self, ptr: ThinPtr) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).access_pod_mut(ptr) }
    }
    pub unsafe fn access_object_mut<'a, T: FixedSizeObject>(&mut self, ptr: ThinPtr) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).access_object_mut(ptr) }
    }
    pub unsafe fn access_pod<'a, T: AnyBitPattern>(&self, ptr: ThinPtr) -> &'a T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*context_ptr).access_pod(ptr) }
    }
    pub unsafe fn access_object<'a, T: FixedSizeObject>(&self, ptr: ThinPtr) -> &'a T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*context_ptr).access_object(ptr) }
    }

    pub fn observe_registrar(self, registrar: SequenceNr) {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            return;
        }
        if unsafe{(*(context_ptr as *const DatabaseContextData)).is_mutable == false} {
            return;
        }
        unsafe { (*context_ptr).observe_registrar(registrar) }
    }
    pub unsafe fn access_pod_slice<'a, T: Pod>(self, range: FatPtr) -> &'a [T] {
        let p = CONTEXT.get();
        if p.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*p).access_slice(range) }
    }
    pub unsafe fn access_pod_slice_mut<'a, T: Pod>(self, range: FatPtr) -> Pin<&'a mut [T]> {
        let context_ptr = get_context_mut_ptr();
        unsafe { Pin::new_unchecked( (*context_ptr).access_slice_mut(range)) }
    }
    pub unsafe fn access_object_slice<'a, T: FixedSizeObject>(self, range: FatPtr) -> &'a [T] {
        let p = CONTEXT.get();
        if p.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*p).access_object_slice(range) }
    }
    pub unsafe fn access_object_slice_mut<'a, T: FixedSizeObject>(self, range: FatPtr) -> Pin<&'a mut [T]> {
        let context_ptr = get_context_mut_ptr();
        unsafe { Pin::new_unchecked( (*context_ptr).access_object_slice_mut(range)) }
    }
}

#[derive(Pod, Zeroable, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Savefile)]
#[repr(transparent)]
pub struct MessageId {
    data: [u32; 4],
}

const ASSURE_SUPPORTED_USIZE: () = const {
    if size_of::<usize>() != 8 {
        panic!("noatun currently only supports 64 bit platforms with 64 bit usize");
    }
};

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time_ms = self.timestamp();

        let time = chrono::DateTime::from_timestamp_millis(time_ms as i64).unwrap();

        let time_str = time.to_rfc3339_opts(SecondsFormat::Millis, true);
        write!(
            f,
            "{:?}-{:x}-{:x}-{:x}",
            time_str,
            (self.data[1] & 0xffff0000) >> 16,
            self.data[2],
            self.data[3]
        )
    }
}

impl Debug for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if cfg!(test) && self.data[0] == 0 {
            write!(
                f,
                "#{:x}_{:x}_{:x}",
                self.data[1], self.data[2], self.data[3]
            )
        } else {
            write!(f, "{}", self)
        }
    }
}

impl MessageId {
    pub const ZERO: MessageId = MessageId { data: [0u32; 4] };
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

    /// Next larger MessageId.
    /// Panics if current id is the largest possible.
    /// Note: This *CAN* change the timestamp of the message, though this is unlikely.
    /// Note: The timestamp can at most increase by 1 ms.
    // TODO: Possibly add test-feature that will randomly change the timestamp, just for testing.
    pub fn successor(&self) -> MessageId {
        let mut temp = *self;
        for element in temp.data.iter_mut().rev() {
            if *element < u32::MAX {
                *element += 1;
                return temp;
            }
            *element = 0;
        }
        panic!("successor() invoked on MessageId::MAX");
    }

    /// Create an artificial MessageId, mostly useful for tests and possibly debugging.
    pub fn new_debug(nr: u32) -> Self {
        Self {
            data: [0, 0, 0, nr],
        }
    }

    pub fn generate_for_time(time: DateTime<Utc>) -> Result<MessageId> {
        let mut random_part = [0u8; 10];
        rand::thread_rng().fill_bytes(&mut random_part);
        Self::from_parts(time, random_part)
    }
    pub fn from_parts_for_test(time: DateTime<Utc>, random: u64) -> MessageId {
        let mut data = [0u8; 10];
        data[2..10].copy_from_slice(&random.to_le_bytes());

        Self::from_parts(time, data).unwrap()
    }
    pub fn timestamp(&self) -> u64 {
        let restes = (self.data[0] as u64) + (((self.data[1] & 0xffff) as u64) << 32);
        restes
    }
    pub fn from_parts(time: DateTime<Utc>, random: [u8; 10]) -> Result<MessageId> {
        let t: u64 = time
            .timestamp_millis()
            .try_into()
            .context("Time value is out of range. Value must be ")?;
        if t >= 1 << 48 {
            bail!("Time value is too large");
        }
        let mut data = [0u8; 16];
        data[0..6].copy_from_slice(&t.to_le_bytes()[0..6]);
        data[6..16].copy_from_slice(&random);

        Ok(MessageId {
            data: bytemuck::cast(data),
        })
    }
    pub fn from_parts_raw(time: u64, random: [u8; 10]) -> Result<MessageId> {
        if time >= 1 << 48 {
            bail!("Time value is too large");
        }
        let mut data = [0u8; 16];
        data[0..6].copy_from_slice(&time.to_le_bytes()[0..6]);
        data[6..16].copy_from_slice(&random);

        Ok(MessageId {
            data: bytemuck::cast(data),
        })
    }
}

#[derive(Clone, Copy, Pod, Zeroable,PartialEq,Eq,PartialOrd,Ord,Hash, serde_derive::Serialize, serde_derive::Deserialize, Savefile)]
#[repr(C)]
pub struct NoatunTime(pub u64);

impl Display for NoatunTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time = chrono::DateTime::from_timestamp_millis(self.0 as i64).unwrap();

        let time_str = time.to_rfc3339_opts(SecondsFormat::Millis, true);
        write!(f, "{}", time_str)
    }
}
impl Debug for NoatunTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time = chrono::DateTime::from_timestamp_millis(self.0 as i64).unwrap();

        let time_str = time.to_rfc3339_opts(SecondsFormat::Millis, true);
        write!(f, "{}", time_str)
    }
}

impl NoatunTime {
    pub fn successor(&self) -> NoatunTime {
        NoatunTime(self.0 + 1)
    }
    pub const ZERO: NoatunTime = NoatunTime(0);
    pub const MAX: NoatunTime = NoatunTime(u64::MAX);

    pub fn now() -> Self {
        Self(Utc::now().timestamp_millis() as u64)
    }

    #[must_use]
    pub fn as_ms(self) -> u64 {
        self.0
    }
    #[must_use]
    pub fn add_ms(self, ms: u64) -> NoatunTime {
        NoatunTime(self.0.saturating_add(ms))
    }
    pub fn sub_ms(self, ms: u64) -> NoatunTime {
        NoatunTime(self.0.saturating_sub(ms))
    }
    pub fn to_datetime(&self) -> DateTime<Utc> {
        //TODO: Don't panic here!
        DateTime::<Utc>::from_timestamp_millis(self.0 as i64).unwrap()
    }
    pub fn from_datetime(t: DateTime<Utc>) -> NoatunTime {
        NoatunTime(t.timestamp_millis() as u64)
    }
}

//TODO: Do away with the Clone-bound
pub trait MessagePayload: Debug  {
    type Root: Object;
    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>);

    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;
    fn serialize<W: Write>(&self, writer: W) -> Result<()>;
}

#[derive(Debug,Clone)]
pub struct MessageHeader {
    pub id: MessageId,
    pub parents: Vec<MessageId>,
}

#[derive(Debug)]
pub struct Message<M: MessagePayload> {
    pub header: MessageHeader,
    pub payload: M,
}

impl<M: MessagePayload> Message<M> {
    pub fn new(id: MessageId, parents: Vec<MessageId>, payload: M) -> Self {
        Self {
            header: MessageHeader { id, parents },
            payload,
        }
    }
    pub fn id(&self) -> MessageId {
        self.header.id
    }
}

/// A state-less object, mostly useful for testing
#[derive(Clone, Copy, AnyBitPattern)]
#[repr(C)]
pub struct DummyUnitObject;

impl Object for DummyUnitObject {
    type Ptr = ThinPtr;
    type DetachedType = ();
    type DetachedOwnedType = ();

    fn detach(&self) -> Self::DetachedType {
        ()
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {}

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        unsafe { Pin::new_unchecked(&mut *(1usize as *mut DummyUnitObject)) }
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        &DummyUnitObject
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        // # SAFETY
        // Any dangling pointer is a valid pointer to a zero-sized type
        unsafe { Pin::new_unchecked(&mut *(1usize as *mut DummyUnitObject)) }
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
    fn is_null(&self) -> bool;
}



pub fn from_bytes_mut<T: FixedSizeObject>(s: &mut [u8]) -> &mut T {
    assert_eq!(s.len(), size_of::<T>());
    assert!( (s.as_mut_ptr() as *mut T).is_aligned());

    // # Safety
    // We've checked alignment and size, and those are the only requirements
    // an Object need to be valid.
    unsafe { transmute::<*mut u8, &mut T>(s.as_mut_ptr()) }
}
pub fn bytes_of<T: FixedSizeObject>(t: &T) -> &[u8] {
    // # Safety
    // FixedSizeObject instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe {
        slice::from_raw_parts(t as *const _ as *const u8, size_of::<T>())
    }
}

/// # Safety
/// To implement this safely:
///  * Self must be repr(C) or repr(transparent). repr(packed) may work, but
///    causes composability problems, as types that own Self may then also need to
///    be packed.
///  * Self must not utilize any niches in owned objects. This is because the undo-function
///    of noatun will overwrite such niches during undo.
///  * Self can have niches, but noatun guarantees they will never be used and Self is
///    allowed to clobber them.
///  * All fields of Self must implement Object
///  * Self must not be Sync or Send.
///  * Self must not be Unpin.
///
/// TLDR:
///  * Use repr(C)
///  * Make sure all your fields also implement Object.
///
/// # Note on Pin::set
/// Pin::set might seem to be a problem:
/// It allows the user to overwrite the target of a pinned ptr, in a for-noatun-unobservable way.
/// However:
/// 1: Just don't do that!
/// 2: In order to be able to call Pin::set, the user needs to have an owned value of
///    an Object-type. This should be impossible to obtain.
/// Therefore, any type that implements Object must make sure that safe
/// code cannot obtain an owned instance of Self:
///   * No Default-impl!
///   * No new() impl (though DetachedType can of course have new())
pub trait Object {
    /// This is meant to be either ThinPtr for sized objects, or
    /// FatPtr for dynamically sized objects. Other types are likely to not make sense.
    type Ptr: Pointer;
    type DetachedType: ?Sized;
    type DetachedOwnedType : Borrow<Self::DetachedType>;

    fn detach(&self) -> Self::DetachedOwnedType;

    /// Initialize all the fields in 'self' from the given 'detached' type.
    /// The detached type is a regular rust pod struct, with no requirements
    /// on alignment, pinning or similar. It can therefore be passed around freely,
    /// being more convenient to use for initialization.
    ///
    /// Note that you don't _have_ to use this method, it's perfectly fine
    /// to initialize all Object's "in place", after constructing/allocating default
    /// versions of them.
    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType);

    /// This can in most cases be:
    /// ```dontrun
    ///    let ret: &mut Self = NoatunContext.allocate_pod();
    ///    ret.init_from_detached(detached);
    ///    ret
    /// ```
    /// The only cases where some other implementation is required is when 'Self' does
    /// not have a fixed size.
    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self>;

    /// Access a shared instance of Self at the given pointer address.
    ///
    /// # Safety
    /// The caller must ensure that the accessed object is not aliased with a mutable
    /// reference to the same object.
    ///
    /// Note! Instances of Object must only be created inside the database, and
    /// carefully shepherded so that they do not escape! All callers must ensure that
    /// the lifetime 'a' ends up bound to that of the Object that owns the returned instance,
    /// which must ultimately be bounded by the lifetime of the root object!
    // TODO: Don't expose these methods. Too hard to use correctly!
    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self;
    /// Access a mutable instance of Self at the given pointer address.
    /// NOTE!
    /// Self must not allow direct access to any of its fields. Self must
    /// provide methods that can be used to mutate the fields, and those methods
    /// must report all writes to the DatabaseContext.
    ///
    /// NOTE!
    /// The above holds for all places that may be reachable through Self, not only direct
    /// fields on Self. It also holds for collections or any other data. I.e, if Self
    /// is a collection type, it cannot give direct mutable access to a u8 element, or similar.
    ///
    /// # Safety
    /// The caller must ensure that the accessed object is not aliased with any other
    /// reference to the same object.
    ///
    /// Note! Instances of Object must only be created inside the database, and
    /// carefully shepherded so that they do not escape! All callers must ensure that
    /// the lifetime 'a' ends up bound to that of the Object that owns the returned instance,
    /// which must ultimately be bounded by the lifetime of the root object!
    // TODO: Don't expose these methods. Too hard to use correctly!
    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self>;
}

#[macro_export]
macro_rules! noatun_object {

    ( bounded_type pod $typ:ty) => {
        $crate::DatabaseCell<$typ>
    };
    ( declare_field pod $typ: ty ) => {
        $crate::DatabaseCell<$typ>
    };
    ( declare_detached_field pod $typ: ty ) => {
        $typ
    };
    ( new_declare_param pod $typ: ty ) => {
        $typ
    };
    ( new_assign_field pod $self: ident $name: ident $typ: ty ) => {
        unsafe { ::std::pin::Pin::new_unchecked(&mut $self.$name).set($name); }
    };
    ( getter pod $name:ident $typ: ty  ) => {
        pub fn $name(&self) -> $typ {
            self.$name.get()
        }
    };
    ( setter pod $name:ident $typ: ty  ) => {
        $crate::paste!(
            pub fn [<set_ $name>](self: ::std::pin::Pin<&mut Self>, val: $typ) {
                unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().$name).set(val); }
            }
        );
    };

    ( bounded_type object $typ:ty) => {
        $typ
    };
    ( declare_field object $typ: ty ) => {
        $typ
    };
    ( declare_detached_field object $typ: ty ) => {
        <$typ as $crate::Object>::DetachedOwnedType
    };
    ( new_declare_param object $typ: ty ) => {
        &<$typ as $crate::Object>::DetachedType
    };
    ( new_assign_field object $self:ident $name: ident $typ: ty ) => {
        unsafe { <_ as $crate::Object>::init_from_detached(Pin::new_unchecked(&mut $self.$name), $name); }
    };
    ( getter object $name:ident $typ: ty  ) => {
        pub fn $name(&self) -> &$typ {
            &self.$name
        }
    };
    ( setter object $name:ident $typ: ty  ) => {
        $crate::paste!(
            pub fn [<$name _mut>](self: ::std::pin::Pin<&mut Self>) -> ::std::pin::Pin<&mut $typ> {
                let tself = unsafe { self.get_unchecked_mut() };
                unsafe { ::std::pin::Pin::new_unchecked(&mut tself.$name) }
            }
        );
    };

    ( declare_detached_struct $n_detached:ident fields $( $kind:ident $name: ident $typ:ty ),* ) => {
        #[derive(Debug,Clone,$crate::serde_derive::Serialize, $crate::serde_derive::Deserialize, Savefile)]

        pub struct $n_detached
        {
            $( $name : noatun_object!(declare_detached_field $kind $typ) ),*
        }
    };

    ( detached_type $n_detached: ident) => {
        $n_detached
    };

    ( struct $n:ident { $( $kind:ident $name: ident : $typ:ty $(,)* )* } $(;)* ) => {


            #[derive(Debug,Copy,Clone, $crate::AnyBitPattern)]
            #[repr(C)]
            pub struct $n where $( noatun_object!(bounded_type $kind $typ) : $crate::Object ),*
            {
                phantom: ::std::marker::PhantomPinned,
                $(
                    #[doc(hidden)]
                    $name : noatun_object!(declare_field $kind $typ)
                ),*
            }
            $crate::paste!(
                pub struct [<$n PinProject>]<'a> {
                    $(
                        $name: ::std::pin::Pin<&'a mut noatun_object!(declare_field $kind $typ)>
                    ),*
                }
            );


            impl $n {

                pub fn init(
                    &mut self,
                    $( $name: noatun_object!(new_declare_param $kind $typ) ),*
                    ) {
                    $( noatun_object!(new_assign_field $kind self $name $typ); )*
                }

                $( noatun_object!(getter $kind $name $typ); )*


                $(
                    noatun_object!(setter $kind $name $typ);
                )*

                $crate::paste! {
                    pub fn pin_project<'a>(self: Pin<&'a mut Self>) -> [<$n PinProject>]<'a> {
                        unsafe {
                            let $n {
                                $($name),* , ..
                            } = self.get_unchecked_mut();

                            [<$n PinProject>] {
                                $($name: Pin::new_unchecked($name)),*

                            }
                        }
                    }
                }

            }

            $crate::paste! {
                noatun_object!{declare_detached_struct [<$n Detached>] fields $($kind $name $typ),*}
            }


            impl $crate::Object for $n {
                type Ptr = $crate::ThinPtr;
                type DetachedType = $crate::paste!(noatun_object!(detached_type [<$n Detached>]));
                type DetachedOwnedType = $crate::paste!(noatun_object!(detached_type [<$n Detached>]));


                fn detach(&self) -> Self::DetachedOwnedType {
                    Self::DetachedOwnedType {
                        $(
                            $name: self.$name.detach()
                        ),*
                    }
                }

                fn init_from_detached(mut self: ::std::pin::Pin<&mut Self>, detached: &Self::DetachedType) {
                    $(
                    unsafe {
                        Pin::new_unchecked(&mut self.as_mut().get_unchecked_mut().$name).init_from_detached(&detached.$name);

                    }
                    )*
                }

                unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> ::std::pin::Pin<&'a mut Self> {
                    let mut ret: ::std::pin::Pin<&mut Self> = NoatunContext.allocate_pod();
                    ret.as_mut().init_from_detached(detached);
                    ret
                }

                unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
                    unsafe { NoatunContext.access_object(index) }
                }

                unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
                    unsafe { NoatunContext.access_object_mut(index) }
                }
            }


    };

}

pub trait FixedSizeObject: Object<Ptr = ThinPtr> + Sized + AnyBitPattern {}

impl<T: Object<Ptr = ThinPtr> + Sized + Copy + AnyBitPattern> FixedSizeObject for T {}

pub trait Application: Object {
    type Message: MessagePayload<Root = Self>;
    /// Parameters that will be available in the "initialize_root" call.
    type Params;

    fn initialize_root<'a>(params: &Self::Params) -> Pin<&'a mut Self>;
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
// TODO: We should probably have a generic ThinPtr type, like ThinPtr<T>,
// that allows type-safe access to &mut T
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

    fn is_null(&self) -> bool {
        self.0 == 0
    }
}

impl ThinPtr {
    pub fn null() -> ThinPtr {
        ThinPtr(0)
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

    fn is_null(&self) -> bool {
        self.start == 0
    }
}

impl<T: FixedSizeObject> Object for [T] where T::DetachedType: Sized {
    type Ptr = FatPtr;
    type DetachedType = [T::DetachedOwnedType];
    type DetachedOwnedType = Vec<T::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter().map(|x|x.detach()).collect()
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        unsafe {
            for (dst, src) in self.get_unchecked_mut().iter_mut().zip(detached.into_iter()) {
                Pin::new_unchecked(dst).init_from_detached(src.borrow());
            }
        }
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let bytes = size_of::<T>() * detached.len();
        let alloc = NoatunContext.allocate_raw(bytes, align_of::<T>());

        let slice: &mut [T] = unsafe {slice::from_raw_parts_mut(alloc as *mut T, detached.len())};
        for (src, dst) in detached.into_iter().zip(&mut *slice) {
            Pin::new_unchecked(dst).init_from_detached(src.borrow());
        }
        Pin::new_unchecked(slice)
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_object_slice(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_object_slice_mut(index) }
    }
}

#[derive(Clone, Copy)]
enum MultiInstanceThreadBlocker {
    Idle,
    InstanceActive,
    Disabled,
}

thread_local! {
    pub(crate) static MULTI_INSTANCE_BLOCKER: Cell<MultiInstanceThreadBlocker> = const { Cell::new(MultiInstanceThreadBlocker::Idle) };
}

pub unsafe fn disable_multi_instance_blocker() {
    MULTI_INSTANCE_BLOCKER.set(MultiInstanceThreadBlocker::Disabled);
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

struct ContextGuard;

impl ContextGuard {
    fn new(context: &DatabaseContextData) -> ContextGuard {
        if !CONTEXT.get().is_null() {
            panic!(
                "'with_root' must not be called within an existing database access context.
                         For example, it cannot be called within a 'with_root', 'with_root_mut' or
                         message apply operation.
                "
            );
        }
        CONTEXT.set(context as *const _ as *mut _);
        ContextGuard
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CONTEXT.set(null_mut());
    }
}
struct ContextGuardMut;

impl ContextGuardMut {
    fn new(context: &mut DatabaseContextData) -> ContextGuardMut {
        if !CONTEXT.get().is_null() {
            panic!(
                "'with_root' must not be called within an existing database access context.
                         For example, it cannot be called within a 'with_root', 'with_root_mut' or
                         message apply operation.
                "
            );
        }
        context.is_mutable = true;
        CONTEXT.set(context as *mut _);
        ContextGuardMut
    }
}

impl Drop for ContextGuardMut {
    fn drop(&mut self) {
        unsafe {
            (*CONTEXT.get()).is_mutable = false;
        }
        CONTEXT.set(null_mut());

    }
}

pub use paste::paste;

noatun_object!(
        struct Kalle {
            pod hej:u32,
            pod tva:u32,
            object da: crate::data_types::DatabaseVec<crate::data_types::DatabaseCell<u32>>
        }
);
noatun_object!(
    struct Nalle {
        pod hej:u32,
        pod tva:u32,
        object da: crate::data_types::DatabaseVec<crate::data_types::DatabaseCell<u32>>
    }
);

fn msg_serialize<T: savefile::Serialize + savefile::Packed>(
    obj: &T,
    mut writer: impl Write,
) -> anyhow::Result<()> {
    Ok(savefile::Serializer::bare_serialize(&mut writer, 0, obj)?)
}
fn msg_deserialize<T: savefile::Deserialize + savefile::Packed>(buf: &[u8]) -> anyhow::Result<T> {
    Ok(Deserializer::bare_deserialize(
        &mut std::io::Cursor::new(buf),
        0,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::{DatabaseCellArrayExt, NoatunString};
    use crate::disk_access::FileAccessor;
    use crate::distributor::DistributorMessage;
    use crate::projection_store::{MainDbAuxHeader, MainDbHeader};
    use crate::sequence_nr::SequenceNr;
    use byteorder::{LittleEndian, WriteBytesExt};
    use chrono::{NaiveDate, Utc};
    use data_types::DatabaseCell;
    use data_types::DatabaseObjectHandle;
    use data_types::DatabaseVec;
    use database::Database;
    use datetime_literal::datetime;
    use savefile::{load_noschema, save_noschema};
    use savefile_derive::Savefile;
    use sha2::{Digest, Sha256};
    use std::io::{Cursor, SeekFrom};
    use std::iter::once;
    use tokio::io::AsyncSeekExt;

    mod distributor_tests;
    mod tests_using_noatun_object_macro;

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

        mmap.seek(SeekFrom::Start(3_000_000)).unwrap();
        mmap.write_u8(1).unwrap();
        assert_eq!(initial_ptr, mmap.map_mut_ptr());

        mmap.seek(SeekFrom::Start(3_000_000)).unwrap();
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

    pub struct DummyMessage<T> {
        phantom_data: PhantomData<T>,
    }
    impl<T> Debug for DummyMessage<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "DummyMessage")
        }
    }

    impl<T: Object> MessagePayload for DummyMessage<T> {
        type Root = T;

        fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
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

    #[derive(Clone, Copy, AnyBitPattern)]
    #[repr(C)]
    struct CounterObject {
        // TODO: This isn't Unpin, but it should be!
        // Though, since it's just for testing, it's not critical
        counter: DatabaseCell<u32>,
        counter2: DatabaseCell<u32>,
    }

    impl Object for CounterObject {
        type Ptr = ThinPtr;
        type DetachedType = ();
        type DetachedOwnedType = ();

        fn detach(&self) -> Self::DetachedOwnedType {
            todo!()
        }

        fn init_from_detached(self:Pin<&mut Self>, detached: &Self::DetachedType) {
            todo!()
        }

        unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
            todo!()
        }

        unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
            unsafe { NoatunContext.access_pod(index) }
        }

        unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
            unsafe { NoatunContext.access_object_mut(index) }
        }
    }

    impl CounterObject {
        fn set_counter(mut self: Pin<&mut Self>, value1: u32, value2: u32) {
            unsafe {
                self.as_mut().map_unchecked_mut(|x|&mut x.counter).set(value1);
                self.as_mut().map_unchecked_mut(|x|&mut x.counter2).set(value2);
            }
        }
    }

    impl Application for CounterObject {
        type Message = CounterMessage;
        type Params = ();

        fn initialize_root<'a>(params: &Self::Params) -> Pin<&'a mut Self> {
            let new_obj = NoatunContext.allocate_pod();
            new_obj
        }
    }

    #[derive(Debug)]
    struct IncrementMessage {
        increment_by: u32,
    }

    impl MessagePayload for IncrementMessage {
        type Root = CounterObject;

        fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
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
        let mut db: Database<CounterObject> = Database::create_new(
            "test/test1.bin",
            true,
            1000,
            Duration::from_secs(1000),
            None,
            (),
        )
        .unwrap();

        db.with_root_mut(|mut counter| {
            unsafe {
                let counter = unsafe { counter.get_unchecked_mut() };
                assert_eq!(counter.counter.get(), 0);
                Pin::new_unchecked(&mut counter.counter). set(42);
                Pin::new_unchecked(&mut counter.counter2).set(43);
                Pin::new_unchecked(&mut counter.counter). set(44);

                assert_eq!(counter.counter.get(), 44);
                assert_eq!(counter.counter.get(), 44);
                assert_eq!(counter.counter2.get(), 43);
            }
        });
    }

    #[derive(Debug, Clone, Savefile)]
    struct CounterMessage {
        id: MessageId,
        parent: Vec<MessageId>,
        inc1: i32,
        set1: u32,
    }
    impl CounterMessage {
        fn wrap(&self) -> Message<CounterMessage> {
            Message::new(self.id, self.parent.clone(), self.clone())
        }
    }
    impl MessagePayload for CounterMessage {
        type Root = CounterObject;

        fn apply(&self, time: NoatunTime, mut root: Pin<&mut CounterObject>) {
            unsafe {
                if self.inc1 != 0 {
                    let val = root.counter.get().saturating_add_signed(self.inc1);
                    root.map_unchecked_mut(|x|&mut x.counter).set(val);
                } else {
                    root.map_unchecked_mut(|x|&mut x.counter).set(self.set1);
                }
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
    fn test_projection_time_limit() {
        let mut db: Database<CounterObject> = Database::create_new(
            "test/msg_store_time_limit.bin",
            true,
            10000,
            Duration::from_secs(1000),
            Some(datetime!(2024-01-02 00:00:00 Z)),
            (),
        )
        .unwrap();

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::from_parts(datetime!(2024-01-01 00:00:00 Z), [0; 10]).unwrap(),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        db.mark_transmitted(MessageId::new_debug(0x100));

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::from_parts(datetime!(2024-01-02 00:00:00 Z), [0; 10]).unwrap(),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::from_parts(datetime!(2024-01-03 00:00:00 Z), [0; 10]).unwrap(),
                inc1: 1, //This is never projected, because of time limit
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        db.with_root_mut(|root| {
            // Time limit means last message isn't projected
            assert_eq!(root.counter.get(), 2);
        });

        db.with_root_preview(
            datetime!(2024-01-03 00:00:00 Z),
            [CounterMessage {
                parent: vec![],
                id: MessageId::from_parts(datetime!(2024-01-03 00:00:00 Z), [0; 10]).unwrap(),
                inc1: 2,
                set1: 0,
            }]
            .into_iter(),
            |root| {
                assert_eq!(root.counter.get(), 4);
            },
        )
        .unwrap();

        db.with_root_mut(|root| {
            // Time limit means last message isn't projected
            assert_eq!(root.counter.get(), 2);
        });
    }
    #[test]
    fn test_msg_store_real() {
        let mut db: Database<CounterObject> = Database::create_new(
            "test/msg_store.bin",
            true,
            10000,
            Duration::from_secs(1000),
            None,
            (),
        )
        .unwrap();

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::new_debug(0x100),
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        db.mark_transmitted(MessageId::new_debug(0x100));

        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: MessageId::new_debug(0x101),
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: MessageId::new_debug(0x102),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        println!("Update heads: {:?}", db.get_update_heads());
        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root| {
            assert_eq!(root.counter.get(), 43);
        });
    }

    #[test]
    fn test_msg_store_inmem_miri() {
        let mut db: Database<CounterObject> = Database::create_in_memory(
            10000,
            Duration::from_secs(1000),
            Some(datetime!(2021-01-01 Z)),
            None,
            (),
        )
        .unwrap();

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::new_debug(0x100),
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: MessageId::new_debug(0x101),
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: MessageId::new_debug(0x102),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);
        assert!(!db.contains_message(MessageId::new_debug(0x100)).unwrap());
        assert!(db.contains_message(MessageId::new_debug(0x101)).unwrap());
        assert!(db.contains_message(MessageId::new_debug(0x102)).unwrap());

        db.with_root_mut(|root| {
            assert_eq!(root.counter.get(), 43);
        });
    }

    #[test]
    fn test_msg_store_after_cutoff_inmem_miri() {
        let mut db: Database<CounterObject> = Database::create_in_memory(
            10000,
            Duration::from_secs(1000),
            Some(datetime!(2024-01-01 Z)),
            None,
            (),
        )
        .unwrap();

        let m1 = MessageId::from_parts(datetime!(2024-01-01 Z), [0u8; 10]).unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![],
                id: m1,
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.mark_transmitted(m1).unwrap();
        let m2 = MessageId::from_parts(datetime!(2024-01-01 Z), [1u8; 10]).unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: m2,
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.set_mock_time(datetime!(2024-01-10 Z));
        db.reproject().unwrap();
        println!("Appending 2nd");
        let m3 = MessageId::from_parts(datetime!(2024-01-10 Z), [2u8; 10]).unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: m3,
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        assert!(!db.contains_message(m1).unwrap());
        assert!(db.contains_message(m2).unwrap());
        assert!(db.contains_message(m3).unwrap());

        db.with_root_mut(|root| {
            assert_eq!(root.counter.get(), 43);
        });
    }

    #[test]
    fn test_cutoff_handling() {
        let mut db: Database<CounterObject> =
            Database::create_in_memory(10000, Duration::from_secs(1000), None, None, ()).unwrap();

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::new_debug(0x100),
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: MessageId::new_debug(0x101),
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: MessageId::new_debug(0x102),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        let mut d = distributor::Distributor::new();

        println!("Heads: {:?}", d.get_periodic_message(&db));

        let r = d
            .receive_message(
                &mut db,
                std::iter::once(DistributorMessage::RequestUpstream {
                    query: vec![(MessageId::new_debug(0x102), 2)],
                }),
            )
            .unwrap();
        println!("Clarify: {:?}", r);

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root| {
            assert_eq!(root.counter.get(), 43);
        });
    }

    #[test]
    fn test_handle() {
        let mut db: Database<DatabaseObjectHandle<DatabaseCell<u32>>> = Database::create_new(
            "test/test_handle.bin",
            true,
            1000,
            Duration::from_secs(1000),
            None,
            (),
        )
        .unwrap();

        db.with_root(|handle| {
            assert_eq!(handle.get().get(), 43);
        });
    }

    impl Application for DatabaseObjectHandle<DatabaseCell<u32>> {
        type Message = DummyMessage<DatabaseObjectHandle<DatabaseCell<u32>>>;
        type Params = ();

        fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
            let obj = DatabaseObjectHandle::allocate(DatabaseCell::new(43u32));
            obj
        }
    }
    impl Application for DatabaseObjectHandle<[DatabaseCell<u8>]> {
        type Message = DummyMessage<DatabaseObjectHandle<[DatabaseCell<u8>]>>;
        type Params = ();

        fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
            let obj = DatabaseObjectHandle::allocate_unsized(
                [43u8, 45].map(DatabaseCell::new).as_slice(),
            );
            obj
        }
    }

    #[test]
    fn test_handle_to_unsized_miri() {
        let mut db: Database<DatabaseObjectHandle<[DatabaseCell<u8>]>> =
            Database::create_in_memory(
                1000,
                Duration::from_secs(1000),
                Some(datetime!(2021-01-01 Z)),
                None,
                (),
            )
            .unwrap();

        db.with_root(|handle| {
            assert_eq!(handle.get().observe(), &[43, 45]);
        });
    }

    #[test]
    fn test_handle_miri() {
        let mut db: Database<DatabaseObjectHandle<DatabaseCell<u32>>> = Database::create_in_memory(
            1000,
            Duration::from_secs(1000),
            Some(datetime!(2021-01-01 Z)),
            None,
            (),
        )
        .unwrap();

        db.with_root(|handle| {
            assert_eq!(handle.get().get(), 43);
        });

        db.with_root_mut(|root| {
            let a1 = root.get_mut();
            assert_eq!(a1.get().get(), 43);
        });
    }
    impl Application for DatabaseVec<CounterObject> {
        type Params = ();

        fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
            let obj: Pin<&mut DatabaseVec<CounterObject>> = DatabaseVec::new();
            obj
        }

        type Message = DummyMessage<DatabaseVec<CounterObject>>;
    }
    impl Application for NoatunString {
        type Params = ();

        fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
            unsafe { NoatunString::allocate_from_detached("hello".into()) }
        }

        type Message = DummyMessage<NoatunString>;
    }

    #[test]
    fn test_string0() {
        let mut db: Database<NoatunString> = Database::create_new(
            "test/test_string0",
            true,
            10000,
            Duration::from_secs(1000),
            None,
            (),
        )
            .unwrap();
        db.with_root_mut(|mut test_str| {
            unsafe {
                assert_eq!(test_str.len(), 5);
                assert_eq!(test_str.get(), "hello");
                let ptr = test_str.get().as_ptr();
                test_str.as_mut().assign("hell");
                assert_eq!(ptr, test_str.get().as_ptr());
                assert_eq!(test_str.get(), "hell");
                test_str.as_mut().assign("hello world!");
                assert_eq!(test_str.get(), "hello world!");

            }
        });
    }

                #[test]
    fn test_vec0() {
        let mut db: Database<DatabaseVec<CounterObject>> = Database::create_new(
            "test/test_vec0",
            true,
            10000,
            Duration::from_secs(1000),
            None,
            (),
        )
        .unwrap();
        db.with_root_mut(|mut counter_vec| {
            unsafe {
                assert_eq!(counter_vec.len(), 0);

                let new_element = counter_vec.as_mut().push_zeroed();
                let mut new_element = counter_vec.as_mut().getmut(0);

                new_element.map_unchecked_mut(|x| &mut x.counter).set(47);
                let mut new_element = counter_vec.as_mut().push_zeroed();
                new_element.map_unchecked_mut(|x|&mut x.counter).set(48);

                assert_eq!(counter_vec.len(), 2);

                let item = counter_vec.as_mut().getmut(1);
                //let item2 = counter_vec.get_mut(context, 1);
                assert_eq!(item.counter.get(), 48);
                //assert_eq!(*item2.counter, 48);

                for _ in 0..10 {
                    let new_element = counter_vec.as_mut().push_zeroed();
                }

                let item = counter_vec.as_mut().getmut(1);
                assert_eq!(item.counter.get(), 48);
            }
        });
    }

    #[test]
    fn test_vec_miri0() {
        let mut db: Database<DatabaseVec<CounterObject>> = Database::create_in_memory(
            10000,
            Duration::from_secs(1000),
            Some(datetime!(2021-01-01 Z)),
            None,
            (),
        )
        .unwrap();
        db.with_root_mut(|mut counter_vec| {
            assert_eq!(counter_vec.len(), 0);

            let new_element = counter_vec.as_mut().push_zeroed();

            let mut new_element = counter_vec.as_mut().getmut(0);

            unsafe {
                new_element.map_unchecked_mut(|x|&mut x.counter).set(47);
            }
            let mut new_element = counter_vec.as_mut().push_zeroed();
            unsafe {
                new_element.map_unchecked_mut(|x| &mut x.counter).set(48);
            }

            assert_eq!(counter_vec.len(), 2);

            let item = counter_vec.as_mut().getmut(1);
            //let item2 = counter_vec.get_mut(context, 1);
            assert_eq!(item.counter.get(), 48);
            //assert_eq!(*item2.counter, 48);

            for i in 0..10 {
                let new_element = counter_vec.as_mut().push_zeroed();
            }

            let item = counter_vec.as_mut().getmut(1);
            assert_eq!(item.counter.get(), 48);
            assert_eq!(counter_vec.len(), 12);

            counter_vec.as_mut().shift_remove(1);
            assert_eq!(counter_vec.len(), 11);
            assert_eq!(counter_vec.as_mut().get(0).counter.get(), 47);

            counter_vec.as_mut().retain(|x| x.counter.get() == 0);
            assert_eq!(counter_vec.len(), 10);

            for i in 0..10 {
                assert_eq!(counter_vec.get(i).counter.get() as usize, 0);
            }
        });
    }
    #[test]
    fn test_vec_undo() {
        let mut db: Database<DatabaseVec<CounterObject>> = Database::create_new(
            "test/vec_undo",
            true,
            10000,
            Duration::from_secs(1000),
            None,
            (),
        )
        .unwrap();

        {
            db.with_root_mut(|mut counter_vec| {
                NoatunContext.set_next_seqnr(SequenceNr::from_index(1));
                assert_eq!(counter_vec.len(), 0);

                let mut new_element = counter_vec.as_mut().push_zeroed();
                unsafe {
                    new_element.as_mut().map_unchecked_mut(|x|&mut x.counter).set(47);
                    new_element.as_mut().map_unchecked_mut(|x|&mut x.counter2).set(48);
                }

                NoatunContext.set_next_seqnr(SequenceNr::from_index(2));
                assert_eq!(counter_vec.len(), 1);
                NoatunContext.set_next_seqnr(SequenceNr::from_index(3));
            });
        }

        {
            db.with_root_mut(|counter_vec| {
                let mut counter = counter_vec.getmut(0);

                unsafe {
                    counter.as_mut().map_unchecked_mut(|x|&mut x.counter).set(50);
                    NoatunContext.rewind(SequenceNr::from_index(2));
                }
                assert_eq!(counter.counter.get(), 47);
            });
        }

        db.force_rewind(SequenceNr::from_index(1));

        {
            db.with_root_mut(|counter_vec| {
                assert_eq!(counter_vec.len(), 0);
            });
        }
    }

    #[test]
    fn test_object_macro() {
        use crate::data_types::DatabaseVec;
        noatun_object!(
            struct Kalle {
                pod hej:u32,
                pod tva:u32,
                object da: DatabaseVec<DatabaseCell<u32>>
            }
        );
        noatun_object!(
            struct Nalle {
                pod hej:u32,
                pod tva:u32,
                object da: DatabaseVec<DatabaseCell<u32>>
            }
        );
    }
}
