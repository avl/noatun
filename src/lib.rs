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

pub use crate::data_types::{DatabaseCell, DatabaseVec};
use crate::sequence_nr::SequenceNr;
use anyhow::{bail, Result};
pub use bytemuck::{AnyBitPattern, Pod, Zeroable};
use chrono::{DateTime, SecondsFormat, Utc};
pub use cutoff::{CutOffDuration, CutOffState};
pub use database::Database;
pub(crate) use projection_store::DatabaseContextData;
use rand::RngCore;
use savefile::Deserializer;
pub use savefile_derive::Savefile;
pub use serde_derive;
use std::borrow::Borrow;
use std::cell::Cell;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::mem::MaybeUninit;
use std::ops::{Add, Sub};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::null_mut;
use std::slice;
use std::time::Duration;
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
mod xxh3_vendored;


thread_local! {
    pub static CONTEXT: Cell<*mut DatabaseContextData> = const { Cell::new(null_mut()) };
}

#[derive(Clone, Copy)]
pub struct NoatunContext;

fn get_context_mut_ptr() -> *mut DatabaseContextData {
    let context_ptr = CONTEXT.get();
    if context_ptr.is_null() {
        //TODO: Unify this error message with 'ensure_mutable'
        panic!("No mutable NoatunContext is presently available on this thread");
    }
    unsafe {
        (*context_ptr).assert_mutable()
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
    pub fn update_registrar_ptr(self, seq: *mut SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).update_registrar_ptr(seq) }
    }
    pub fn clear_registrar_ptr(self, seq: *mut SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).clear_registrar_ptr(seq) }
    }
    pub fn start_ptr_mut(self) -> *mut u8 {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).start_ptr_mut() }
    }
    pub fn start_ptr(self) -> *const u8 {
        let context_ptr = get_context_ptr();
        unsafe { (*context_ptr).start_ptr() }
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
    pub fn zero(&self, dst: FatPtr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).zero(dst) }
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
    pub fn update_registrar(&self, registrar: &mut SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe {
            (*context_ptr).update_registrar(registrar);
        }
    }
    pub fn write_pod<T: Pod>(&self, value: T, dest: Pin<&mut T>) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_pod(value, dest) }
    }
    pub fn write_any<T: AnyBitPattern>(&self, value: T, dest: Pin<&mut T>) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_any(value, dest) }
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
    // TODO: This shouldn't really be 'safe'. The returned lifetime is unbounded, but it
    // should be bounded to the current database. We can't express that lifetime.
    pub fn allocate_pod<'a, T: AnyBitPattern>(&self) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).allocate_pod() }
    }
    pub unsafe fn access_pod_mut<'a, T: Pod>(&mut self, ptr: ThinPtr) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).access_pod_mut(ptr) }
    }
    pub unsafe fn access_object_mut<'a, T: FixedSizeObject>(
        &mut self,
        ptr: ThinPtr,
    ) -> Pin<&'a mut T> {
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
        if unsafe { (*(context_ptr as *const DatabaseContextData)).is_mutable == false } {
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
        unsafe { Pin::new_unchecked((*context_ptr).access_slice_mut(range)) }
    }
    pub unsafe fn access_object_slice<'a, T: FixedSizeObject>(self, range: FatPtr) -> &'a [T] {
        let p = CONTEXT.get();
        if p.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*p).access_object_slice(range) }
    }
    pub unsafe fn access_object_slice_mut<'a, T: FixedSizeObject>(
        self,
        range: FatPtr,
    ) -> Pin<&'a mut [T]> {
        let context_ptr = get_context_mut_ptr();
        unsafe { Pin::new_unchecked((*context_ptr).access_object_slice_mut(range)) }
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

        let time = chrono::DateTime::from_timestamp_millis(time_ms.as_ms() as i64).unwrap();

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

const FOR_TEST_NON_RANDOM_ID: bool = false;
#[cfg(test)]
static NON_RANDOM_ID_COUNTER: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl MessageId {
    pub const ZERO: MessageId = MessageId { data: [0u32; 4] };
    pub fn min(self, other: MessageId) -> MessageId {
        if self < other {
            self
        } else {
            other
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        bytemuck::cast_slice(&self.data)
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

    pub fn generate_for_time(time: NoatunTime) -> Result<MessageId> {
        let mut random_part = [0u8; 10];

        #[cfg(test)]
        {
            if FOR_TEST_NON_RANDOM_ID {
                random_part[0..8].copy_from_slice(
                    &NON_RANDOM_ID_COUNTER
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        .to_le_bytes(),
                );
            } else {
                rand::thread_rng().fill_bytes(&mut random_part);
            }
        }
        #[cfg(not(test))]
        {
            rand::thread_rng().fill_bytes(&mut random_part);
        }

        Self::from_parts(time, random_part)
    }
    pub fn from_parts_for_test(time: NoatunTime, random: u64) -> MessageId {
        let mut data = [0u8; 10];
        data[2..10].copy_from_slice(&random.to_le_bytes());

        Self::from_parts(time, data).unwrap()
    }
    pub fn timestamp(&self) -> NoatunTime {
        let restes = (self.data[0] as u64) + (((self.data[1] & 0xffff) as u64) << 32);
        NoatunTime(restes)
    }
    pub fn from_parts(time: NoatunTime, random: [u8; 10]) -> Result<MessageId> {
        let t: u64 = time.as_ms();
        Self::from_parts_raw(t, random)
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

#[derive(
    Clone,
    Copy,
    Pod,
    Zeroable,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde_derive::Serialize,
    serde_derive::Deserialize,
    Savefile,
)]
#[repr(C)]
pub struct NoatunTime(pub u64);

impl Add<Duration> for NoatunTime {
    type Output = NoatunTime;
    fn add(self, rhs: Duration) -> Self::Output {
        NoatunTime(self.0 + rhs.as_millis() as u64)
    }
}
impl Sub<Duration> for NoatunTime {
    type Output = NoatunTime;
    fn sub(self, rhs: Duration) -> Self::Output {
        NoatunTime(self.0.saturating_sub(rhs.as_millis() as u64))
    }
}

impl Add<NoatunTime> for Duration {
    type Output = NoatunTime;
    fn add(self, rhs: NoatunTime) -> Self::Output {
        NoatunTime(rhs.0 + self.as_millis() as u64)
    }
}

impl From<DateTime<Utc>> for NoatunTime {
    fn from(value: DateTime<Utc>) -> Self {
        let ms = value.timestamp_millis();
        if ms < 0 {
            NoatunTime(0)
        } else {
            NoatunTime(ms as u64)
        }
    }
}
impl From<NoatunTime> for DateTime<Utc> {
    fn from(value: NoatunTime) -> Self {
        value.to_datetime()
    }
}

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

impl Add for NoatunTime {
    type Output = NoatunTime;

    fn add(self, rhs: Self) -> Self::Output {
        NoatunTime(self.0 + rhs.0)
    }
}

impl Sub for NoatunTime {
    type Output = NoatunTime;

    fn sub(self, rhs: Self) -> Self::Output {
        NoatunTime(self.0 - rhs.0)
    }
}

impl NoatunTime {
    pub fn elapsed_ms_since(self, other: NoatunTime) -> u64 {
        let ms = self.0.saturating_sub(other.0);
        ms
    }

    pub fn next_multiple_of(self, other: NoatunTime) -> NoatunTime {
        //TODO: Checked arithmetic
        NoatunTime(self.0.next_multiple_of(other.0))
    }
    pub fn prev_multiple_of(self, other: NoatunTime) -> NoatunTime {
        //TODO: Checked arithmetic
        NoatunTime(self.0.next_multiple_of(other.0) - other.0)
    }
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
    pub const fn from_datetime(t: DateTime<Utc>) -> NoatunTime {
        NoatunTime(t.timestamp_millis() as u64)
    }
}

/// Controls how long this message will persist
pub enum Persistence {
    /// The message will be automatically removed once:
    /// 1) All data it has written has been overwritten by later messages
    /// 2) No later, existing, message depends on information written by this message
    ///
    /// This is the default.
    ///
    /// This default is useful for messages that don't need to be kept for records-keeping,
    /// like updates of sensor values, or other periodic updates.
    UntilOverwritten,
    /// Like UntilOverwritten, except the message will unconditionally be kept around until
    /// the cutoff-period has elapsed. This can be useful to make sure that a complete log of 'recent'
    /// messages exists at all times, to ease troubleshooting or building functionality to show
    /// 'recent events' to users.
    AtLeastUntilCutoff,
    // This message will not be deleted.
    // TODO: Implement?
    //Forever
}

pub trait MessagePayload: Debug {
    type Root: Object;
    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>);

    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;
    fn serialize<W: Write>(&self, writer: W) -> Result<()>;

    fn persistence(&self) -> Persistence {
        Persistence::UntilOverwritten
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageHeader {
    pub id: MessageId,
    pub parents: Vec<MessageId>,
}

#[derive(Debug)]
pub struct Message<M: MessagePayload> {
    pub header: MessageHeader,
    pub payload: M,
}

impl<M: MessagePayload> PartialEq for Message<M>
where
    M: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.payload == other.payload
    }
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

    fn detach(&self) -> Self::DetachedType {}

    fn clear(self: Pin<&mut Self>) {

    }

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {}

    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        unsafe { Pin::new_unchecked(&mut *(1usize as *mut DummyUnitObject)) }
    }

    unsafe fn access<'a>(_index: Self::Ptr) -> &'a Self {
        &DummyUnitObject
    }

    unsafe fn access_mut<'a>(_index: Self::Ptr) -> Pin<&'a mut Self> {
        // # SAFETY
        // Any dangling pointer is a valid pointer to a zero-sized type
        unsafe { Pin::new_unchecked(&mut *(1usize as *mut DummyUnitObject)) }
    }
}

pub enum GenPtr {
    Thin(ThinPtr),
    Fat(FatPtr),
}

#[derive(Clone, Copy, Zeroable, Pod, Debug)]
#[repr(C)]
struct SerializableGenPtr {
    ptr: usize,
    len: usize, //usize::MAX for thin
}
impl From<SerializableGenPtr> for GenPtr {
    fn from(value: SerializableGenPtr) -> Self {
        if value.len == usize::MAX {
            GenPtr::Thin(ThinPtr(value.ptr))
        } else {
            GenPtr::Fat(FatPtr{
                start: value.ptr,
                len: value.len,
            })
        }
    }
}
impl From<GenPtr> for SerializableGenPtr {
    fn from(ptr: GenPtr) -> Self {
        match ptr {
            GenPtr::Thin(t) => {
                Self {
                    ptr: t.0,
                    len: usize::MAX,
                }
            }
            GenPtr::Fat(f) => {
                Self {
                    ptr: f.start,
                    len: f.len,
                }
            }
        }
    }
}


pub trait Pointer: Copy + Debug + 'static {
    fn start(self) -> usize;
    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self;
    fn as_generic(&self) -> GenPtr;
    fn is_null(&self) -> bool;
}

pub fn from_bytes_mut<T: FixedSizeObject>(s: &mut [u8]) -> &mut T {
    assert_eq!(s.len(), size_of::<T>());
    assert!((s.as_mut_ptr() as *mut T).is_aligned());

    // # Safety
    // We've checked alignment and size, and those are the only requirements
    // an Object need to be valid.
    unsafe {
        &mut *s.as_mut_ptr().cast::<T>()
        //transmute::<*mut u8, &mut T>(s.as_mut_ptr())
    }
}
pub fn bytes_of<T: FixedSizeObject>(t: &T) -> &[u8] {
    // # Safety
    // FixedSizeObject instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe { slice::from_raw_parts(t as *const _ as *const u8, size_of::<T>()) }
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
    type DetachedOwnedType: Borrow<Self::DetachedType>;

    fn detach(&self) -> Self::DetachedOwnedType;

    fn clear(self: Pin<&mut Self>);

    /// Initialize all the fields in 'self' from the given 'detached' type.
    /// The detached type is a regular rust pod struct, with no requirements
    /// on alignment, pinning or similar. It can therefore be passed around freely,
    /// being more convenient to use for initialization.
    ///
    /// Note that you don't _have_ to use this method, it's perfectly fine
    /// to initialize all Object's "in place", after constructing/allocating default
    /// versions of them.
    ///
    /// All noatun objects are valid when initialized with zero bits.
    /// This means that the value is always valid before init runs.
    /// Init is thus technically an overwrite of an existing value. If that
    /// value had a registrar, it is overwritten.
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
                unsafe { ::std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().$name).set(val); }
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
        unsafe { <_ as $crate::Object>::init_from_detached(::std::pin::Pin::new_unchecked(&mut $self.$name), $name); }
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

    ( declare_detached_struct $n_detached:ident fields { $($derive_item:ident),* }  $( $kind:ident $name: ident $typ:ty ),* ) => {
        #[derive($($derive_item,)* Debug,Clone,$crate::serde_derive::Serialize, $crate::serde_derive::Deserialize, $crate::Savefile)]
        pub struct $n_detached
        {
            $( $name : noatun_object!(declare_detached_field $kind $typ) ),*
        }
    };

    ( detached_type $n_detached: ident) => {
        $n_detached
    };

    (    $(#[derive( $($derive_item:ident),*  )])?
         struct $n:ident { $( $kind:ident $name: ident : $typ:ty $(,)* )* } $(;)* ) => {


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
                    pub fn pin_project<'a>(self: ::std::pin::Pin<&'a mut Self>) -> [<$n PinProject>]<'a> {
                        unsafe {
                            let $n {
                                $($name),* , ..
                            } = self.get_unchecked_mut();

                            [<$n PinProject>] {
                                $($name: ::std::pin::Pin::new_unchecked($name)),*

                            }
                        }
                    }
                }

            }

            $crate::paste! {
                noatun_object!{declare_detached_struct [<$n Detached>] fields { $($($derive_item),*)? } $($kind $name $typ),*  }
            }


            impl $crate::Object for $n {
                type Ptr = $crate::ThinPtr;
                type DetachedType = $crate::paste!(noatun_object!(detached_type [<$n Detached>]));
                type DetachedOwnedType = $crate::paste!(noatun_object!(detached_type [<$n Detached>]));


                fn clear(self: ::std::pin::Pin<&mut Self>) {
                    let tself = unsafe { self.get_unchecked_mut() };
                    $( unsafe { ::std::pin::Pin::new_unchecked(&mut tself.$name).clear(); } )*
                }

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
                        ::std::pin::Pin::new_unchecked(&mut self.as_mut().get_unchecked_mut().$name).init_from_detached(&detached.$name);

                    }
                    )*
                }

                unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> ::std::pin::Pin<&'a mut Self> {
                    let mut ret: ::std::pin::Pin<&mut Self> = $crate::NoatunContext.allocate_pod();
                    ret.as_mut().init_from_detached(detached);
                    ret
                }

                unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
                    unsafe { $crate::NoatunContext.access_object(index) }
                }

                unsafe fn access_mut<'a>(index: Self::Ptr) -> ::std::pin::Pin<&'a mut Self> {
                    unsafe { $crate::NoatunContext.access_object_mut(index) }
                }
            }


    };

}

pub(crate) fn bytes_of_uninit<T:AnyBitPattern>(t: &T) -> &[MaybeUninit<u8>] {
    let ptr = t as *const _ as *const MaybeUninit<u8>;
    // SAFETY:
    // Pointer is known to point to valid object. There may be padding bytes
    // that are unknown, but that's okay sinze we create a [MaybeUninit<u8>]
    unsafe { slice::from_raw_parts(ptr, size_of::<T>()) }
}

pub(crate) fn uninit_slice(slice: &[u8]) -> &[MaybeUninit<u8>] {
    // SAFETY:
    // MaybeUninit[u8] has exact same layout as u8
    unsafe {
        std::mem::transmute::<&[u8], &[MaybeUninit<u8>]>(slice)
    }
}

pub trait FixedSizeObject: Object<Ptr = ThinPtr> + Sized + AnyBitPattern where <Self as Object>::DetachedOwnedType : Sized {

}

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

impl<T: FixedSizeObject> Object for [T]
where
    T::DetachedType: Sized,
{
    type Ptr = FatPtr;
    type DetachedType = [T::DetachedOwnedType];
    type DetachedOwnedType = Vec<T::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter().map(|x| x.detach()).collect()
    }

    fn clear(self: Pin<&mut Self>) {
        let tself = unsafe {self.get_unchecked_mut()};
        for item in tself {
            unsafe { Pin::new_unchecked(item).clear() };
        }
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        unsafe {
            for (dst, src) in self.get_unchecked_mut().iter_mut().zip(detached.iter()) {
                Pin::new_unchecked(dst).init_from_detached(src.borrow());
            }
        }
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let bytes = size_of::<T>() * detached.len();
        let alloc = NoatunContext.allocate_raw(bytes, align_of::<T>());

        let slice: &mut [T] = unsafe { slice::from_raw_parts_mut(alloc as *mut T, detached.len()) };
        for (src, dst) in detached.iter().zip(&mut *slice) {
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
            object da: DatabaseCell<u32>
        }
);
noatun_object!(
    struct Nalle {
        pod hej:u32,
        pod tva:u32,
        object da: DatabaseCell<u32>
    }
);

// TODO: Make sure we haven't hardcoded the use of Savefile
pub fn msg_serialize<T: savefile::Serialize + savefile::Packed>(
    obj: &T,
    mut writer: impl Write,
) -> anyhow::Result<()> {
    Ok(savefile::Serializer::bare_serialize(&mut writer, 0, obj)?)
}

// TODO: Make sure we haven't hardcoded the use of Savefile
pub fn msg_deserialize<T: savefile::Deserialize + savefile::Packed>(
    buf: &[u8],
) -> anyhow::Result<T> {
    Ok(Deserializer::bare_deserialize(
        &mut std::io::Cursor::new(buf),
        0,
    )?)
}

#[cfg(test)]
mod tests;

