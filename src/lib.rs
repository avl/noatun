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
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::expect_fun_call)]
#![allow(clippy::needless_late_init)]

pub use crate::data_types::NoatunCell;
use crate::sequence_nr::SequenceNr;
use anyhow::{bail, Result};
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
use std::mem::{transmute_copy, MaybeUninit};
use std::ops::{Add, Sub};
use std::panic::{catch_unwind, AssertUnwindSafe};
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

mod private {
    pub trait Sealed {}
}

struct MessageComponent<const ID: u32, T> {
    value: Option<T>,
}

pub(crate) mod platform_specific;

mod boot_checksum;
pub(crate) mod disk_access;
mod sha2_helper;
mod xxh3_vendored;

/// SAFETY requirements:
/// * Type must not have Drop impl
/// * Type must have repr(C) or repr(transparent)
/// * Type must be a struct
/// * All bit patterns must be valid
/// * All fields must be NoatunStorable
/// * Type must not contain pointers or references
/// * Type must be 'static
/// * Type may not change between versions
///
///
/// Note that type is allowed to have padding.
/// Type is also allowed to contain indices inside the database file (ThinPtr/FatPtr).
pub unsafe trait NoatunStorable: Sized + 'static {
    fn zeroed<T: NoatunStorable>() -> T {
        unsafe { MaybeUninit::<T>::zeroed().assume_init() }
    }
    fn copy_from(&mut self, source: &Self) {
        unsafe {
            let temp = (source as *const Self).read();
            (self as *mut Self).write(temp);
        }
    }
    fn initialize(dest: &mut MaybeUninit<Self>, source: &Self) {
        unsafe {
            let temp = (source as *const Self).read();
            (dest as *mut MaybeUninit<Self> as *mut Self).write(temp);
        }
    }
}

mod noatun_storable_impls {
    use super::NoatunStorable;
    macro_rules! make_noatun_storable {
        ($t: ident) => {
            unsafe impl NoatunStorable for $t {}
        };
    }

    make_noatun_storable!(u8);
    make_noatun_storable!(u16);
    make_noatun_storable!(u32);
    make_noatun_storable!(u64);
    make_noatun_storable!(u128);
    make_noatun_storable!(i8);
    make_noatun_storable!(i16);
    make_noatun_storable!(i32);
    make_noatun_storable!(i64);
    make_noatun_storable!(i128);
    make_noatun_storable!(isize);
    make_noatun_storable!(usize);

    unsafe impl<T: NoatunStorable, const N: usize> NoatunStorable for [T; N] {}
}

thread_local! {
    pub static CONTEXT: Cell<*mut DatabaseContextData> = const { Cell::new(null_mut()) };
}

#[derive(Clone, Copy)]
pub struct NoatunContext;

#[inline]
fn get_context_mut_ptr() -> *mut DatabaseContextData {
    let context_ptr = CONTEXT.get();
    if context_ptr.is_null() {
        panic!(
            "No NoatunContext is presently available on this thread. The noatun-database \
               may only be accessed from within Message apply, and from `with_root`-methods."
        );
    }
    unsafe { (*context_ptr).assert_mutable() }
    context_ptr
}

#[inline]
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

    // Just used by single test. Consider removing?
    pub(crate) unsafe fn rewind(self, new_time: SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).rewind(new_time) }
    }

    pub fn set_next_seqnr(self, seqnr: SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).set_next_seqnr(seqnr) }
    }
    pub fn copy_ptr(&self, src: FatPtr, dest_index: ThinPtr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).copy_bytes(src, dest_index) }
    }
    pub fn zero(&self, dst: FatPtr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).zero(dst) }
    }
    pub fn copy_sized(&self, src: ThinPtr, dest_index: ThinPtr, size_bytes: usize) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).copy_bytes_len(src, dest_index, size_bytes) }
    }
    pub fn copy<T: NoatunStorable>(&self, src: &T, dst: &mut T) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).copy_storable(src, dst) }
    }
    pub fn copy_uninit<T: NoatunStorable>(&self, src: &T, dst: &mut MaybeUninit<T>) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).copy_uninit(src, dst) }
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
    pub fn write<T: NoatunStorable>(&self, value: T, dest: Pin<&mut T>) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_storable(value, dest) }
    }
    pub(crate) fn write_internal<T: NoatunStorable>(&self, value: T, dest: &mut T) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_storable(value, Pin::new_unchecked(dest)) }
    }
    pub fn write_ptr<T: NoatunStorable>(&self, value: T, dest: *mut T) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).write_storable_ptr(value, dest) }
    }
    pub unsafe fn allocate<'a, T: NoatunStorable>(&self) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).allocate_storable() }
    }

    pub unsafe fn allocate_obj<'a, T: Object>(&self) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).allocate_obj() }
    }

    pub unsafe fn access_thin<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*context_ptr).access_thin::<T>(ptr) }
    }
    pub unsafe fn access_thin_mut<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a mut T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*context_ptr).access_thin_mut::<T>(ptr) }
    }
    pub unsafe fn access_fat<'a, T: ?Sized>(&self, ptr: FatPtr) -> &'a T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*context_ptr).access_fat::<T>(ptr) }
    }
    pub unsafe fn access_fat_mut<'a, T: ?Sized>(&self, ptr: FatPtr) -> &'a mut T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        unsafe { (*context_ptr).access_fat_mut::<T>(ptr) }
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
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Savefile)]
#[repr(transparent)]
pub struct MessageId {
    data: [u32; 4],
}

unsafe impl NoatunStorable for MessageId {}

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
            write!(f, "{self}")
        }
    }
}

#[cfg(test)]
static FOR_TEST_NON_RANDOM_ID: bool = true;

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
        cast_slice(&self.data)
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
            data: cast_storable(data),
        })
    }
}

#[derive(
    Clone,
    Copy,
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

unsafe impl NoatunStorable for NoatunTime {}

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
        write!(f, "{time_str}")
    }
}
impl Debug for NoatunTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time = chrono::DateTime::from_timestamp_millis(self.0 as i64).unwrap();

        let time_str = time.to_rfc3339_opts(SecondsFormat::Millis, true);
        write!(f, "{time_str}")
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

    pub fn next_multiple_of(self, other: NoatunTime) -> Option<NoatunTime> {
        Some(NoatunTime(self.0.checked_next_multiple_of(other.0)?))
    }
    pub fn prev_multiple_of(self, other: NoatunTime) -> Option<NoatunTime> {
        Some(NoatunTime(
            self.0.checked_next_multiple_of(other.0)? - other.0,
        ))
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
        if self.0 > i64::MAX as u64 {
            panic!("Noatun time out of range for DateTime<Utc>");
        }
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
}

pub trait Message: Debug {
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
pub struct MessageFrame<M: Message> {
    pub header: MessageHeader,
    pub payload: M,
}

impl<M: Message> PartialEq for MessageFrame<M>
where
    M: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.payload == other.payload
    }
}

impl<M: Message> MessageFrame<M> {
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

pub(crate) fn catch_and_log(f: impl FnOnce()) {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(()) => {}
        Err(err) => {
            if let Some(err) = (*err).downcast_ref::<String>() {
                warn!("apply method panicked: {:?}", err);
            } else if let Some(err) = err.downcast_ref::<&'static str>() {
                warn!("apply method panicked: {:?}", err);
            } else {
                warn!("apply method panicked.");
            }
        }
    }
}

/// A state-less object, mostly useful for testing
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DummyUnitObject;

unsafe impl NoatunStorable for DummyUnitObject {}

impl Object for DummyUnitObject {
    type Ptr = ThinPtr;
    type DetachedType = ();
    type DetachedOwnedType = ();

    fn detach(&self) -> Self::DetachedType {}

    fn clear(self: Pin<&mut Self>) {}

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {}

    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        unsafe { Pin::new_unchecked(&mut *std::ptr::dangling_mut::<DummyUnitObject>()) }
    }
}

pub enum GenPtr {
    Thin(ThinPtr),
    Fat(FatPtr),
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct SerializableGenPtr {
    ptr: usize,
    count: usize, //usize::MAX for thin
}

unsafe impl NoatunStorable for SerializableGenPtr {}

impl From<SerializableGenPtr> for GenPtr {
    fn from(value: SerializableGenPtr) -> Self {
        if value.count == usize::MAX {
            GenPtr::Thin(ThinPtr(value.ptr))
        } else {
            GenPtr::Fat(FatPtr {
                start: value.ptr,
                count: value.count,
            })
        }
    }
}
impl From<GenPtr> for SerializableGenPtr {
    fn from(ptr: GenPtr) -> Self {
        match ptr {
            GenPtr::Thin(t) => Self {
                ptr: t.0,
                count: usize::MAX,
            },
            GenPtr::Fat(f) => Self {
                ptr: f.start,
                count: f.count,
            },
        }
    }
}

pub trait Pointer: NoatunStorable + Sealed + Copy + Debug + 'static {
    fn start(self) -> usize;
    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self;
    fn as_generic(&self) -> GenPtr;
    fn is_null(&self) -> bool;

    unsafe fn access<'a, T: ?Sized>(&self) -> &'a T;
    unsafe fn access_mut<'a, T: ?Sized>(&self) -> Pin<&'a mut T>;

    unsafe fn access_ctx<'a, T: ?Sized>(&self, context: &DatabaseContextData) -> &'a T;
    unsafe fn access_ctx_mut<'a, T: ?Sized>(&self, context: &mut DatabaseContextData) -> &'a mut T;
}

pub fn from_bytes<T: NoatunStorable>(s: &[u8]) -> &T {
    assert_eq!(s.len(), size_of::<T>());
    assert!((s.as_ptr() as *mut T).is_aligned());

    unsafe { &*s.as_ptr().cast::<T>() }
}
pub fn from_bytes_mut<T: NoatunStorable>(s: &mut [u8]) -> &mut T {
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

pub fn cast_storable<I: NoatunStorable, O: NoatunStorable>(i: I) -> O {
    const {
        if size_of::<I>() != size_of::<O>() {
            panic!("Source and destination size must be the same");
        }
    }
    unsafe { transmute_copy::<I, O>(&i) }
}

pub fn bytes_of<T: NoatunStorable>(t: &T) -> &[u8] {
    // # Safety
    // FixedSizeObject instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe { slice::from_raw_parts(t as *const _ as *const u8, size_of::<T>()) }
}
pub fn bytes_of_mut<T: NoatunStorable>(t: &mut T) -> &mut [u8] {
    // # Safety
    // NoatunStorable instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe { slice::from_raw_parts_mut(t as *mut _ as *mut u8, size_of::<T>()) }
}
pub fn bytes_of_mut_uninit<T: NoatunStorable>(t: &mut MaybeUninit<T>) -> &mut [u8] {
    unsafe { magic_initialize_ptr(t as *mut MaybeUninit<T>) };
    // # Safety
    // NoatunStorable instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe { slice::from_raw_parts_mut(t as *mut _ as *mut u8, size_of::<T>()) }
}
pub fn cast_slice_mut<I: NoatunStorable, O: NoatunStorable>(s: &mut [I]) -> &mut [O] {
    const {
        assert!(align_of::<O>() <= align_of::<I>());
    }
    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    unsafe { slice::from_raw_parts_mut(s.as_mut_ptr() as *mut O, count_o) }
}
pub fn cast_slice<I: NoatunStorable, O: NoatunStorable>(s: &[I]) -> &[O] {
    const {
        assert!(align_of::<O>() <= align_of::<I>());
    }
    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    unsafe { slice::from_raw_parts(s.as_ptr() as *const O, count_o) }
}

/// Requires alignment to be correct at runtime, panics otherwise
pub fn dyn_cast_slice<I: NoatunStorable, O: NoatunStorable>(s: &[I]) -> &[O] {
    assert!((s.as_ptr() as *mut O).is_aligned());

    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    unsafe { slice::from_raw_parts(s.as_ptr() as *const O, count_o) }
}
/// Requires alignment to be correct at runtime, panics otherwise
pub fn dyn_cast_slice_mut<I: NoatunStorable, O: NoatunStorable>(s: &mut [I]) -> &mut [O] {
    assert!((s.as_ptr() as *mut O).is_aligned());
    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    unsafe { slice::from_raw_parts_mut(s.as_mut_ptr() as *mut O, count_o) }
}

pub fn read_unaligned<T>(data: &[u8]) -> T {
    let raw = data as *const [u8] as *const T;
    unsafe { raw.read_unaligned() }
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
    ///    let ret: &mut Self = NoatunContext.allocate_storable();
    ///    ret.init_from_detached(detached);
    ///    ret
    /// ```
    /// The only cases where some other implementation is required is when 'Self' does
    /// not have a fixed size.
    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self>;
}

#[macro_export]
macro_rules! noatun_object {

    ( bounded_type pod $typ:ty) => {
        $crate::NoatunCell<$typ>
    };
    ( declare_field pod $typ: ty ) => {
        $crate::NoatunCell<$typ>
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


            #[derive(Debug)]
            #[repr(C)]
            pub struct $n where $( noatun_object!(bounded_type $kind $typ) : $crate::Object ),*
            {
                phantom: ::std::marker::PhantomPinned,
                $(
                    #[doc(hidden)]
                    $name : noatun_object!(declare_field $kind $typ)
                ),*
            }
            unsafe impl $crate::NoatunStorable for $n {}
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
                    let mut ret: ::std::pin::Pin<&mut Self> = $crate::NoatunContext.allocate();
                    ret.as_mut().init_from_detached(detached);
                    ret
                }

            }


    };

}

/// Get bytes of object that may contain padding bytes
pub(crate) fn bytes_of_uninit<T: NoatunStorable>(t: &T) -> &[MaybeUninit<u8>] {
    let ptr = t as *const _ as *const MaybeUninit<u8>;
    // SAFETY:
    // Pointer is known to point to valid object. There may be padding bytes
    // that are unknown, but that's okay sinze we create a [MaybeUninit<u8>]
    unsafe { slice::from_raw_parts(ptr, size_of::<T>()) }
}

/// Get bytes of object that may be uninitialized.
pub(crate) fn bytes_of_maybe_uninit<T: NoatunStorable>(t: &MaybeUninit<T>) -> &[MaybeUninit<u8>] {
    let ptr = t as *const _ as *const MaybeUninit<u8>;
    // SAFETY:
    // Pointer is known to point to valid object. There may be padding bytes
    // that are unknown, but that's okay sinze we create a [MaybeUninit<u8>]
    unsafe { slice::from_raw_parts(ptr, size_of::<T>()) }
}
pub(crate) fn uninit_slice(slice: &[u8]) -> &[MaybeUninit<u8>] {
    // SAFETY:
    // MaybeUninit[u8] has exact same layout as u8
    unsafe { std::mem::transmute::<&[u8], &[MaybeUninit<u8>]>(slice) }
}

pub trait FixedSizeObject: Object<Ptr = ThinPtr> + NoatunStorable + Sized + 'static
where
    <Self as Object>::DetachedOwnedType: Sized,
{
}

impl<T: Object<Ptr = ThinPtr> + NoatunStorable + 'static> FixedSizeObject for T {}

pub trait Application: FixedSizeObject {
    type Message: Message<Root = Self>;
    /// Parameters that will be available in the "initialize_root" call.
    type Params;

    /// Default initialization function does nothing.
    ///
    /// You can override to add initialization data. This data is always
    /// present.
    ///
    /// Note that this present isn't actually persisted. So if later versions of
    /// the software initialize the database differently, this can affect the
    /// materialization of the database.
    fn initialize_root(_root: Pin<&mut Self>, _params: &Self::Params) {}
}
#[derive(Debug)]
#[repr(C)]
struct RawFatPtr {
    data: *const u8,
    size: usize,
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct FatPtr {
    start: usize,
    /// Second part of fat pointer, typically item count of tail
    count: usize,
}
impl FatPtr {
    /// Start index, and size in bytes
    pub fn from_idx_count(start: usize, count: usize) -> FatPtr {
        FatPtr { start, count }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ThinPtr(pub usize);

unsafe impl NoatunStorable for ThinPtr {}
unsafe impl NoatunStorable for FatPtr {}

impl Sealed for ThinPtr {}
impl Sealed for FatPtr {}

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

    unsafe fn access<'a, T: ?Sized>(&self) -> &'a T {
        NoatunContext.access_thin::<T>(*self)
    }

    unsafe fn access_mut<'a, T: ?Sized>(&self) -> Pin<&'a mut T> {
        Pin::new_unchecked(NoatunContext.access_thin_mut::<T>(*self))
    }

    #[doc(hidden)]
    unsafe fn access_ctx<'a, T: ?Sized>(&self, context: &DatabaseContextData) -> &'a T {
        context.access_thin::<T>(*self)
    }

    #[doc(hidden)]
    unsafe fn access_ctx_mut<'a, T: ?Sized>(&self, context: &mut DatabaseContextData) -> &'a mut T {
        context.access_thin_mut::<T>(*self)
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
            std::mem::size_of::<RawFatPtr>(),
        );

        let raw: RawFatPtr = unsafe { transmute_copy(&(addr as *const T)) };

        FatPtr {
            start: ((addr as *const T as *const u8 as usize) - (buffer_start as usize)),
            count: raw.size,
        }
    }
    fn as_generic(&self) -> GenPtr {
        GenPtr::Fat(*self)
    }

    fn is_null(&self) -> bool {
        self.start == 0
    }

    unsafe fn access<'a, T: ?Sized>(&self) -> &'a T {
        NoatunContext.access_fat::<T>(*self)
    }

    unsafe fn access_mut<'a, T: ?Sized>(&self) -> Pin<&'a mut T> {
        Pin::new_unchecked(NoatunContext.access_fat_mut::<T>(*self))
    }

    unsafe fn access_ctx<'a, T: ?Sized>(&self, context: &DatabaseContextData) -> &'a T {
        context.access_fat::<T>(*self)
    }

    unsafe fn access_ctx_mut<'a, T: ?Sized>(&self, context: &mut DatabaseContextData) -> &'a mut T {
        context.access_fat_mut::<T>(*self)
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
        let tself = unsafe { self.get_unchecked_mut() };
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

use crate::private::Sealed;
use crate::undo_store::magic_initialize_ptr;
pub use paste::paste;
use tracing::warn;

noatun_object!(
        struct Kalle {
            pod hej:u32,
            pod tva:u32,
            object da: NoatunCell<u32>
        }
);
noatun_object!(
    struct Nalle {
        pod hej:u32,
        pod tva:u32,
        object da: NoatunCell<u32>
    }
);

pub fn msg_serialize<T: savefile::Serialize + savefile::Packed>(
    obj: &T,
    mut writer: impl Write,
) -> anyhow::Result<()> {
    Ok(savefile::Serializer::bare_serialize(&mut writer, 0, obj)?)
}

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
