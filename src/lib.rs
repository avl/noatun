#![doc(html_favicon_url = "../../../icon/noatun-black128.png")]
#![doc = include_str!("../doc_helper/docs-svg.md")] // Run `update_docs.sh` to update this from `docs/docs.md`
#![allow(clippy::collapsible_if)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::bool_comparison)]
#![deny(clippy::missing_safety_doc)]
#![deny(clippy::undocumented_unsafe_blocks)]
#![deny(missing_docs)]
#![allow(clippy::let_and_return)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::expect_fun_call)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::derivable_impls)]

pub use crate::data_types::{NoatunCell, OpaqueNoatunCell};
use crate::noatun_instant::Instant;
use crate::private::Sealed;
use crate::sequence_nr::Tracker;
use anyhow::{bail, Result};
use chrono::{DateTime, SecondsFormat, Utc};
pub use cutoff::{CutOffDuration, CutOffState};
pub use database::{Database, DatabaseSettings, OpenMode};
use datetime_literal::datetime;
pub use paste::paste;
pub(crate) use projection_store::DatabaseContextData;
use rand::RngCore;
use savefile::Deserializer;
pub use savefile_derive::Savefile;
#[cfg(feature = "serde")]
use serde::de::DeserializeOwned;
#[cfg(feature = "serde")]
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::borrow::Borrow;
use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::{transmute_copy, MaybeUninit};
use std::ops::{Add, Sub};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::null_mut;
use std::slice;
use std::time::Duration;

use tracing::error;

pub mod diagnostics;
mod disk_abstraction;
mod message_store;
mod mini_pather;
mod projection_store;
#[cfg(feature = "debug_color")]
mod term_colors;
mod undo_store;

pub mod ratatui_inspector;

#[cfg(not(target_endian = "little"))]
compile_error!("Noatun currently only supports little-endian machines");

/// Module use to abstract 'Instant'. If the tokio feature is activated,
/// it resolves to 'tokio::time::Instant', otherwise std::time::Instant.
pub mod noatun_instant;

#[cfg(feature = "debug_color")]
use term_colors as colors;

#[cfg(not(feature = "debug_color"))]
mod dummy_term_colors;

#[cfg(not(feature = "debug_color"))]
use dummy_term_colors as colors;

#[cfg(feature = "debug_color")]
use crate::colors::{colored_hex_int, colored_hex_sint};
use crate::cutoff::CutOffTime;

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

pub(crate) mod platform_specific;

mod boot_checksum;
pub(crate) mod disk_access;
mod sha2_helper;

pub mod simple_metrics;
mod xxh3_vendored;

pub mod prelude {
    //! Commonly used types

    pub use crate::data_types::{
        NoatunCell, NoatunHashMap, NoatunKey, NoatunString, NoatunVec, OpaqueNoatunCell,
        OpaqueNoatunString, OpaqueNoatunVec,
    };
    pub use crate::database::{Database, DatabaseSession, DatabaseSessionMut, DatabaseSettings};
    pub use crate::{
        CutOffDuration, CutOffState, Message, MessageId, NoatunPod, NoatunStorable, NoatunTime,
        Object,
    };
}

#[doc(hidden)]
#[cfg(feature = "debug")]
#[macro_export]
macro_rules! dprintln {
    ($($x:tt)*) => { std::println!($($x)*) }
}

#[doc(hidden)]
#[cfg(not(feature = "debug"))]
#[macro_export]
macro_rules! dprintln {
    ($($x:tt)*) => {{}};
}

/// Hasher for hashing noatun database schemas.
///
/// Every type that is to be used in the noatun materialized view must support
/// creating a unique hash representing its own exact memory layout. This mechanism is used to
/// automatically rebuild the materialized view any time the materialized types are changed.
pub struct SchemaHasher(Sha256);

impl SchemaHasher {
    /// Hash the given string
    pub fn write_str(&mut self, s: &str) {
        self.0.update([0u8]);
        self.0.update(s.len().to_le_bytes());
        self.0.update(s.as_bytes());
    }
    /// Hash the given usize.
    pub fn write_usize(&mut self, n: usize) {
        self.0.update([1u8]);
        self.0.update(n.to_le_bytes());
    }
}

/// Calculate the correct hash for the schema of the given object
pub fn calculate_schema_hash<T: Object>() -> [u8; 16] {
    let mut temp = SchemaHasher(Sha256::default());
    T::hash_object_schema(&mut temp);
    use sha2::digest::FixedOutput;
    temp.0.finalize_fixed()[0..16].try_into().unwrap()
}

impl Write for SchemaHasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A trait for things that can be stored in the noatun materialized view.
///
/// # Safety
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
/// Type is also allowed to contain indices inside the database file ([`ThinPtr`]/[`FatPtr`]).
///
/// Note that tuples do not fulfill the criteria (no guaranteed stable memory layout).
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be stored in a noatun database.",
    label = "`{Self}` does not implement NoatunStorable.",
    note = "For structs, use the `noatun_object!`-macro to create a noatun object type.",
    note = "For collections, try `NoatunHashMap` or `NoatunVec`.",
    note = "Manually implementing this trait is not recommended, but can be possible. See trait docs."
)]
pub unsafe trait NoatunStorable: Sized + 'static {
    /// Return an instance of T that has a memory-representation of all zeroes.
    ///
    /// An all-zero instance must be valid for every NoatunStorable instance
    fn zeroed<T: NoatunStorable>() -> T {
        // Safety: All-zero bit-pattern must be valid for NoatunStorable object
        unsafe { MaybeUninit::<T>::zeroed().assume_init() }
    }

    /// Shallow copy source to self, unconditionally overwriting self.
    fn copy_from(&mut self, source: &Self) {
        // Safety: Source and destination are both valid
        unsafe {
            let temp = (source as *const Self).read();
            (self as *mut Self).write(temp);
        }
    }

    /// Initialize 'dest' from source.
    ///
    /// This is done using a simple memory copy.
    fn initialize(dest: &mut MaybeUninit<Self>, source: &Self) {
        // Safety: 'self' is a valid ref, dest is valid for writing
        unsafe {
            let temp = (source as *const Self).read();
            (dest as *mut MaybeUninit<Self> as *mut Self).write(temp);
        }
    }

    /// Write a unique value to 'hasher', that uniquely identifies this data type.
    ///
    /// This is used to detect schema conflicts between persisted materialized views
    /// and in-memory representations.
    ///
    /// The following guidelines are provided:
    ///  * Structs should write their full name, including crate name, their field count, and then
    ///    call each field's `hash_schema`.
    ///  * Newtypes should write their full name, including crate name, and the hash of their
    ///    inner type (unless the inner type is a primitive), and a '/' followed by a version number.
    ///    The version number must be changed if memory format changes.
    ///  * Primitives write their name prefixed by "std::" (this is implemented by Noatun).
    ///  * Collections, or more advanced types made specifically for Noatun
    ///    are encouraged to write their full name, plus a '/' followed by version
    ///    number, and if they are generic, the number of generic parameters and the hashes
    ///    of each generic.
    fn hash_schema(hasher: &mut SchemaHasher);
}

/// Trait for noatun storable objects that can be unpin, that don't contain any
/// pointers or references inside the noatun materialized view
///
/// The type should not contain any [`Tracker`] instances.
///
/// See `NoatunStorable` for further requirements.
///
/// # Safety
///  * The type must be completely self-contained, it must not contain pointers
///    or references to other data, in any way.
pub unsafe trait NoatunPod: Copy + NoatunStorable {}

mod noatun_storable_impls {
    use super::NoatunStorable;
    use crate::SchemaHasher;
    macro_rules! make_noatun_storable_primitive {
        ($t: ident) => {
            // Safety: Type is a primitive and trivially NoatunStorable
            unsafe impl NoatunStorable for $t {
                fn hash_schema(hasher: &mut SchemaHasher) {
                    hasher.write_str(concat!("std::", stringify!($t)));
                }
            }
            // Safety: Type is a primitive and trivially NoatunPod
            unsafe impl $crate::NoatunPod for $t {}
        };
    }

    make_noatun_storable_primitive!(u8);
    make_noatun_storable_primitive!(u16);
    make_noatun_storable_primitive!(u32);
    make_noatun_storable_primitive!(u64);
    make_noatun_storable_primitive!(u128);
    make_noatun_storable_primitive!(i8);
    make_noatun_storable_primitive!(i16);
    make_noatun_storable_primitive!(i32);
    make_noatun_storable_primitive!(i64);
    make_noatun_storable_primitive!(i128);
    make_noatun_storable_primitive!(isize);
    make_noatun_storable_primitive!(usize);

    // Safety: Slices of NoatunStorable objects are themselves NoatunStorable
    unsafe impl<T: NoatunStorable, const N: usize> NoatunStorable for [T; N] {
        fn hash_schema(hasher: &mut SchemaHasher) {
            hasher.write_str(concat!("std::[_:", stringify!(N), "]"));
            T::hash_schema(hasher);
        }
    }
}

thread_local! {
    /// The current context for the running thread.
    ///
    /// This is always set when user code runs in [`Message::apply`],
    /// [`DatabaseSession::with_root`] or similar methods.
    pub static CONTEXT: Cell<*mut DatabaseContextData> = const { Cell::new(null_mut()) };
}

#[cfg(any(feature = "debug", debug_assertions))]
thread_local! {
    /// In debug-builds, this thread local data tracks which node is current processed
    /// Useful for debugging printouts and logging.
    pub static DEBUG_NODE: Cell<u16> = const { Cell::new(0) };
}

#[doc(hidden)]
#[cfg(any(feature = "debug", debug_assertions))]
#[macro_export]
macro_rules! track_node {
    ($node:expr) => {
        $crate::DEBUG_NODE.set($node);
    };
}

#[doc(hidden)]
#[cfg(not(any(feature = "debug", debug_assertions)))]
#[macro_export]
macro_rules! track_node {
    ($_node:expr) => {};
}

#[doc(hidden)]
#[inline]
pub fn cur_node() -> u16 {
    #[cfg(any(feature = "debug", debug_assertions))]
    {
        DEBUG_NODE.get()
    }
    #[cfg(not(any(feature = "debug", debug_assertions)))]
    {
        65535
    }
}

/// Global unit struct used for accessing the current thread local noatun context.
///
/// The context gives access to the materialized view and its metadata.
///
/// Only advanced users of noatun need to interact with this. It's needed to develop
/// new custom data types for noatun. Regular structs can be implemented using
/// the [`crate::noatun_object`]-macro, but custom collections or other special types
/// may need to interact with `NoatunContext` directly.
#[derive(Clone, Copy)]
pub struct NoatunContext;

/// Always returns a valid pointer (panics if this is not possible).
///
/// The pointer is guaranteed to outlive the current materialized view root object.
#[inline]
fn get_context_mut_ptr() -> *mut DatabaseContextData {
    let context_ptr = CONTEXT.get();
    if context_ptr.is_null() {
        panic!(
            "No NoatunContext is presently available on this thread. The noatun-database \
               may only be accessed from within Message apply, and from `with_root`-methods."
        );
    }
    // Safety: Context is valid
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

impl NoatunContext {
    /// Verify that reading from opaque messages is currently allowed.
    ///
    /// Such reading is not allowed during message application (while building the
    /// materialized view).
    #[inline]
    pub fn assert_opaque_access_allowed(self, used_type: &str, alternative_type: &str) {
        // Safety: Context is valid
        let context = unsafe { &*get_context_ptr() };

        if context.is_message_apply() {
            panic!(
                "An attempt was made to read from {used_type} within Message::apply. \
                 This is not allowed. Avoid reading this data, or change to use {alternative_type} instead."
            );
        }
    }

    /// Take ownership of the given tracker, after having overwritten all its associated data.
    ///
    /// Any previous owner will have its refcount decreased.
    ///
    /// Unless 'opaque' is false, the owner of the tracker is marked as being non-opaque
    /// (not readable during message application). Note, all writers of a specific tracker
    /// must agree on its opaqueness. The opaqueness is a property of the tracked data, not
    /// the owner of the tracker.
    ///
    /// # Safety
    /// The 'seq' pointer must point to valid, unaliased memory.
    pub unsafe fn update_tracker_ptr(self, seq: *mut Tracker, opaque: bool) {
        let context_ptr = get_context_mut_ptr();
        // Safety: Context is valid
        unsafe { (*context_ptr).update_tracker_ptr(seq, opaque) }
    }

    /// Report that the data for the given tracker has been cleared.
    ///
    /// This does not take ownership of the data. The tracked data will not have an owner
    /// after this call returns. To ensure that the writing message
    /// does not get pruned, it should mark itself as a tombstone.
    ///
    /// # Safety
    /// The `seq` pointer must be valid to dereference.
    pub unsafe fn clear_tracker_ptr(self, seq: *mut Tracker, opaque: bool) {
        let context_ptr = get_context_mut_ptr();
        // Safety: Context is valid
        unsafe { (*context_ptr).clear_registrar_ptr(seq, opaque) }
    }

    /// Return a mutable reference to the start of the materialized view's store.
    pub fn start_ptr_mut(self) -> *mut u8 {
        let context_ptr = get_context_mut_ptr();
        // Safety: Context is valid
        unsafe { (*context_ptr).start_ptr_mut() }
    }

    /// Return a non-mutable reference to the start of the materialized view's store.
    pub fn start_ptr(self) -> *const u8 {
        let context_ptr = get_context_ptr();
        // Safety: Context is valid
        unsafe { (*context_ptr).start_ptr() }
    }

    /// Calculate the offset for the given object, inside the backing store.
    pub fn index_of<T: Object + ?Sized>(self, t: &T) -> T::Ptr {
        let context_ptr = get_context_ptr();
        // Safety: Context is valid
        unsafe { (*context_ptr).index_of(t) }
    }

    // Just used by single test. Consider removing?
    #[cfg(test)]
    pub(crate) unsafe fn rewind(self, new_time: crate::sequence_nr::SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        unsafe { (*context_ptr).rewind(new_time) }
    }

    #[cfg(test)]
    pub(crate) fn set_next_seqnr(self, seqnr: crate::sequence_nr::SequenceNr) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).set_next_seqnr(seqnr) }
    }

    /// Copy all bytes designated by 'src' into region starting at 'dest_index'
    ///
    /// # Safety
    /// The two pointers must be valid, and not currently aliased.
    pub unsafe fn copy_ptr(&self, src: FatPtr, dest_index: ThinPtr) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).copy_bytes(src, dest_index) }
    }
    /// Clear all bytes designated by 'dst'
    ///
    /// # Safety
    /// The pointer must be valid, and not currently aliased.
    pub unsafe fn zero(&self, dst: FatPtr) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).zero(dst) }
    }
    /// Clear all bytes of 'dst'
    pub fn zero_storable<T: NoatunStorable>(&self, dst: Pin<&mut T>) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).zero_storable(dst) }
    }
    /// Clear all bytes of 'dst'
    pub fn zero_internal<T: NoatunStorable>(&self, dst: &mut T) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).zero_internal(dst) }
    }

    /// Copy 'size_bytes' bytes from 'src' to 'dest_index'.
    ///
    /// # Safety
    /// The two pointers must be valid, and not currently aliased.
    pub unsafe fn copy_sized(&self, src: ThinPtr, dest_index: ThinPtr, size_bytes: usize) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).copy_bytes_len(src, dest_index, size_bytes) }
    }
    /// Copy from 'src' to 'dst'.
    ///
    /// This overwrites any previous value of 'dst'.
    /// Don't use this if 'dst' contains a 'Tracker', since this is just a dumb memcpy.
    pub fn copy<T: NoatunStorable>(&self, src: &T, dst: &mut T) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).copy_storable(src, dst) }
    }

    /// Return the offset of 'ptr' within the materialized view store.
    pub fn index_of_ptr(&self, ptr: *const u8) -> ThinPtr {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).index_of_ptr(ptr) }
    }
    /// Allocate 'size' bytes with alignment 'align'
    pub fn allocate_raw(&self, size: usize, align: usize) -> *mut u8 {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).allocate_raw(size, align) }
    }
    /// Report that data owned by 'tracker' has been fully overwritten.
    pub fn update_tracker(&self, tracker: &mut Tracker, opaque: bool) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe {
            (*context_ptr).update_registrar(tracker, opaque);
        }
    }
    /// Copy 'value' to 'dest'. Any previous contents of 'dest' are lost.
    ///
    /// Do not use if 'dest' contains [`Tracker`]-instances.
    pub fn write<T: NoatunStorable>(&self, value: T, dest: Pin<&mut T>) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).write_storable(value, dest) }
    }
    /// Report that the current message has removed data in a way that is not tracked.
    ///
    /// The message is to be marked as a tombstone, and will not be pruned until it
    /// has become older than the cutoff age (with some other conditions).
    pub fn wrote_tombstone(&self) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).set_wrote_tombstone() }
    }
    pub(crate) fn write_internal<T: NoatunStorable>(&self, value: T, dest: &mut T) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).write_storable(value, Pin::new_unchecked(dest)) }
    }

    /// Write 'value' to 'dest'
    #[allow(clippy::not_unsafe_ptr_arg_deref)] //write_storable_ptr actually validates 'dest'
    pub fn write_ptr<T: NoatunStorable>(&self, value: T, dest: *mut T) {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).write_storable_ptr(value, dest) }
    }

    /// Allocate an instance of 'T' and return a mutable pinned reference to it.
    ///
    /// # Safety
    /// The returned reference must be tied to the lifetime of the current
    /// root object, before being returned to safe code.
    pub unsafe fn allocate<'a, T: NoatunStorable>(&self) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).allocate_storable() }
    }

    /// Allocate an instance of 'T' and return a mutable pinned reference to it.
    ///
    /// # Safety
    /// The returned reference must be tied to the lifetime of the current
    /// root object, before being returned to safe code.
    pub unsafe fn allocate_obj<'a, T: Object>(&self) -> Pin<&'a mut T> {
        let context_ptr = get_context_mut_ptr();
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).allocate_obj() }
    }

    /// Access an instance of 'T' at the location given by the ptr..
    ///
    /// # Safety
    /// The ptr must be valid
    pub unsafe fn access_thin<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).access_thin::<T>(ptr) }
    }
    /// Access an instance of 'T' at the location given by the ptr..
    ///
    /// # Safety
    /// The ptr must be valid and unaliased
    pub unsafe fn access_thin_mut<'a, T: ?Sized>(&self, ptr: ThinPtr) -> &'a mut T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).access_thin_mut::<T>(ptr) }
    }
    /// Access an instance of 'T' at the location given by the ptr..
    ///
    /// # Safety
    /// The ptr must be valid
    pub unsafe fn access_fat<'a, T: ?Sized>(&self, ptr: FatPtr) -> &'a T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).access_fat::<T>(ptr) }
    }
    /// Access an instance of 'T' at the location given by the ptr..
    ///
    /// # Safety
    /// The ptr must be valid and unaliased
    pub unsafe fn access_fat_mut<'a, T: ?Sized>(&self, ptr: FatPtr) -> &'a mut T {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            panic!("No NoatunContext available");
        }
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).access_fat_mut::<T>(ptr) }
    }

    /// Report that the current message has observed (=read) the data tracked
    /// by 'tracker'.
    pub fn observe_registrar(self, tracker: Tracker) {
        let context_ptr = CONTEXT.get();
        if context_ptr.is_null() {
            return;
        }
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        if unsafe { (*(context_ptr as *const DatabaseContextData)).is_message_apply == false } {
            return;
        }
        // Safety:
        // context_ptr is a valid pointer (get_context_mut_ptr always returns
        // valid pointers. The pointer is valid for the lifetime of the current
        // root object, which does not end in this function.
        unsafe { (*context_ptr).observe_registrar(tracker) }
    }
}

/// The identity of a message.
///
/// The id consists of a timestamp with millisecond precision, and a random part.
/// In total, the id is always exactly 16 bytes, and is meant to be truly globally unique.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Savefile)]
#[repr(transparent)]
pub struct MessageId {
    data: [u32; 4],
}

impl Hash for MessageId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Safety: self.data is 16 bytes
        let data1: [u64; 2] = unsafe { std::mem::transmute(self.data) };
        state.write_u64(data1[0]);
        state.write_u64(data1[1]);
    }
}

// Safety: MessageId has only NoatunStorable fields
unsafe impl NoatunStorable for MessageId {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::MessageId/1")
    }
}

const _ASSURE_SUPPORTED_USIZE: () = const {
    if size_of::<usize>() != 8 {
        panic!("noatun currently only supports 64 bit platforms with 64 bit usize");
    }
};

impl MessageId {
    /// Return a shortened representation of a MessageId.
    ///
    /// This representation is not guaranteed to be unique, but can be useful in tests
    /// and debugging. The returned value has 32 bits of entropy, meaning it is very likely
    /// to be unique within a small set of messages. For up to 93 messages, the chance of
    /// collision between 'short' ids is approximately 1 in a million.
    pub fn short(&self) -> String {
        format!("{:08x}", self.data[3])
    }
}

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time_ms = self.timestamp();

        let time = chrono::DateTime::from_timestamp_millis(time_ms.as_ms() as i64).unwrap();

        let time_str = time.to_rfc3339_opts(SecondsFormat::Millis, true);
        #[cfg(feature = "debug_color")]
        {
            write!(
                f,
                "{:?}-{}-{}-{}",
                time_str,
                colored_hex_sint((self.data[1] & 0xffff).wrapping_sub(0x4000) as i32),
                colored_hex_int(self.data[2]),
                colored_hex_int(self.data[3])
            )
        }

        #[cfg(not(feature = "debug_color"))]
        {
            if cfg!(all(test)) {
                write!(
                    f,
                    "{:?}-{:x}-{:x}-{:x}",
                    time_str,
                    (self.data[1] & 0xffff).wrapping_sub(0x4000) as i32,
                    self.data[2],
                    self.data[3]
                )
            } else {
                write!(
                    f,
                    "{:?}-{:04x}-{:08x}-{:08x}",
                    time_str,
                    (self.data[1] & 0xffff).wrapping_sub(0x4000) as i32,
                    self.data[2],
                    self.data[3]
                )
            }
        }
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

thread_local! {
    static TEST_EPOCH: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Reset the random id sequence used during test to obtain predictable EphemeralNodeId
#[cfg(test)]
pub fn reset_random_id() {
    crate::distributor::NON_RANDOM_EPHEMERAL_NODE_ID_COUNTER
        .store(0, std::sync::atomic::Ordering::SeqCst);
    NON_RANDOM_ID_COUNTER.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Helper useful for tests
///
/// Returns the elapsed time since the value provided using ['set_test_epoch'].
/// Also see [`test_elapsed`].
pub fn test_epoch() -> Instant {
    TEST_EPOCH.with(|epoch| match epoch.get() {
        None => {
            let now = Instant::now();
            epoch.set(Some(now));
            now
        }
        Some(val) => val,
    })
}

/// Helper useful for tests.
///
/// Sets the current test epoch for this thread. Also see [`test_elapsed`].
pub fn set_test_epoch(instant: Instant) {
    TEST_EPOCH.with(|epoch| {
        epoch.set(Some(instant));
    })
}

/// Returns the time elapsed since the test epoch established using
/// [`set_test_epoch`]
pub fn test_elapsed() -> Duration {
    test_epoch().elapsed()
}

impl MessageId {
    /// An all-zero MessageId. This is generally not useful
    /// outside of tests
    pub const ZERO: MessageId = MessageId { data: [0u32; 4] };
    /// Returns the smallest of 'self' and 'other'.
    ///
    /// Messages with earlier timestamps sort before messages with later timestamps.
    pub fn min(self, other: MessageId) -> MessageId {
        if self < other {
            self
        } else {
            other
        }
    }
    /// Return the 16 bytes of this message id as a slice
    pub fn as_bytes(&self) -> &[u8] {
        cast_slice(&self.data)
    }
    /// Return true if the message is equal to ZERO
    pub fn is_zero(&self) -> bool {
        self.data[0] == 0 && self.data[1] == 0
    }

    /// Next larger MessageId.
    /// Panics if current id is the largest possible.
    /// Note: This *CAN* change the timestamp of the message, though this is unlikely.
    /// Note: The timestamp can at most increase by 1 ms.
    /// Note: Use this with care to not accidentally create duplicate ids
    ///
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
    ///
    /// The time will be in 1970-01-01 T 00:00:00
    pub fn new_debug(nr: u32) -> Self {
        Self {
            data: [0, 0, 0, nr],
        }
    }

    /// For a MessageId created by [`Self::new_debug`], return the value of 'nr' used
    /// in the construction.
    ///
    /// This is just the last 32 bits of the MessageId.
    pub fn debug_value(&self) -> u32 {
        self.data[3]
    }

    /// Create an artificial MessageId, mostly useful for tests and possibly debugging.
    ///
    /// Like new_debug, but creates an id with a timestamp of '2020-01-01 T 00:00:00'
    pub fn new_debug2(nr: u64) -> Self {
        MessageId::from_parts_for_test(
            NoatunTime::from_datetime(datetime!(2020-01-01 T 00:00:00 Z)),
            nr,
        )
    }

    /// Return a new randomized message id, with timestamp equal to now
    pub fn new_random() -> Result<Self> {
        Self::generate_for_time(NoatunTime::now())
    }

    /// Return a random MessageId, for the given timestamp.
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

        let mut temp = Self::from_parts(time, random_part)?;

        // Leave space for same-timestamp increments
        temp.pred_succ_reserve();

        Ok(temp)
    }

    /// Reserve space for predecessors/successors within the same timestamp
    fn pred_succ_reserve(&mut self) {
        let mut bits = (self.data[1] >> 14) & 3;
        if bits == 0 {
            bits = 1;
        }
        if bits == 3 {
            bits = 2;
        }
        self.data[1] &= 0xffff_3fff;
        self.data[1] |= bits << 14;
    }

    /// Creates a new message id which is guaranteed to have a greater sort order
    /// than 'self', but the same timestamp.
    ///
    /// NOTE! Every original NoatunMessage is guaranteed to have at least 8191 successors.
    /// This means that `unique_successor` can be used to 'update' a message at least 8191 times.
    ///
    /// # Returns
    /// The new message-id, or an error if the successor pool is exhausted or if the
    /// timestamp is out of range.
    pub fn unique_successor(&self) -> Result<MessageId> {
        let mut counter = self.data[1] & 0xffff;
        if counter == 0xffff {
            bail!(
                "unique successors of {} exhausted, no more can be generated",
                self
            );
        }
        counter += 1;
        let time = self.timestamp();
        let mut raw = Self::generate_for_time(time)?;
        raw.data[1] &= 0xffff_0000;
        raw.data[1] |= counter;
        Ok(raw)
    }
    /// Creates a new message id which is guaranteed to have a smaller sort order
    /// than 'self', but the same timestamp.
    ///
    /// NOTE! Every original NoatunMessage is guaranteed to have at least 8191 predecessors.
    /// This means that `unique_predecessor` can be used to 'update' a message at least 8191 times.
    ///
    /// # Returns
    /// The new message-id, or an error if the predecessor pool is exhausted or if the
    /// timestamp is out of range.
    pub fn unique_predecessor(&self) -> Result<MessageId> {
        let mut counter = self.data[1] & 0xffff;
        if counter == 0 {
            bail!(
                "unique predecessors of {} exhausted, no more can be generated",
                self
            );
        }
        counter -= 1;
        let time = self.timestamp();
        let mut raw = Self::generate_for_time(time)?;
        raw.data[1] &= 0xffff_0000;
        raw.data[1] |= counter;
        Ok(raw)
    }

    /// Return a MessageId produced from the given time and random part.
    ///
    /// This method is mainly intended for tests.
    pub fn from_parts_for_test(time: NoatunTime, random: u64) -> MessageId {
        let mut data = [0u8; 10];
        data[2..10].copy_from_slice(&random.to_le_bytes());
        let mut temp = Self::from_parts(time, data).unwrap();
        temp.pred_succ_reserve();
        temp
    }
    /// Return the timestamp of this message id
    pub fn timestamp(&self) -> NoatunTime {
        let mut alternative_value = (self.data[0] as u64) << 16;
        alternative_value |= (self.data[1] >> 16) as u64;
        NoatunTime(alternative_value)
    }

    /// Create a MessageId from the given time and random part.
    ///
    /// Note, this is a low level operation. Use [`Self::generate_for_time`] to generate
    /// a unique id for a specified timestamp.
    pub fn from_parts(time: NoatunTime, random: [u8; 10]) -> Result<MessageId> {
        let t: u64 = time.as_ms();
        Self::from_parts_raw(t, random)
    }
    /// Return true if the given timestamp can be used as the timestamp of a MessageId.
    ///
    /// Generally, only timestamps extremely far into the future are invalid, all other
    /// timestamps are valid.
    pub fn is_time_valid_for_message(time: NoatunTime) -> bool {
        time.as_ms() < 1 << 48
    }
    /// Create a MessageId from a timestamp part and a random part.
    ///
    /// Note, the time must be smaller than 2^48.
    pub fn from_parts_raw(time: u64, random: [u8; 10]) -> Result<MessageId> {
        if time >= 1 << 48 {
            bail!("Time value is too large");
        }
        let mut data = [0u8; 16];
        let time_le_bytes = time.to_le_bytes();
        data[0..4].copy_from_slice(&time_le_bytes[2..6]);
        data[4..6].copy_from_slice(&random[0..2]);
        data[6..8].copy_from_slice(&time_le_bytes[0..2]);
        data[8..16].copy_from_slice(&random[2..10]);

        Ok(MessageId {
            data: cast_storable(data),
        })
    }
}

/// Wall clock time
///
/// Internally, NoatunTime is the number of milliseconds elapsed since 1970-01-01 00:00:00 UTC.
/// In other words, this is a unix timestamp with millisecond precision.
///
/// Nominally NoatunTime is 48 bits, which makes the maximum value be sometime after the year
/// 10000.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Savefile)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(C)]
pub struct NoatunTime(pub u64);

// Safety: NoatunTime has only NoatunStorable fields
unsafe impl NoatunStorable for NoatunTime {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunTime/1")
    }
}
// Safety: NoatunTime has only NoatunPod fields
unsafe impl NoatunPod for NoatunTime {}

impl Add<Duration> for NoatunTime {
    type Output = NoatunTime;
    fn add(self, rhs: Duration) -> Self::Output {
        NoatunTime(self.0 + rhs.as_millis() as u64).truncated()
    }
}
impl Sub<Duration> for NoatunTime {
    type Output = NoatunTime;
    fn sub(self, rhs: Duration) -> Self::Output {
        NoatunTime(self.0.saturating_sub(rhs.as_millis() as u64)).truncated()
    }
}

impl Add<NoatunTime> for Duration {
    type Output = NoatunTime;
    fn add(self, rhs: NoatunTime) -> Self::Output {
        NoatunTime(rhs.0 + self.as_millis() as u64).truncated()
    }
}

impl PartialEq<CutOffTime> for NoatunTime {
    fn eq(&self, other: &CutOffTime) -> bool {
        *self == other.to_noatun_time()
    }
}

impl PartialOrd<CutOffTime> for NoatunTime {
    fn partial_cmp(&self, other: &CutOffTime) -> Option<Ordering> {
        Some(self.cmp(&other.to_noatun_time()))
    }
}

impl From<DateTime<Utc>> for NoatunTime {
    fn from(value: DateTime<Utc>) -> Self {
        let ms = value.timestamp_millis();
        if ms < 0 {
            NoatunTime(0)
        } else {
            NoatunTime(ms as u64).truncated()
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
        NoatunTime(self.0 + rhs.0).truncated()
    }
}

impl Sub for NoatunTime {
    type Output = NoatunTime;

    fn sub(self, rhs: Self) -> Self::Output {
        NoatunTime(self.0 - rhs.0).truncated()
    }
}

impl NoatunTime {
    /// Returns the number of milliseconds between 'self' and an earlier 'other'.
    ///
    /// If 'other' is later than 'self', 0 is returned.
    pub fn elapsed_ms_since(self, other: NoatunTime) -> u64 {
        let ms = self.0.saturating_sub(other.0);
        ms
    }

    #[doc(hidden)]
    pub fn debug_time(minutes: u64) -> NoatunTime {
        let noatun = NoatunTime::from(datetime!(2020-01-01 T 00:00:00 Z));
        (noatun + Duration::from_secs(minutes * 60)).truncated()
    }

    /// Find the next value that is a multiple of 'other', greater or equal to 'self'.
    pub fn next_multiple_of(self, other: NoatunTime) -> Option<NoatunTime> {
        NoatunTime(self.0.checked_next_multiple_of(other.0)?).validate()
    }

    /// Find the previous value that is a multiple of 'other', smaller or equal to 'self'.
    pub fn prev_multiple_of(self, other: NoatunTime) -> Option<NoatunTime> {
        Some(NoatunTime(
            self.0.checked_next_multiple_of(other.0)? - other.0,
        ))
    }

    /// Return the smallest possible increment of 'self'.
    ///
    /// This returns a value that is 1 ms later than 'self', except for the special case
    /// where 'self' == `NoatunTime::MAX`, in which case it returns `NoatunTime::MAX`.
    pub fn successor(&self) -> NoatunTime {
        NoatunTime(self.0 + 1).truncated()
    }
    /// Return the smallest possible decrement of 'self'.
    ///
    /// This returns a value that is 1 ms earlier than 'self', except for the special case
    /// where 'self' == `NoatunTime::EPOCH`, in which case it returns `NoatunTime::EPOCH`.
    pub fn saturating_predecessor(&self) -> NoatunTime {
        NoatunTime(self.0.saturating_sub(1))
    }

    /// Returns a NoatunTime equal to 1970-01-01 00:00:00 UTC
    pub const EPOCH: NoatunTime = NoatunTime(0);
    /// The largest possible NoatunTime: 10889-08-02 05:31:50.655 UTC.
    pub const MAX: NoatunTime = NoatunTime((1 << 48) - 1);

    /// Returns the current time.
    ///
    /// If the time is outside the range [1970-01-01T00:00:00Z, 10889-08-02T05:31:50.655Z],
    /// the returned value will be clamped to this range.
    pub fn now() -> Self {
        let millis = Utc::now().timestamp_millis().clamp(0, (1 << 48) - 1);
        Self(millis as u64)
    }

    /// Return this time expressed as the number of milliseconds since 1970-01-01 00:00:00 UTC.
    #[must_use]
    pub fn as_ms(self) -> u64 {
        self.0
    }

    /// Add the given number of milliseconds, returning a later time.
    ///
    /// If ms is 0, 'self' is returned unchanged.
    #[must_use]
    pub fn add_ms(self, ms: u64) -> NoatunTime {
        NoatunTime(self.0.saturating_add(ms)).truncated()
    }

    /// Subtract the given number of milliseconds, returning an earlier time.
    ///
    /// If ms is 0, 'self' is returned unchanged.
    pub fn sub_ms(self, ms: u64) -> NoatunTime {
        NoatunTime(self.0.saturating_sub(ms)).truncated()
    }

    /// Convert 'self' to [`DateTime<Utc>`]
    pub fn to_datetime(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_millis(self.0 as i64).unwrap()
    }

    const fn validate(self) -> Option<NoatunTime> {
        if self.0 > NoatunTime::MAX.0 {
            return None;
        }
        Some(self)
    }
    const fn truncated(self) -> NoatunTime {
        if self.0 > NoatunTime::MAX.0 {
            return NoatunTime::MAX;
        }
        self
    }
    /// Convert from [`DateTime<Utc>`] to NoatunTime.
    ///
    /// In the unlikely event that 't' is too large for NoatunTime (year >10000),
    /// this will truncate the value.
    ///
    /// The reason for not making this fallible is that the failure is extremely unlikely
    /// to occur in practice, and making this fallible is frequently inconvenient.
    ///
    /// For a fallible version, see: [`Self::try_from_datetime`].
    pub const fn from_datetime(t: DateTime<Utc>) -> NoatunTime {
        let ts = t.timestamp_millis();
        if ts < 0 {
            return NoatunTime::EPOCH;
        }
        NoatunTime(ts as u64).truncated()
    }

    /// Fallible version of [`Self::from_datetime`].
    ///
    /// If the input time is before [`Self::EPOCH`] or after [`Self::MAX`], this returns
    /// None.
    pub const fn try_from_datetime(t: DateTime<Utc>) -> Option<NoatunTime> {
        let ts = t.timestamp_millis();
        if ts < 0 {
            return None;
        }
        Some(NoatunTime(ts as u64).truncated())
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

/// An event handled by Noatun
///
/// Messages carry all primary data in Noatun. The noatun materialized
/// view is a function of the complete set of messages. Messages are the events in
/// the event source pattern followed by Noatun.
///
/// As an implementer of Message you need to provide:
///
///  * [`Self::apply`], that tells Noatun how to apply a message to the materialized view
///  * [`Self::Root`] - type of materialized view root object that the message must be applied to
///  * [`Self::Serializer`] - serializer to serialize message to wire and disk.
///    See trait [`NoatunMessageSerializer`]. Noatun provides impls for serde postcard and
///    savefile out of the box.
///
/// Note, Noatun will keep message instances in RAM in some contexts, for example
/// when messages are sitting in internal send buffers. This is generally
/// ok for messages up to a few kilobytes in size. However, messages that
/// carry large amounts of data may wish to use temporary files to conserve RAM.
pub trait Message: Debug + Sized + 'static {
    /// The type of materialized view that this message is compatible with.
    ///
    /// Each noatun application is expected to have a single message type, and
    /// thus a single root object that contains all information in the system.
    type Root: Object + NoatunStorable;

    /// Designated the serializer needed to serialize this object.
    ///
    /// Having this as an associated type means we can ensure messages are serializable,
    /// without requiring any particular serializer traits (like serde's Serialize/Deserialize).
    type Serializer: NoatunMessageSerializer<Self>;

    /// Apply the message to the noatun materialized view, with root type [`Self::Root`].
    ///
    /// Implementations of this method must never fail, and must not panic. Automatic proptesting
    /// or fuzz-testing is encouraged to ensure this.
    ///
    /// Logical errors that occur during application should be stored in the database. This so that
    /// any UI or other user of the materialized view can show the appropriate error message.
    fn apply(&self, id: MessageId, root: Pin<&mut Self::Root>);

    /// Affect the retention of this message type.
    ///
    /// Sometimes it can be desired to make sure messages linger in the system, even
    /// though they don't affect the materialized view anymore. One example is when
    /// a complete log of historical events is desired for traceability or compliance.
    fn persistence(&self) -> Persistence {
        Persistence::UntilOverwritten
    }
}

/// Extension trait to provide serialization for Message impls
pub trait MessageExt: Message {
    /// Serialize 'self' to the given writer
    fn serialize<W: Write>(&self, writer: W) -> Result<()> {
        Self::Serializer::serialize(self, writer)
    }

    /// Deserialize a 'Self' from the buffer
    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        Self::Serializer::deserialize(buf)
    }
}

impl<T> MessageExt for T where T: Message {}

/// A Noatun-message has a header with the id, and the list of parents.
/// See [`MessageFrame`].
#[derive(Debug, Clone, PartialEq)]
pub struct MessageHeader {
    /// The identifier of this message.
    ///
    /// The identifier is fixed for the entire life cycle of the message.
    pub id: MessageId,
    /// The set of parents. As parents are pruned, the set of parents is pruned as well.
    /// Pruning is always done in a way that will not affect the materialized view end result.
    pub parents: Vec<MessageId>,
}

/// A MessageFrame is the fundamental building block of a Noatun database.
///
/// The MessageFrame contains the user-supplied Message type, as well as the message's
/// globally unique identifier and its list of parents.
#[derive(Debug)]
pub struct MessageFrame<M: Message> {
    /// Header of message. Contains identity and parents.
    pub header: MessageHeader,
    /// Actual message payload. User defined type.
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
    /// Create a new MessageFrame instance from its constituent parts
    pub fn new(id: MessageId, parents: Vec<MessageId>, payload: M) -> Self {
        Self {
            header: MessageHeader { id, parents },
            payload,
        }
    }
    /// The MessageId of this message frame
    pub fn id(&self) -> MessageId {
        self.header.id
    }
}

pub(crate) fn catch_and_log(f: impl FnOnce(), abort: bool) {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(()) => {}
        Err(err) => {
            if let Some(err) = (*err).downcast_ref::<String>() {
                error!("apply method panicked: {:?}", err);
            } else if let Some(err) = err.downcast_ref::<&'static str>() {
                error!("apply method panicked: {:?}", err);
            } else {
                error!("apply method panicked.");
            }
            if abort {
                std::process::abort();
            }
        }
    }
}

/// A state-less object, mostly useful for testing
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DummyUnitObject;

// Safety: DummyUnitObject has no fields
unsafe impl NoatunStorable for DummyUnitObject {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::DummyUnitObject/1")
    }
}

impl Object for DummyUnitObject {
    type Ptr = ThinPtr;
    type NativeType = ();
    type NativeOwnedType = ();

    fn export(&self) -> Self::NativeType {}

    fn destroy(self: Pin<&mut Self>) {}

    fn init_from(self: Pin<&mut Self>, _detached: &Self::NativeType) {}

    unsafe fn allocate_from<'a>(_detached: &Self::NativeType) -> Pin<&'a mut Self> {
        // Safety: Newly allocated object is safely pinned
        unsafe { Pin::new_unchecked(&mut *std::ptr::dangling_mut::<DummyUnitObject>()) }
    }

    fn hash_object_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::DummyUnitObject/1");
    }
}

/// A pointer into the materialized view store.
pub enum GenPtr {
    /// A thin pointer.
    ///
    /// Internally, this is just a 64bit offset into the materialized view file.
    ///
    /// Thin pointers are used to reference objects of a fixed size, such as integer primitives
    /// or regular structs.
    Thin(ThinPtr),
    /// A fat pointer
    ///
    /// A fat pointer is different from a thin pointer in that it also has a size.
    /// Standard rust fat pointers can contain a vtable, but Noatun fat pointers are always
    /// just offset + size.
    Fat(FatPtr),
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct SerializableGenPtr {
    ptr: usize,
    count: usize, //usize::MAX for thin
}

// Safety: SerializableGenPtr has only NoatunStorable fields
unsafe impl NoatunStorable for SerializableGenPtr {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::SerializableGenPtr/1")
    }
}

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

/// Type abstracting any type of pointer into the materialized view
pub trait Pointer: NoatunStorable + Sealed + Copy + Debug + 'static {
    /// The offset of the first byte of the pointee
    fn start(self) -> usize;
    /// Create a pointer to the given object.
    ///
    /// `buffer_start` is the address in memory where the materialized view
    /// memory mapping starts.
    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self;
    /// Return a type-erased noatun pointer
    fn as_generic(&self) -> GenPtr;
    /// Return true if the pointer is null
    fn is_null(&self) -> bool;

    /// Return a reference to the pointee
    ///
    /// # Safety
    /// This pointer (self) must be valid
    unsafe fn access<'a, T: ?Sized>(&self) -> &'a T;
    /// Return a mutable reference to the pointee
    ///
    /// # Safety
    /// This pointer (self) must be valid and unaliased
    unsafe fn access_mut<'a, T: ?Sized>(&self) -> Pin<&'a mut T>;

    /// Return a reference to the pointee, given an explicit context
    ///
    /// # Safety
    /// This pointer (self) must be valid
    unsafe fn access_ctx<'a, T: ?Sized>(&self, context: &DatabaseContextData) -> &'a T;
    /// Return a mutable reference to the pointee, given an explicit context
    ///
    /// # Safety
    /// This pointer (self) must be valid and unaliased
    unsafe fn access_ctx_mut<'a, T: ?Sized>(&self, context: &mut DatabaseContextData) -> &'a mut T;
}

/// Convert from a slice of bytes to a T.
///
/// This is safe, since all NoatunStorable instances must be valid for any bit pattern.
pub fn from_bytes<T: NoatunStorable>(s: &[u8]) -> &T {
    assert_eq!(s.len(), size_of::<T>());
    assert!((s.as_ptr() as *mut T).is_aligned());

    // Safety: s is a valid object, with correct size and alignment as T.
    // Since T is NoatunStorable, any bit pattern is valid
    unsafe { &*s.as_ptr().cast::<T>() }
}

/// Convert from a slice of bytes to a T.
///
/// This is safe, since all NoatunStorable instances must be valid for any bit pattern.
pub fn from_bytes_mut<T: NoatunStorable>(s: &mut [u8]) -> &mut T {
    assert_eq!(s.len(), size_of::<T>());
    assert!((s.as_mut_ptr() as *mut T).is_aligned());

    // Safety:
    // We've checked alignment and size, and those are the only requirements
    // an Object need to be valid.
    unsafe { &mut *s.as_mut_ptr().cast::<T>() }
}

/// Cast between two different storable objects
///
/// This will panic at compile-time if 'I' and 'O' do not have the same size.
///
/// This is safe, since all bit patterns of NoatunStorable types are valid.
pub fn cast_storable<I: NoatunStorable, O: NoatunStorable>(i: I) -> O {
    const {
        if size_of::<I>() != size_of::<O>() {
            panic!("Source and destination size must be the same");
        }
    }
    // Safety: We checked the sizes, and since both types are NoatunStorabe,
    // any bit pattern is valid
    unsafe { transmute_copy::<I, O>(&i) }
}

/// Cast a reference to a NoatunStorable into a slice of bytes
pub fn bytes_of<T: NoatunStorable>(t: &T) -> &[u8] {
    // Safety:
    // FixedSizeObject instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe { slice::from_raw_parts(t as *const _ as *const u8, size_of::<T>()) }
}

/// Cast a reference to a NoatunStorable into a slice of bytes
pub fn bytes_of_mut<T: NoatunStorable>(t: &mut T) -> &mut [u8] {
    // Safety:
    // NoatunStorable instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe { slice::from_raw_parts_mut(t as *mut _ as *mut u8, size_of::<T>()) }
}

/// Cast a reference to a NoatunStorable into a slice of bytes
pub fn bytes_of_mut_object<T: Object>(t: &mut T) -> &mut [u8] {
    // Safety:
    // NoatunStorable instances can always be viewed as a set of by tes
    // That set of bytes can have uninitialized values, so we can't use the values.
    // Just copying uninitialized values is ok, and that's all we'll end up doing.
    unsafe { slice::from_raw_parts_mut(t as *mut _ as *mut u8, size_of::<T>()) }
}

/// Cast from a slice of I to a slice of O.
///
/// A compile time panic occurs if the alignment of O is not smaller or equal to that of I.
///
/// If the size of the input slice is not a multiple of the size of O, this
/// method panics at runtime.
pub fn cast_slice_mut<I: NoatunStorable, O: NoatunStorable>(s: &mut [I]) -> &mut [O] {
    const {
        assert!(align_of::<O>() <= align_of::<I>());
    }
    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    // Safety: 's' is valid
    unsafe { slice::from_raw_parts_mut(s.as_mut_ptr() as *mut O, count_o) }
}

/// Cast from a slice of I to a slice of O.
///
/// A compile time panic occurs if the alignment of O is not smaller or equal to that of I.
///
/// If the size of the input slice is not a multiple of the size of O, this
/// method panics at runtime.
pub fn cast_slice<I: NoatunStorable, O: NoatunStorable>(s: &[I]) -> &[O] {
    const {
        assert!(align_of::<O>() <= align_of::<I>());
    }
    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    // Safety: 's' is valid
    unsafe { slice::from_raw_parts(s.as_ptr() as *const O, count_o) }
}

/// Requires alignment to be correct at runtime, panics otherwise
pub fn dyn_cast_slice<I: NoatunStorable, O: NoatunStorable>(s: &[I]) -> &[O] {
    assert!((s.as_ptr() as *mut O).is_aligned());

    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    // Safety: 's' is valid
    unsafe { slice::from_raw_parts(s.as_ptr() as *const O, count_o) }
}
/// Requires alignment to be correct at runtime, panics otherwise
pub fn dyn_cast_slice_mut<I: NoatunStorable, O: NoatunStorable>(s: &mut [I]) -> &mut [O] {
    assert!((s.as_ptr() as *mut O).is_aligned());
    let tot_size_i = size_of_val(s);
    let count_o = tot_size_i / size_of::<O>();
    assert_eq!(tot_size_i, size_of::<O>() * count_o);
    // Safety: 's' is valid
    unsafe { slice::from_raw_parts_mut(s.as_mut_ptr() as *mut O, count_o) }
}

/// Read a value of type T from the bytes of 'data'.
///
/// 'data' must be the size of T.
pub fn read_unaligned<T>(data: &[u8]) -> T {
    if data.len() != size_of::<T>() {
        panic!("Slice is not the correct size");
    }
    let raw = data as *const [u8] as *const T;
    // Safety: 'data' is valid
    unsafe { raw.read_unaligned() }
}

/// # Safety
/// To implement this safely:
///  * Self must be repr(C) or repr(transparent). repr(packed) may also work.
///  * Self must not utilize any niches in owned objects. This is because the undo-function
///    of noatun will overwrite such niches during undo.
///  * Self can have niches, but noatun guarantees they will never be used and Self is
///    allowed to clobber them.
///  * All fields of Self must implement NoatunStorable
///  * Self must not be Unpin.
///  * Any bit pattern must be valid
///  * An all-zero bit pattern must represent a reasonable "default" value.
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
///    an Object-type. This must be made impossible to obtain.
///
/// Therefore, any type that implements Object must make sure that safe
/// code cannot obtain an owned instance of Self:
///   * No Default-impl!
///   * No new() impl (though NativeType can of course have new())
///
/// # On default values
/// Objects should ideally not implement [`Default`]. Noatun assumes all objects
/// can be constructed in a reasonable default state by simply initializing them
/// with an all zero bit pattern. To avoid confusion, types should therefore not
/// implement `Default`.
///
/// # Contrast to NoatunPod
/// This trait represents an object that can be stored on its own as a first class
/// noatun database object. However, noatun has a distinction between objects and values.
/// At the top level, the root object of a noatun database must be an Object. However,
/// the [`NoatunCell`] type allows storing values [implementing [`NoatunPod`]]. This allows
/// storing rust standard primitives like u8, u16, u32 etc directly in the database. It also
/// allows storing custom types, for example newtype wrappers, identifiers etc directly in
/// the database. All other information should be wrapped in a type that implements Object
/// (such as [`data_types::NoatunCell`]).
///
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a noatun Object, and can't appear on its own in a noatun database.",
    label = "`{Self}` does not implement Object.",
    note = "For primitives, try wrapping in NoatunCell.",
    note = "For structs, use `noatun_object!`-macro.",
    note = "For collections, try `NoatunHashMap` or `NoatunVec`.",
    note = "Manually implementing this trait is not recommended, but can be possible. See trait docs."
)]
pub trait Object {
    /// This is meant to be either ThinPtr for sized objects, or
    /// FatPtr for dynamically sized objects. Other types are unlikely to make sense.
    type Ptr: Pointer;

    /// A type that is functionally equivalent to `Self`, but which can be moved out
    /// of the database and used like a plain old data type.
    ///
    /// ```
    /// use noatun::{noatun_object, Object};
    ///
    /// noatun_object!(
    ///     struct Cat {
    ///         pod weight_g: u32
    ///     }
    /// );
    ///
    /// fn export(cat: &Cat) -> CatNative {
    ///     cat.export()
    /// }
    ///
    ///
    /// ```
    ///
    ///
    /// # Technical note:
    /// Instances of `Object` may never be moved out of the database. There are several
    /// reasons for this:
    ///
    ///  - 1: Objects stored in the database may reference memory mapped storage that
    ///    is only available when the database is open. These references are not
    ///    tracked using rust lifetimes, so if the object was moved out, there might
    ///    be a segfault (or worse) on subsequent access.
    ///  - 2: The undo-logic would have to be special-cased so as not to emit undo entries
    ///    for instances that are not in the database.
    ///  - 3: The undo-machinery is on a lower level than the objects, and undo could thus
    ///    affect objects referenced by moved-out instances, with unintended results.
    type NativeType: ?Sized;
    /// Owned version of [`Self::NativeType`].
    ///
    /// If Self is `str`, `NativeOwnedType` should be String, for example.
    type NativeOwnedType: Borrow<Self::NativeType>;

    /// Create an owned 'external' copy of this object.
    ///
    /// Detached types are regular rust objects, not stored in the noatun database.
    /// For example, the external type for [`data_types::NoatunString`] is [`std::primitive::str`]
    fn export(&self) -> Self::NativeOwnedType;

    /// Clear this object.
    ///
    ///
    /// This is called when an object disappears because it is no longer
    /// observable. For example, this happens when an element is deleted from a collection
    /// like [`data_types::NoatunVec`] or [`data_types::NoatunHashMap`] .
    ///
    /// From a correctness perspective, this method doesn't need to do anything.
    /// Since the value will not be observable again, its contents don't matter.
    ///
    /// However, any observables should be overwritten, so that messages that
    /// wrote them can be pruned.
    ///
    /// Noatun makes no assumptions about the contents of the memory after this
    /// method has finished. For collections that can reuse memory, the memory
    /// will be cleared at the latest before any such reuse.
    fn destroy(self: Pin<&mut Self>);

    /// Initialize all the fields in 'self' from the given 'external' type.
    /// The external type is a regular rust pod struct, with no requirements
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
    /// value had a tracker, it is overwritten.
    ///
    /// Implementations of this method should not panic. Doing so doesn't lead to
    /// unsoundness, but may leave self in an undesirable half-initialized state.
    ///
    /// It's explicitly allowed to use this method when `Self` has already been
    /// initialized. It must be overwritten, with any behavior that arises from that.
    /// Specifically, a new value must not simply be moved over, replacing the old value.
    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType);

    /// This can in most cases be:
    /// ```dontrun
    ///    let ret: &mut Self = NoatunContext.allocate_storable();
    ///    ret.init_from(external);
    ///    ret
    /// ```
    /// The only cases where some other implementation is required is when 'Self' does
    /// not have a fixed size.
    ///
    /// # Safety
    /// The returned reference lifetime must be tied to the lifetime of the current root object,
    /// before being returned to safe code.
    unsafe fn allocate_from<'a>(external: &Self::NativeType) -> Pin<&'a mut Self>;

    /// Write a unique value to 'hasher', that uniquely identifies this data type.
    ///
    /// This is used to detect schema conflicts between persisted materialized views
    /// and in-memory representations.
    ///
    /// The following guidelines are provided:
    ///  * Structs should write their full name, including crate name, their field count, and the
    ///    hash of each field.
    ///  * Newtypes should write their full name, including crate name, and the hash of their
    ///    inner type (unless the inner type is a primitive), and a '/' followed by a version number.
    ///    The version number must be changed if memory format changes.
    ///  * Primitives write their name prefixed by "std::" (this is implemented by Noatun).
    ///  * Collections, or more advanced types made specifically for Noatun
    ///    are encouraged to write their full name, plus a '/' followed by version
    ///    number, and if they are generic, the number of generic parameters and the hashes
    ///    of each generic.
    ///
    /// Note, `NoatunStorable` also has a similar method. Types that implement both NoatunStorable
    /// and Object can use the same implementation.
    fn hash_object_schema(hasher: &mut SchemaHasher);
}

// TODO(future): Could we support a noatun_enum! macro?

/// Count the number of token trees given as parameter
///
/// This is used by the [`noatun_object`]-macro to figure out the number
/// of fields in a struct.
#[macro_export]
macro_rules! count_ast_nodes {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + $crate::count_ast_nodes!($($xs)*));
}

/// Define a noatun pod struct.
///
/// Usage:
///
/// ```
/// use noatun::noatun_pod;
///
/// noatun_pod!{
///    struct MyPod {
///        x: u32,
///        y: u16,
///    }
/// }
///
/// ```
/// or
/// ```
/// use noatun::noatun_pod;
///
/// noatun_pod!{
///    struct MyTuplePod(u32,u16)
/// }
///
/// ```
#[macro_export]
macro_rules! noatun_pod {
    (   $(#[derive($($trait_name: ident),*)])*
        $(#[doc = $doc:expr])*
        struct $name: ident {
            $(
                $(#[doc = $field_doc:expr])*
                $field_name:ident : $field_type: ty
            ),*$(,)?
    }$(;)*) => {
        $(#[derive($($trait_name),*)])*
        #[derive(Clone, Copy, Debug, $crate::Savefile)]
        $(#[doc = $doc])*
        #[repr(C)]
        pub struct $name {
            $(
                $(#[doc = $field_doc])*
                pub $field_name: $field_type,
            )*
        }
        unsafe impl $crate::NoatunStorable for $name where
            $($field_type: $crate::NoatunStorable,)*
        {
            fn hash_schema(hasher: &mut $crate::SchemaHasher) {
                hasher.write_str(std::any::type_name::<Self>());
                hasher.write_usize($crate::count_ast_nodes!($($field_type)*));
                $(
                    hasher.write_str(stringify!($field_name));
                    <$field_type as $crate::NoatunStorable>::hash_schema(hasher);
                )*
            }
        }
        unsafe impl $crate::NoatunPod for $name where
            $($field_type: $crate::NoatunPod,)*
        {
        }
    };
    (   $(#[derive($($trait_name: ident),*)])*
        $(#[doc = $doc:expr])*
        struct $name: ident(
            $(
                $(#[doc = $field_doc:expr])*
                $field_type: ty
            ),* $(,)?
    )$(;)?) => {
        $(#[derive($($trait_name),*)])*
        #[derive(Clone, Copy, Debug, $crate::Savefile)]
        $(#[doc = $doc])*
        #[repr(C)]
        pub struct $name (
            $(
                $(#[doc = $field_doc])*
                pub $field_type,
            )*
        );
        unsafe impl $crate::NoatunStorable for $name where
            $($field_type: $crate::NoatunStorable,)*
        {
            fn hash_schema(hasher: &mut $crate::SchemaHasher) {
                hasher.write_str(std::any::type_name::<Self>());
                hasher.write_usize($crate::count_ast_nodes!($($field_type)*));
                $(
                    <$field_type as $crate::NoatunStorable>::hash_schema(hasher);
                )*
            }
        }
        unsafe impl $crate::NoatunPod for $name where
            $($field_type: $crate::NoatunPod,)*
        {
        }
    };
}

/// Define a noatun object.
///
/// Example:
/// ```rust
/// use noatun::noatun_object;
/// use noatun::data_types::{NoatunString, NoatunVec};
///
/// noatun_object!(
///     #[derive(PartialEq)]
///     struct Employee {
///         object name: NoatunString,
///         pod salary: u32
///     }
/// );
///
/// noatun_object!(
///     struct ExampleDb {
///         pod total_salary_cost: u32,
///         object employees: NoatunVec<Employee>,
///     }
/// );
/// ```
///
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
        #[doc = "Get the value for field"]
        #[doc = stringify!($name)]
        #[allow(unused)]
        pub fn $name(&self) -> $typ {
            self.$name.get()
        }
    };
    ( setter pod $name:ident $typ: ty  ) => {
        $crate::paste!(
            #[doc = "Set a new value for field"]
            #[doc = stringify!($name)]
            #[allow(unused)]
            pub fn [<set_ $name>](self: ::std::pin::Pin<&mut Self>, val: $typ) {
                unsafe { ::std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().$name).set(val); }
            }
        );
    };


    ( bounded_type opod $typ:ty) => {
        $crate::OpaqueNoatunCell<$typ>
    };
    ( declare_field opod $typ: ty ) => {
        $crate::OpaqueNoatunCell<$typ>
    };
    ( declare_detached_field opod $typ: ty ) => {
        $typ
    };
    ( new_declare_param opod $typ: ty ) => {
        $typ
    };
    ( new_assign_field opod $self: ident $name: ident $typ: ty ) => {
        unsafe { ::std::pin::Pin::new_unchecked(&mut $self.$name).set($name); }
    };
    ( getter opod $name:ident $typ: ty  ) => {

    };
    ( setter opod $name:ident $typ: ty  ) => {
        $crate::paste!(
            #[doc = "Set a new value for field"]
            #[doc = stringify!($name)]
            #[allow(unused)]
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
        <$typ as $crate::Object>::NativeOwnedType
    };
    ( new_declare_param object $typ: ty ) => {
        &<$typ as $crate::Object>::NativeType
    };
    ( new_assign_field object $self:ident $name: ident $typ: ty ) => {
        unsafe { <_ as $crate::Object>::init_from(::std::pin::Pin::new_unchecked(&mut $self.$name), $name); }
    };
    ( getter object $name:ident $typ: ty  ) => {
        #[doc ="Get the value of field"]
        #[doc=stringify!($name)]
        #[allow(unused)]
        pub fn $name(&self) -> &$typ {
            &self.$name
        }
    };
    ( setter object $name:ident $typ: ty  ) => {
        $crate::paste!(
            #[doc ="Set the value of field"]
            #[doc=stringify!($name)]
            #[allow(unused)]
            pub fn [<$name _mut>](self: ::std::pin::Pin<&mut Self>) -> ::std::pin::Pin<&mut $typ> {
                let tself = unsafe { self.get_unchecked_mut() };
                unsafe { ::std::pin::Pin::new_unchecked(&mut tself.$name) }
            }
        );
    };

    ( detached_type $n_detached: ident) => {
        $n_detached
    };

    (    $(#[derive( $($derive_item:ident),*  )])?
         $(#[doc = $doc:expr])*
         struct $n:ident { $( $(#[doc = $field_doc:expr])* $kind:ident $name: ident : $typ:ty $(,)* )* } $(;)* ) => {

            // We use this little trick instead of putting trait bounds on the struct.
            // The reason is that this makes the type signature cleaner, and doesn't give
            // as unwieldy type docs in IDE:s.
            const _:() = {
                const fn __items_in_noatun_db_must_impl_noatun_object_trait() where $( noatun_object!(bounded_type $kind $typ) : $crate::Object ),* {}
                static __ITEMS_IN_NOATUN_DB_MUST_IMPL_NOATUN_OBJECT_TRAIT_CHECKER: () = __items_in_noatun_db_must_impl_noatun_object_trait();
            };

            #[derive(Debug)]
            #[repr(C)]
            $(#[doc = $doc])*
            pub struct $n
            {
                phantom: ::std::marker::PhantomPinned,
                $(
                    $(#[doc = $field_doc])*
                    #[allow(unused)]
                    pub $name : noatun_object!(declare_field $kind $typ)
                ),*
            }
            unsafe impl $crate::NoatunStorable for $n {
                fn hash_schema(hasher: &mut $crate::SchemaHasher) {
                    // type names are not guaranteed to be unique, but that doesn't matter,
                    // since the name check is just an extra bonus. If all fields match, but
                    // type differs, everything should still "work", though of course
                    // there might be surprises. "Real" renames of a type will tend to change
                    // the `std::any::type_name`.
                    hasher.write_str(std::any::type_name::<Self>());
                    hasher.write_usize($crate::count_ast_nodes!($($name)*));
                    $(
                        <$typ as $crate::NoatunStorable>::hash_schema(hasher);
                    )*
                }

            }
            $crate::paste!(
                #[doc ="pin_project helper for"]
                #[doc = stringify!($n)]
                #[derive(Debug)]
                pub struct [<$n PinProject>]<'a> {
                    $(
                        $(#[doc = $field_doc])*
                        #[allow(unused)]
                        pub $name: ::std::pin::Pin<&'a mut noatun_object!(declare_field $kind $typ)>
                    ),*
                }
            );


            impl $n {

                #[doc = "Initialize an instance of"]
                #[doc = stringify!($n)]
                #[doc = "with an explicit value for each field"]
                #[allow(unused)]
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
                    #[doc ="Give pinned access to each field in"]
                    #[doc=stringify!($n)]
                    #[allow(unused)]
                    pub fn pin_project(self: ::std::pin::Pin<&mut Self>) -> [<$n PinProject>]<'_> {
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
                $(#[derive($($derive_item,)*)])*
                #[derive(Debug,Clone,$crate::Savefile)]
                $(#[doc = $doc])*
                #[doc = ""]
                #[doc = "This is a external version of"]
                #[doc = stringify!($n)]
                pub struct [<$n Native>]
                {
                    $(
                        $(#[doc = $field_doc])*
                        #[allow(unused)]
                        $name : $crate::noatun_object!(declare_detached_field $kind $typ)
                    ),*
                }
            }


            impl $crate::Object for $n {
                type Ptr = $crate::ThinPtr;
                type NativeType = $crate::paste!(noatun_object!(detached_type [<$n Native>]));
                type NativeOwnedType = $crate::paste!(noatun_object!(detached_type [<$n Native>]));


                fn destroy(self: ::std::pin::Pin<&mut Self>) {
                    let tself = unsafe { self.get_unchecked_mut() };
                    $( unsafe { ::std::pin::Pin::new_unchecked(&mut tself.$name).destroy(); } )*
                }

                fn export(&self) -> Self::NativeOwnedType {
                    Self::NativeOwnedType {
                        $(
                            $name: self.$name.export()
                        ),*
                    }
                }

                fn init_from(mut self: ::std::pin::Pin<&mut Self>, external: &Self::NativeType) {
                    $(
                    unsafe {
                        ::std::pin::Pin::new_unchecked(&mut self.as_mut().get_unchecked_mut().$name).init_from(&external.$name);

                    }
                    )*
                }

                unsafe fn allocate_from<'a>(external: &Self::NativeType) -> ::std::pin::Pin<&'a mut Self> {
                    let mut ret: ::std::pin::Pin<&mut Self> = $crate::NoatunContext.allocate();
                    ret.as_mut().init_from(external);
                    ret
                }

                fn hash_object_schema(hasher: &mut $crate::SchemaHasher) {
                    <Self as $crate::NoatunStorable>::hash_schema(hasher);
                }

            }


    };

}

/// Trait for implementations of `Object` that have a fixed size, and have an internal
/// representation that is bitwise compatible with their Database format.
///
/// Most noatun Object types implement this. There is a blanket implementation for
/// all types T that have a fixed size and implement `NoatunStorable`.
pub trait FixedSizeObject: Object<Ptr = ThinPtr> + NoatunStorable + Sized + 'static
where
    <Self as Object>::NativeOwnedType: Sized,
{
}

impl<T: Object<Ptr = ThinPtr> + NoatunStorable + 'static> FixedSizeObject for T {}

#[derive(Debug)]
#[repr(C)]
struct RawFatPtr {
    data: *const u8,
    size: usize,
}

/// A fat pointer to noatun materialized view storage.
///
/// Contains a start and a count.
///
/// Note, in contrast to regular rust fat pointers, the second field really must be a count,
/// and can not be a vtable.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct FatPtr {
    /// Start offset in file
    start: usize,
    /// Second part of fat pointer, typically item count of tail
    ///
    /// For a u8 ptr, this is the size of the object in bytes.
    count: usize,
}
impl FatPtr {
    /// Start index, and size in bytes
    pub fn from_idx_count(start: usize, count: usize) -> FatPtr {
        FatPtr { start, count }
    }

    /// Gives a fatptr to the `size_bytes` bytes starting at `thin`,
    pub fn from_thin_size(thin: ThinPtr, size_bytes: usize) -> FatPtr {
        FatPtr {
            start: thin.0,
            count: size_bytes,
        }
    }
}

/// An offset into the on disk store.
///
/// The numeric value is the offset in bytes, from the start of the file on disk.
#[derive(Copy, Clone, Debug)]
pub struct ThinPtr(pub usize);

// Safety: All fields of ThinPtr are NoatunStorable
unsafe impl NoatunStorable for ThinPtr {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::ThinPtr/1")
    }
}
// Safety: All fields of FatPtr are NoatunStorable
unsafe impl NoatunStorable for FatPtr {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::FatPtr/1")
    }
}

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
    /// Return a null pointer
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

        // Safety: Layout is identical
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
    T::NativeType: Sized,
{
    type Ptr = FatPtr;
    type NativeType = [T::NativeOwnedType];
    type NativeOwnedType = Vec<T::NativeOwnedType>;

    fn export(&self) -> Self::NativeOwnedType {
        self.iter().map(|x| x.export()).collect()
    }

    fn destroy(self: Pin<&mut Self>) {
        // Safety: We don't move out of tself
        let tself = unsafe { self.get_unchecked_mut() };
        for item in tself {
            // Safety: item is a valid object
            unsafe { Pin::new_unchecked(item).destroy() };
        }
    }

    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType) {
        // Safety: 'dst' is safely pinnable (since all objects in Noatun materialized view
        // are pinned. We don't move out of 'self'.
        unsafe {
            for (dst, src) in self.get_unchecked_mut().iter_mut().zip(external.iter()) {
                Pin::new_unchecked(dst).init_from(src.borrow());
            }
        }
    }

    unsafe fn allocate_from<'a>(external: &Self::NativeType) -> Pin<&'a mut Self> {
        let bytes = size_of::<T>() * external.len();
        let alloc = NoatunContext.allocate_raw(bytes, align_of::<T>());

        // Safety: alloc is valid, we just allocated it
        let slice: &mut [T] = unsafe { slice::from_raw_parts_mut(alloc as *mut T, external.len()) };
        for (src, dst) in external.iter().zip(&mut *slice) {
            Pin::new_unchecked(dst).init_from(src.borrow());
        }
        Pin::new_unchecked(slice)
    }

    fn hash_object_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::[T]/1");
        <T as NoatunStorable>::hash_schema(hasher);
    }
}

/// Strategy for handling any pre-existing database files when opening a database
#[derive(Clone)]
pub enum Target {
    /// Open an existing file, but fail if no file exists.
    OpenExisting(PathBuf),
    /// If file does not exist, create it.
    ///
    /// If a file does exist, overwrite it.
    ///
    /// This does never make use of any stored data.
    CreateNewOrOverwrite(PathBuf),
    /// If file does not exist, create it.
    ///
    /// Otherwise, use the stored file.
    CreateNew(PathBuf),
}
impl Target {
    /// The path to the selected file
    pub fn path(&self) -> &Path {
        let (Target::CreateNew(x) | Target::CreateNewOrOverwrite(x) | Target::OpenExisting(x)) =
            self;
        x
    }
    /// True if the file should be created if it doesn't exist
    pub fn create(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_) | Target::CreateNew(_))
    }
    /// True if any existing file should be unconditionally overwritten
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
    fn new(context: &mut DatabaseContextData, is_message_apply: bool) -> ContextGuardMut {
        if !CONTEXT.get().is_null() {
            panic!(
                "'with_root' must not be called within an existing database access context.
                         For example, it cannot be called within a 'with_root', 'with_root_mut' or
                         message apply operation.
                "
            );
        }
        context.is_mutable = true;
        context.is_message_apply = is_message_apply;
        CONTEXT.set(context as *mut _);
        ContextGuardMut
    }
}

impl Drop for ContextGuardMut {
    fn drop(&mut self) {
        // Safety: CONTEXT is valid, since otherwise we wouldn't have a guard
        unsafe {
            (*CONTEXT.get()).is_mutable = false;
            (*CONTEXT.get()).is_message_apply = false;
        }
        CONTEXT.set(null_mut());
    }
}

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

/// Trait for a serializer that can serialize/deserialize a Message of type `MSG`.
///
/// This trait is used to make noatun completely independent of any particular serialization
/// framework, like serde. To use serde + postcard, use `PostcardMessageSerializer`.
pub trait NoatunMessageSerializer<MSG> {
    /// Deserialize an instance of type `MSG` from 'buf'
    fn deserialize(buf: &[u8]) -> anyhow::Result<MSG>
    where
        Self: Sized;

    /// Serialize 'MSG', writing it to 'writer'.
    fn serialize<W: Write>(msg: &MSG, writer: W) -> anyhow::Result<()>;
}

/// Serializer strategy that uses the serde-based "postcard" serializer
#[cfg(feature = "postcard")]
pub struct PostcardMessageSerializer<MSG: Serialize + DeserializeOwned>(pub PhantomData<MSG>);

#[cfg(feature = "postcard")]
impl<MSG: Serialize + DeserializeOwned> NoatunMessageSerializer<MSG>
    for PostcardMessageSerializer<MSG>
{
    fn deserialize(buf: &[u8]) -> anyhow::Result<MSG>
    where
        Self: Sized,
    {
        Ok(postcard::from_bytes(buf)?)
    }

    fn serialize<W: Write>(msg: &MSG, writer: W) -> anyhow::Result<()> {
        postcard::to_io(msg, writer)?;
        Ok(())
    }
}

/// A noatun serializer using the savefile library. See [`savefile `].
pub struct SavefileMessageSerializer<
    MSG: savefile::Serialize + savefile::Deserialize + savefile::Packed,
>(pub PhantomData<MSG>);

/// Dummy serializer that panics on all use
///
/// Only useful for compile tests or similar, where actual serialization isn't needed, but
/// a type implementing `NoatunMessageSerializer<T>` is needed
pub struct DummyMessageSerializer;

impl<MSG> NoatunMessageSerializer<MSG> for DummyMessageSerializer {
    fn deserialize(_buf: &[u8]) -> Result<MSG>
    where
        Self: Sized,
    {
        panic!("attempt to deserialize message using dummy serializer")
    }

    fn serialize<W: Write>(_msg: &MSG, _writer: W) -> Result<()> {
        panic!("attempt to serialize message using dummy serializer")
    }
}

impl<MSG: savefile::Serialize + savefile::Deserialize + savefile::Packed>
    NoatunMessageSerializer<MSG> for SavefileMessageSerializer<MSG>
{
    fn deserialize(buf: &[u8]) -> Result<MSG>
    where
        Self: Sized,
    {
        msg_deserialize(buf)
    }

    fn serialize<W: Write>(msg: &MSG, writer: W) -> Result<()> {
        msg_serialize(msg, writer)
    }
}

#[cfg(test)]
mod tests;
