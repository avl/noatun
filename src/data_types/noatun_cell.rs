use crate::sequence_nr::{Tracker};
use crate::{NoatunContext, NoatunPod, NoatunStorable, Object, SchemaHasher, ThinPtr, CONTEXT};
use std::fmt::{Debug, Formatter};
use std::ops::{AddAssign, Deref, SubAssign};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use tracing::trace;

/// A wrapper around a regular plain old data type. T must implement
/// `NoatunStorable`, which basically means it must be a Copy type that does
/// not contain any pointers.
#[repr(C)]
pub struct NoatunCell<T: NoatunPod> {
    value: T,
    /// The most recent message that did a write to this cell
    tracker: Tracker,
}

/// An opaque noatun cell. This is a wrapper around a regular data type.
///
/// This type does not allow reading the inner value while applying messages to the
/// materialized view. In return, messages that wrote an opaque value, can be pruned as
/// soon as the opaque value is overwritten.
///
/// T must implement `NoatunStorable`, which basically means it must be a Copy type that does
/// not contain any pointers.
#[repr(C)]
pub struct OpaqueNoatunCell<T> {
    value: T,
    /// The most recent message that did a write to this cell
    tracker: Tracker,
}


/// Safety: OpaqueNoatunCell<T> contains only NoatunStorable types
unsafe impl<T: NoatunStorable> NoatunStorable for OpaqueNoatunCell<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::OpaqueNoatunCell/1");
        T::hash_schema(hasher);
    }
}

/// Safety: NoatunCell<T> contains only NoatunStorable types
unsafe impl<T: NoatunPod> NoatunStorable for NoatunCell<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunCell/1");
        T::hash_schema(hasher);
    }
}

/// Safety: NoatunCell<T> contains only NoatunPod types
impl<T: NoatunPod + Debug> Debug for NoatunCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.value;
        value.fmt(f)
    }
}

/// Safety: OpaqueNoatunCell<T> contains only NoatunPod types
impl<T: NoatunPod + Debug> Debug for OpaqueNoatunCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.value;
        value.fmt(f)
    }
}

/// Extension trait to export the contents of a slice of NoatunCell items.
pub trait NoatunCellArrayExt<T: NoatunStorable> {
    /// Retrieve the contents of every cell, and return in a Vec
    fn observe(&self) -> Vec<T>;
}
impl<T: NoatunPod> NoatunCellArrayExt<T> for &[NoatunCell<T>] {
    fn observe(&self) -> Vec<T> {
        self.iter().map(|x| x.get()).collect()
    }
}

impl<T> PartialEq<NoatunCell<T>> for NoatunCell<T>
where
    T: NoatunPod + PartialEq,
{
    fn eq(&self, other: &NoatunCell<T>) -> bool {
        let val1 = self.value;
        let val2 = other.value;
        val1 == val2
    }
}

impl<T: NoatunPod> Deref for NoatunCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        NoatunContext.observe_registrar(self.tracker);
        &self.value
    }
}

impl<T: NoatunPod> NoatunCell<T> {
    /// Get the current value of the cell.
    /// 
    /// This records a read dependency on the data. 
    pub fn get(&self) -> T
    where
        T: Copy,
    {
        NoatunContext.observe_registrar(self.tracker);
        self.value
    }

    /// Set the value of this cell
    pub fn set(self: Pin<&mut Self>, new_value: T) {
        let cptr = CONTEXT.get();
        // Safety: cptr is valid, aligned and unaliased (because we only put such pointers
        // into the thread local.
        let c = unsafe { &mut *cptr };
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        c.assert_mutable();
        // Safety: tself.value is a valid reference
        unsafe { c.write_storable_ptr(new_value, addr_of_mut!(tself.value)) }
        // Safety: `tself.tracker` is a valid object
        unsafe {
            c.update_tracker_ptr(addr_of_mut!(tself.tracker), false);
        }
    }
}

impl<T: NoatunStorable> OpaqueNoatunCell<T> {

    /// Get the value of this cell.
    ///
    /// WARNING!
    /// This must only be called from queries, not from message apply, or it will panic.
    ///
    /// I.e, you can call this from within `with_root`, but not from within
    /// `Message::apply`.
    pub fn query(&self) -> T
    where
        T: Copy,
    {
        NoatunContext.assert_opaque_access_allowed("OpaqueNoatunCell", "NoatunCell");
        self.value
    }

    /// Set the value of this cell
    pub fn set(self: Pin<&mut Self>, new_value: T) {
        let cptr = CONTEXT.get();
        // Safety: We only put valid pointers into the thread local
        let c = unsafe { &mut *cptr };
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        c.assert_mutable();
        // Safety: tself.value is a value reference
        unsafe {
            c.write_storable_ptr(new_value, addr_of_mut!(tself.value));
        }
        // Safety: tself.tracker is a valid object
        unsafe {
            c.update_tracker_ptr(addr_of_mut!(tself.tracker), true);
        }
    }
}

impl<T: NoatunStorable + AddAssign<T> + Copy> AddAssign<T> for Pin<&mut OpaqueNoatunCell<T>> {
    fn add_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.tracker);
        let mut new_value = self.value;
        new_value += rhs;
        self.as_mut().set(new_value);
    }
}
impl<T: NoatunStorable + SubAssign<T> + Copy> SubAssign<T> for Pin<&mut OpaqueNoatunCell<T>> {
    fn sub_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.tracker);
        let mut new_value = self.value;
        new_value -= rhs;
        self.as_mut().set(new_value);
    }
}

impl<T: NoatunPod + AddAssign<T> + Copy> AddAssign<T> for Pin<&mut NoatunCell<T>> {
    fn add_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.tracker);
        let mut new_value = self.value;
        new_value += rhs;
        self.as_mut().set(new_value);
    }
}
impl<T: NoatunPod + SubAssign<T> + Copy> SubAssign<T> for Pin<&mut NoatunCell<T>> {
    fn sub_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.tracker);
        let mut new_value = self.value;
        new_value -= rhs;
        self.as_mut().set(new_value);
    }
}


impl<T: NoatunPod + Debug> Object for NoatunCell<T> {
    type Ptr = ThinPtr;
    type NativeType = T;
    type NativeOwnedType = T;

    fn export(&self) -> Self::NativeOwnedType {
        self.get()
    }

    fn destroy(self: Pin<&mut Self>) {
        // Safety: We don't move out of the reference
        let tself = unsafe { self.get_unchecked_mut() };
        trace!("NoatunCell::clear: {:?}", tself.value);
        // Safety: tself.tracker is a valid object
        unsafe { NoatunContext.clear_tracker_ptr(&mut tself.tracker, false) };
    }

    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType) {
        self.set(*external);
    }

    unsafe fn allocate_from<'a>(external: &Self::NativeType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate();
        ret.as_mut().init_from(external);
        ret
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        Self::hash_schema(hasher);
    }
}

impl<T: NoatunStorable + Debug + Copy> Object for OpaqueNoatunCell<T> {
    type Ptr = ThinPtr;
    type NativeType = T;
    type NativeOwnedType = T;

    fn export(&self) -> Self::NativeOwnedType {
        NoatunContext.assert_opaque_access_allowed("OpaqueNoatunCell", "NoatunCell");
        self.value
    }

    fn destroy(self: Pin<&mut Self>) {
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        trace!("OpaqueNoatunCell::clear: {:?}", tself.value);
        // Safety: tself.tracker is a valid object
        unsafe { NoatunContext.clear_tracker_ptr(&mut tself.tracker, true) };
    }

    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType) {
        self.set(*external);
    }

    unsafe fn allocate_from<'a>(external: &Self::NativeType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate();
        ret.as_mut().init_from(external);
        ret
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        Self::hash_schema(hasher);
    }
}
