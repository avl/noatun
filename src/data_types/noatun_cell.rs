use crate::sequence_nr::{SequenceNr, Tracker};
use crate::{NoatunContext, NoatunStorable, Object, SchemaHasher, ThinPtr, CONTEXT};
use std::fmt::{Debug, Formatter};
use std::ops::{AddAssign, Deref, SubAssign};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use tracing::trace;

/// A wrapper around a regular plain old data type. T must implement
/// `NoatunStorable`, which basically means it must be a Copy type that does
/// not contain any pointers.
#[repr(C)]
pub struct NoatunCell<T> {
    value: T,
    /// The most recent message that did a write to this cell
    registrar: Tracker,
}

#[repr(C)]
pub struct OpaqueNoatunCell<T> {
    value: T,
    /// The most recent message that did a write to this cell
    registrar: Tracker,
}

unsafe impl<T: NoatunStorable> NoatunStorable for OpaqueNoatunCell<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::OpaqueNoatunCell/1");
        T::hash_schema(hasher);
    }
}

unsafe impl<T: NoatunStorable> NoatunStorable for NoatunCell<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunCell/1");
        T::hash_schema(hasher);
    }
}

impl<T: Copy + Debug> Debug for NoatunCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.value;
        value.fmt(f)
    }
}

impl<T: Copy + Debug> Debug for OpaqueNoatunCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.value;
        value.fmt(f)
    }
}

pub trait NoatunCellArrayExt<T: NoatunStorable> {
    fn observe(&self) -> Vec<T>;
}
impl<T: NoatunStorable + Copy> NoatunCellArrayExt<T> for &[NoatunCell<T>] {
    fn observe(&self) -> Vec<T> {
        self.iter().map(|x| x.get()).collect()
    }
}

impl<T> PartialEq<NoatunCell<T>> for NoatunCell<T>
where
    T: Copy + PartialEq,
{
    fn eq(&self, other: &NoatunCell<T>) -> bool {
        let val1 = self.value;
        let val2 = other.value;
        val1 == val2
    }
}

impl<T: Copy> Deref for NoatunCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        NoatunContext.observe_registrar(self.registrar);
        &self.value
    }
}

impl<T: NoatunStorable> NoatunCell<T> {
    pub fn get(&self) -> T
    where
        T: Copy,
    {
        NoatunContext.observe_registrar(self.registrar);
        self.value
    }
    pub fn set(self: Pin<&mut Self>, new_value: T) {
        let cptr = CONTEXT.get();
        let c = unsafe { &mut *cptr };
        let tself = unsafe { self.get_unchecked_mut() };
        c.assert_mutable();
        unsafe { c.write_storable_ptr(new_value, addr_of_mut!(tself.value)) }
        c.update_registrar_ptr(addr_of_mut!(tself.registrar), false);
    }
}

impl<T: NoatunStorable> OpaqueNoatunCell<T> {
    /// WARNING!
    /// This must only be called from queries, not from message apply.
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

    pub fn set(self: Pin<&mut Self>, new_value: T) {
        let cptr = CONTEXT.get();
        let c = unsafe { &mut *cptr };
        let tself = unsafe { self.get_unchecked_mut() };
        c.assert_mutable();
        unsafe {
            c.write_storable_ptr(new_value, addr_of_mut!(tself.value));
        }
        c.update_registrar_ptr(addr_of_mut!(tself.registrar), true);
    }
}

impl<T: NoatunStorable + AddAssign<T> + Copy> AddAssign<T> for Pin<&mut OpaqueNoatunCell<T>> {
    fn add_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.registrar);
        let mut new_value = self.value;
        new_value += rhs;
        self.as_mut().set(new_value);
    }
}
impl<T: NoatunStorable + SubAssign<T> + Copy> SubAssign<T> for Pin<&mut OpaqueNoatunCell<T>> {
    fn sub_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.registrar);
        let mut new_value = self.value;
        new_value -= rhs;
        self.as_mut().set(new_value);
    }
}

impl<T: NoatunStorable + AddAssign<T> + Copy> AddAssign<T> for Pin<&mut NoatunCell<T>> {
    fn add_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.registrar);
        let mut new_value = self.value;
        new_value += rhs;
        self.as_mut().set(new_value);
    }
}
impl<T: NoatunStorable + SubAssign<T> + Copy> SubAssign<T> for Pin<&mut NoatunCell<T>> {
    fn sub_assign(&mut self, rhs: T) {
        NoatunContext.observe_registrar(self.registrar);
        let mut new_value = self.value;
        new_value -= rhs;
        self.as_mut().set(new_value);
    }
}


impl<T: NoatunStorable + Debug + Copy> Object for NoatunCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;
    type DetachedOwnedType = T;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.get()
    }

    fn destroy(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        trace!("NoatunCell::clear: {:?}", tself.value);
        NoatunContext.clear_registrar_ptr(&mut tself.registrar, false);
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        self.set(*detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate();
        ret.as_mut().init_from_detached(detached);
        ret
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        Self::hash_schema(hasher);
    }
}

impl<T: NoatunStorable + Debug + Copy> Object for OpaqueNoatunCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;
    type DetachedOwnedType = T;

    fn detach(&self) -> Self::DetachedOwnedType {
        NoatunContext.assert_opaque_access_allowed("OpaqueNoatunCell", "NoatunCell");
        self.value
    }

    fn destroy(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        trace!("OpaqueNoatunCell::clear: {:?}", tself.value);
        NoatunContext.clear_registrar_ptr(&mut tself.registrar, true);
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        self.set(*detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate();
        ret.as_mut().init_from_detached(detached);
        ret
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        Self::hash_schema(hasher);
    }
}
