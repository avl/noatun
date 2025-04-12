use crate::sequence_nr::SequenceNr;
use crate::{
    DatabaseContextData, FatPtr, FixedSizeObject, NoatunContext, Object, Pointer, ThinPtr, CONTEXT,
};
use bytemuck::{AnyBitPattern, Pod, Zeroable};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Deref, Range};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::slice;
use tracing::trace;
use crate::xxh3_vendored::NoatunHasher;

#[derive(Copy, Clone, Debug, AnyBitPattern)]
#[repr(C)]
pub struct NoatunString {
    start: ThinPtr,
    length: usize,
    registrar: SequenceNr,
    padding: u32
}

impl Object for NoatunString {
    type Ptr = ThinPtr;
    type DetachedType = str;
    type DetachedOwnedType = String;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.get().to_string()
    }

    fn clear(self: Pin<&mut Self>) {
        NoatunContext.observe_registrar(SequenceNr::INVALID);
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        self.assign(detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut temp: Pin<&mut Self> = NoatunContext.allocate_pod();
        temp.as_mut().assign(detached);
        temp
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        NoatunContext.access_pod(index)
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        NoatunContext.access_object_mut(index)
    }
}

impl Deref for NoatunString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl NoatunString {
    pub fn get(&self) -> &str {
        NoatunContext.observe_registrar(self.registrar);

        if self.length == 0 {
            return "";
        }
        let start_ptr = NoatunContext.start_ptr().wrapping_add(self.start.0);
        unsafe {
            let bytes = slice::from_raw_parts(start_ptr, self.length);
            std::str::from_utf8_unchecked(bytes)
        }
    }
    pub fn assign(self: Pin<&mut Self>, value: &str) {
        let tself = unsafe { self.get_unchecked_mut() };
        if tself.get().starts_with(value) {
            if tself.length != value.len() {
                NoatunContext.write_pod_internal(value.len(), &mut tself.length);
            }
            return;
        }
        let raw = NoatunContext.allocate_raw(value.len(), 1);
        let target = unsafe { slice::from_raw_parts_mut(raw, value.len()) };
        target.copy_from_slice(value.as_bytes());
        let raw_index = NoatunContext.index_of_ptr(raw);
        NoatunContext.write_pod_internal(raw_index, &mut tself.start);
        NoatunContext.write_pod_internal(value.len(), &mut tself.length);
        NoatunContext.update_registrar_ptr(addr_of_mut!(tself.registrar));
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct DatabaseOption<T: Copy> {
    value: T,
    registrar: SequenceNr,
    present: u8,
}

impl<T: Copy + Pod> DatabaseOption<T> {
    pub fn set(&mut self, new_value: Option<T>) {
        let c = CONTEXT.get();
        if c.is_null() {
            if let Some(new_value) = new_value {
                self.value = new_value;
                self.present = 1;
            } else {
                self.present = 0;
            }
            return;
            //unreachable!("Attempt to modify DatabaseCell without a mutable context.");
        }
        let c = unsafe { &mut *c };
        if let Some(new_value) = new_value {
            NoatunContext.write_pod_ptr(new_value, std::ptr::addr_of_mut!(self.value));
            NoatunContext.write_pod(1, Pin::new(&mut self.present));
        } else {
            NoatunContext.write_pod(0, Pin::new(&mut self.present));
        }

        c.update_registrar_ptr(addr_of_mut!(self.registrar));
    }
    pub fn get(&self) -> Option<T> {
        NoatunContext.observe_registrar(self.registrar);
        if self.present != 0 {
            Some(self.value)
        } else {
            None
        }
    }
}

//TODO: This is unsound. Stop using bytemuck for these types in Noatun
unsafe impl<T> AnyBitPattern for DatabaseOption<T> where T: AnyBitPattern {}
unsafe impl<T: Copy> Zeroable for DatabaseOption<T> where T: Zeroable {}

// TODO: DatabaseCell should not be copy. .
// It's not a disaster, but it's a bit error-prone.
// Someone can copy it, then overwrite the copy.
// This will not affect the db, but may lead to an out-of-bounds write-report.
// registrar observe should work regardless.
#[derive(Copy, Clone, AnyBitPattern)]
#[repr(C)]
pub struct DatabaseCell<T: Copy> {
    value: T,
    registrar: SequenceNr,
}

impl<T: Copy + Debug> Debug for DatabaseCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.value;
        value.fmt(f)
    }
}

// TODO: Document. Also rename this or DatabaseCell, the names should harmonize.
// TODO: Does OpaqueCell even work? Can we delete messages if they only write OpaqueCell?
#[derive(Copy, Clone, AnyBitPattern)]
#[repr(C)]
pub struct OpaqueCell<T: Copy> {
    value: T,
    registrar: SequenceNr,
}

pub trait DatabaseCellArrayExt<T: Pod> {
    fn observe(&self) -> Vec<T>;
}
impl<T: Pod> DatabaseCellArrayExt<T> for &[DatabaseCell<T>] {
    fn observe(&self) -> Vec<T> {
        self.iter().map(|x| x.get()).collect()
    }
}

impl<T> PartialEq<DatabaseCell<T>> for DatabaseCell<T>
where
    T: Copy + PartialEq,
{
    fn eq(&self, other: &DatabaseCell<T>) -> bool {
        let val1 = self.value;
        let val2 = other.value;
        val1 == val2
    }
}

impl<T: Copy> Deref for DatabaseCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        NoatunContext.observe_registrar(self.registrar);
        &self.value
    }
}

impl<T: Pod> DatabaseCell<T> {
    pub fn get(&self) -> T {
        NoatunContext.observe_registrar(self.registrar);
        self.value
    }
    /*pub fn get_ref(&self, context: &DatabaseContextData) -> &T {
        context.observe_registrar(self.registrar);
        &self.value
    }*/
    pub fn set(self: Pin<&mut Self>, new_value: T) {
        let c = CONTEXT.get();
        /*if c.is_null() {
            let tself = unsafe { self.get_unchecked_mut() };
            tself.value = new_value;
            return;
        }*/
        let c = unsafe { &mut *c };
        let tself = unsafe { self.get_unchecked_mut() };
        //TODO: Use this consistently for all mutable access!
        c.assert_mutable();
        //let _index = c.index_of(tself);
        //context.write(index, bytes_of(&new_value));
        c.write_pod_ptr(new_value, addr_of_mut!(tself.value));
        c.update_registrar_ptr(addr_of_mut!(tself.registrar));
    }
    pub fn clear(self: Pin<&mut Self>) {
        let tself =  unsafe { self.get_unchecked_mut() };
        NoatunContext.clear_registrar_ptr(addr_of_mut!(tself.registrar));
    }
}
/*
impl<T: Pod> Object for OpaqueCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;
    type DetachedOwnedType = T;

    fn detach(&self) -> Self::DetachedOwnedType {
        // Aren't OpaqueCells actually completely useless?
        // We need to make it so their value can be inspected in 'with_root', but
        // not otherwise.
        panic!("OpaqueCell cannot be detached")
    }

    fn clear(self: Pin<&mut Self>) {

    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        self.set(*detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate_pod();
        ret.as_mut().init_from_detached(detached);
        ret
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_object_mut(index) }
    }
}

impl<T: Pod> OpaqueCell<T> {
    pub fn set(self: Pin<&mut Self>, new_value: T) {
        let tself = unsafe { self.get_unchecked_mut() };
        //let index = NoatunContext.index_of(tself);
        //context.write(index, bytes_of(&new_value));
        NoatunContext.write_pod(new_value, unsafe { Pin::new_unchecked(&mut tself.value) });
        NoatunContext.update_registrar(&mut tself.registrar);
    }
}
*/

impl<T: Pod> DatabaseCell<T> {
    /*#[allow(clippy::mut_from_ref)]
    pub fn allocate<'a>() -> &'a mut Self {
        let memory = unsafe { NoatunContext.allocate_pod::<DatabaseCell<T>>() };
        unsafe { &mut *(memory as *mut _) }
    }*/
    pub fn new(value: T) -> DatabaseCell<T> {
        DatabaseCell {
            value,
            registrar: Default::default(),
        }
    }
}


impl<T: Pod+Debug> Object for DatabaseCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;
    type DetachedOwnedType = T;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.value
    }

    fn clear(self: Pin<&mut Self>) {
        let tself =  unsafe { self.get_unchecked_mut() };
        trace!("DatabaseCell::clear: {:?}", tself.value);
        NoatunContext.clear_registrar_ptr(&mut tself.registrar);
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        self.set(*detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate_pod();
        ret.as_mut().init_from_detached(detached);
        ret
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_object_mut(index) }
    }
}

/// Like DatabaseVec, but for crate internal use. Does not track
/// accesses. (Does not track dependencies between messages).
#[repr(C)]
pub(crate) struct RawDatabaseVec<T> {
    length: usize,
    capacity: usize,
    data: usize,
    phantom_data: PhantomData<T>,
}
unsafe impl<T: 'static> Pod for RawDatabaseVec<T> {}

unsafe impl<T> Zeroable for RawDatabaseVec<T> {}

impl<T> Copy for RawDatabaseVec<T> {}

impl<T> Default for RawDatabaseVec<T> {
    fn default() -> Self {
        Self {
            length: 0,
            capacity: 0,
            data: 0,
            phantom_data: Default::default(),
        }
    }
}

impl<T> Clone for RawDatabaseVec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Debug for RawDatabaseVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawDatabaseVec({})", self.length)
    }
}

impl<T: Pod + 'static> RawDatabaseVec<T> {
    fn realloc_add(&mut self, ctx: &mut DatabaseContextData, new_capacity: usize, new_len: usize) {
        debug_assert!(new_capacity >= new_len);
        debug_assert!(new_capacity >= self.capacity);
        debug_assert!(new_len >= self.length);

        let dest = ctx.allocate_raw(new_capacity * size_of::<T>(), align_of::<T>());
        let dest_index = ctx.index_of_ptr(dest);

        if self.length > 0 {
            let old_ptr = FatPtr::from(self.data, size_of::<T>() * self.length);
            ctx.copy(old_ptr, dest_index);
        }

        ctx.write_pod(
            RawDatabaseVec {
                length: new_len,
                capacity: new_capacity,
                data: dest_index.0,
                phantom_data: Default::default(),
            },
            unsafe { Pin::new_unchecked(self) },
        )
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    pub(crate) fn grow(&mut self, ctx: &mut DatabaseContextData, new_length: usize) {
        if new_length <= self.length {
            return;
        }
        if self.capacity < new_length {
            self.realloc_add(ctx, 2 * new_length, new_length);
        } else {
            ctx.write_pod(new_length, unsafe { Pin::new_unchecked(&mut self.length) });
        }
    }
}
impl<T: Pod + 'static> RawDatabaseVec<T> {
    pub fn retain(&mut self, ctx: &mut DatabaseContextData, mut f: impl FnMut(&mut T) -> bool) {
        let mut read_offset = 0;
        let mut write_offset = 0;
        let mut new_len = self.length;

        while read_offset < self.length {
            let read_ptr = ThinPtr(self.data + read_offset * size_of::<T>());
            let val: Pin<&mut T> = unsafe { ctx.access_pod_mut(read_ptr) };
            let retain = f(unsafe { val.get_unchecked_mut() });
            if !retain {
                new_len -= 1;
                read_offset += 1;
            } else {
                if read_offset != write_offset {
                    let write_ptr = ThinPtr(self.data + write_offset * size_of::<T>());
                    NoatunContext.copy_sized(read_ptr, write_ptr, size_of::<T>());
                }
                read_offset += 1;
                write_offset += 1;
            }
        }
        NoatunContext.write_pod_ptr(new_len, addr_of_mut!(self.length));
    }

    pub(crate) fn get_slice(&self, context: &DatabaseContextData, range: Range<usize>) -> &[T] {
        let offset = self.data + range.start * size_of::<T>();
        let len = range.end - range.start;

        unsafe { context.access_slice_at(offset, len) }
    }
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_full_slice_mut(&self, context: &DatabaseContextData) -> &mut [T] {
        let offset = self.data;
        unsafe { context.access_slice_at_mut(offset, self.length) }
    }
    pub(crate) fn get_full_slice(&self, context: &DatabaseContextData) -> &[T] {
        let offset = self.data;
        unsafe { context.access_slice_at(offset, self.length) }
    }
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_slice_mut(
        &self,
        context: &DatabaseContextData,
        range: Range<usize>,
    ) -> &mut [T] {
        let offset = self.data + range.start * size_of::<T>();
        let len = range.end - range.start;

        unsafe { context.access_slice_at_mut(offset, len) }
    }
    pub(crate) fn get(&self, ctx: &DatabaseContextData, index: usize) -> &T {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        unsafe { ctx.access_pod(ThinPtr(offset)) }
    }
    #[allow(clippy::mut_from_ref)]
    // TODO: Maybe make this unsafe(even though it's internal)
    pub(crate) fn get_mut(&self, ctx: &DatabaseContextData, index: usize) -> Pin<&mut T> {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        let t = unsafe { ctx.access_pod_mut(ThinPtr(offset)) };
        t
    }
    pub(crate) fn swap_remove(&mut self, ctx: &mut DatabaseContextData, index: usize) {
        assert!(index < self.length);
        if index + 1 == self.length {
            ctx.write_pod(index, unsafe { Pin::new_unchecked(&mut self.length) });
        } else {
            let last_index = self.length - 1;
            ctx.write_pod(last_index, unsafe { Pin::new_unchecked(&mut self.length) });

            let src_offset = self.data + last_index * size_of::<T>();
            let src_obj = ThinPtr(src_offset);

            let dst_offset = self.data + index * size_of::<T>();
            let dst_obj = ThinPtr(dst_offset);

            ctx.copy_sized(src_obj, dst_obj, size_of::<T>());
        }
    }
    pub(crate) fn write_untracked(&mut self, ctx: &mut DatabaseContextData, index: usize, val: T) {
        let offset = self.data + index * size_of::<T>();
        unsafe {
            ctx.write_pod(val, ctx.access_pod_mut(ThinPtr(offset)));
        };
    }
    pub(crate) fn push_untracked(&mut self, ctx: &mut DatabaseContextData, t: T) -> ThinPtr
    where
        T: AnyBitPattern,
    {
        if self.length >= self.capacity {
            self.realloc_add(ctx, (self.capacity + 1) * 2, self.length + 1);
        } else {
            ctx.write_pod(self.length + 1, Pin::new(&mut self.length));
        }

        self.write_untracked(ctx, self.length - 1, t);
        let offset = self.data + (self.length - 1) * size_of::<T>();
        ThinPtr(offset)
    }
}
impl<T> Object for RawDatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;
    type DetachedType = [T::DetachedOwnedType];
    type DetachedOwnedType = Vec<T::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        todo!("RawDatabaseVec does not support detach")
    }

    fn clear(self: Pin<&mut Self>) {
        // The Raw type is special, is isn't tracked
    }

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {
        panic!("init_from_detached is not implemented for RawDatabaseVec");
    }
    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        panic!("allocate_from_detached is not implemented for RawDatabaseVec");
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }
    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_pod_mut(index) }
    }
}

#[repr(C)]
#[derive(Clone,Copy,Zeroable,Pod)]
//WARNING! this must be identical to first 3 fields of DatabaseVec
struct DatabaseVecLengthCapData {
    length: usize,
    capacity: usize,
    data: usize,
}

#[repr(C)]
pub struct DatabaseVec<T: FixedSizeObject> {
    //WARNING! These first 3 fields must be identical to DatabaseVecLengthCapData
    length: usize,
    capacity: usize,
    data: usize,
    length_registrar: SequenceNr,
    phantom_data: PhantomData<T>,
}

impl<T: FixedSizeObject> Debug for DatabaseVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DatabaseVec({})", { self.length })
    }
}

unsafe impl<T: FixedSizeObject> Zeroable for DatabaseVec<T> {}

impl<T: FixedSizeObject> Copy for DatabaseVec<T> {}

impl<T: FixedSizeObject> Clone for DatabaseVec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

// TODO: This is currently unsound. DatabaseVec has padding.
unsafe impl<T: FixedSizeObject> Pod for DatabaseVec<T> where T: 'static {}

pub struct DatabaseVecIterator<'a, T: FixedSizeObject> {
    vec: &'a DatabaseVec<T>,
    index: usize,
}

pub struct DatabaseVecIteratorMut<'a, T: FixedSizeObject> {
    vec: Pin<&'a mut DatabaseVec<T>>,
    index: usize,
}

impl<'a, T: FixedSizeObject + 'static> Iterator for DatabaseVecIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vec.length {
            return None;
        }
        let index = self.index;
        self.index += 1;
        Some(self.vec.get(index))
    }
}
impl<'a, T: FixedSizeObject + 'static> Iterator for DatabaseVecIteratorMut<'a, T> {
    type Item = Pin<&'a mut T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vec.length {
            return None;
        }
        let index = self.index;
        self.index += 1;
        //TODO: Get rid of this transmute. Why is it even needed?
        let vec = unsafe { Pin::new_unchecked(self.vec.as_mut().get_unchecked_mut()) };
        Some(unsafe {
            Pin::new_unchecked(std::mem::transmute::<&mut T, &mut T>(
                DatabaseVec::getmut(vec, index).get_unchecked_mut(),
            ))
        })
    }
}

impl<T: FixedSizeObject + 'static> DatabaseVec<T> {
    pub fn iter(&self) -> DatabaseVecIterator<T> {
        DatabaseVecIterator {
            vec: self,
            index: 0,
        }
    }
    pub fn iter_mut<'a>(self: Pin<&'a mut Self>) -> DatabaseVecIteratorMut<'a, T> {
        DatabaseVecIteratorMut {
            vec: self,
            index: 0,
        }
    }
    fn realloc_add(&mut self, new_capacity: usize, new_len: usize) {
        debug_assert!(new_capacity >= new_len);
        debug_assert!(new_capacity >= self.capacity);
        debug_assert!(new_len >= self.length);
        let dest = NoatunContext.allocate_raw(new_capacity * size_of::<T>(), align_of::<T>());
        let dest_index = NoatunContext.index_of_ptr(dest);

        if self.length > 0 {
            let old_ptr = FatPtr::from(self.data, size_of::<T>() * self.length);
            NoatunContext.copy(old_ptr, dest_index);
        }

        NoatunContext.write_pod(
            DatabaseVecLengthCapData {
                length: new_len,
                capacity: new_capacity,
                data: dest_index.0,
            },
            //TODO: Add a "write-many-consecutive-fields" macro, with offset+size validation, rather than the following abomination
            unsafe { Pin::new_unchecked(std::mem::transmute::<&mut Self, &mut DatabaseVecLengthCapData>(self)) },
        )
    }
    #[allow(clippy::mut_from_ref)]
    pub fn new<'a>() -> Pin<&'a mut DatabaseVec<T>> {
        NoatunContext.allocate_pod::<DatabaseVec<T>>()
    }
}

impl<T> DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    pub fn len(&self) -> usize {
        NoatunContext.observe_registrar(self.length_registrar);
        self.length
    }
    pub fn is_empty(&self) -> bool {
        NoatunContext.observe_registrar(self.length_registrar);
        self.length == 0
    }
    pub fn get(&self, index: usize) -> &T {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        unsafe { T::access(ThinPtr(offset)) }
    }

    // TODO: Find a better name for this!
    pub fn getmut(self: Pin<&mut Self>, index: usize) -> Pin<&mut T> {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        let t = unsafe { T::access_mut(ThinPtr(offset)) };
        t
    }
    fn get_mut_internal(&mut self, index: usize) -> Pin<&mut T> {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        let t = unsafe { T::access_mut(ThinPtr(offset)) };
        t
    }
    pub(crate) fn write(&mut self, index: usize, val: T) {
        let offset = self.data + index * size_of::<T>();
        unsafe {
            let dest = T::access_mut(ThinPtr(offset));
            NoatunContext.write_object(val, dest);
        };
    }

    pub fn set_item_infallible(mut self: Pin<&mut Self>, index: usize, val: impl Borrow<<T as Object>::DetachedType>) {
        while index <= self.length {
            //TODO: We could optimize this, when growing by a lot, we should set capacity first
            self.as_mut().push_zeroed();
        }
        let tself = unsafe { self.get_unchecked_mut() };
        let offset = ThinPtr(tself.data + index * size_of::<T>());
        unsafe {
            let item_data = <T as Object>::access_mut(offset);
            item_data.init_from_detached(val.borrow());
        }
    }

    pub fn clear(mut self: Pin<&mut Self>) {
        for i in 0..self.length {
            self.as_mut().getmut(i).clear();
        }
        let tself =  unsafe { self.get_unchecked_mut() };
        NoatunContext.update_registrar(&mut tself.length_registrar);
        NoatunContext.write_pod_ptr(0, addr_of_mut!(tself.length));
    }

    pub fn push_zeroed(self: Pin<&mut Self>) -> Pin<&mut T> {
        let tself = unsafe { self.get_unchecked_mut() };
        if tself.length >= tself.capacity {
            tself.realloc_add((tself.capacity + 1) * 2, tself.length + 1);
        } else {
            NoatunContext.write_pod_ptr(tself.length + 1, addr_of_mut!(tself.length));
        }
        NoatunContext.update_registrar(&mut tself.length_registrar);
        tself.get_mut_internal(tself.length - 1)
    }

    pub fn shift_remove(self: Pin<&mut Self>, index: usize) {
        if index >= self.length {
            return;
        }
        let tself = unsafe { self.get_unchecked_mut() };


        if index == tself.length - 1 {
            NoatunContext.update_registrar(&mut tself.length_registrar);
            NoatunContext.write_pod_ptr(tself.length - 1, addr_of_mut!(tself.length));
            return;
        }
        let src_ptr = ThinPtr(tself.data + (tself.length - 1) * size_of::<T>());
        let dst_ptr = ThinPtr(tself.data + index * size_of::<T>());
        unsafe { T::access_mut(dst_ptr).clear(); }
        NoatunContext.copy(FatPtr::from(src_ptr.0, size_of::<T>()), dst_ptr);

        NoatunContext.update_registrar(&mut tself.length_registrar);
        NoatunContext.write_pod_ptr(tself.length - 1, addr_of_mut!(tself.length));
    }

    pub fn retain(self: Pin<&mut Self>, mut f: impl FnMut(Pin<&mut T>) -> bool) {

        let mut read_offset = 0;
        let mut write_offset = 0;
        let mut new_len = self.length;

        while read_offset < self.length {
            let read_ptr = ThinPtr(self.data + read_offset * size_of::<T>());
            let mut val = unsafe { T::access_mut(read_ptr) };
            let retain = f(val.as_mut());
            if !retain {
                val.clear();
                new_len -= 1;
                read_offset += 1;
            } else {
                if read_offset != write_offset {
                    let write_ptr = ThinPtr(self.data + write_offset * size_of::<T>());
                    NoatunContext.copy_sized(read_ptr, write_ptr, size_of::<T>());
                }
                read_offset += 1;
                write_offset += 1;
            }
        }
        let self_mut = unsafe {self.get_unchecked_mut()};
        NoatunContext.update_registrar(&mut self_mut.length_registrar);
        NoatunContext.write_pod_ptr(new_len, addr_of_mut!(self_mut.length));
    }

    pub fn push(mut self: Pin<&mut Self>, t: impl Borrow<<T as Object>::DetachedType>) {
        self.as_mut().push_zeroed();

        let tself = unsafe { self.get_unchecked_mut() };
        let index = tself.length - 1;
        let offset = ThinPtr(tself.data + index * size_of::<T>());
        unsafe {
            <T as Object>::access_mut(offset).init_from_detached(t.borrow());
        }
    }
}

impl<T> Object for DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;
    type DetachedType = [T::DetachedOwnedType];
    type DetachedOwnedType = Vec<T::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter().map(|x| x.detach()).collect()
    }

    fn clear(self: Pin<&mut Self>) {
        self.clear()
    }

    fn init_from_detached(mut self: Pin<&mut Self>, detached: &Self::DetachedType) {
        use std::borrow::Borrow;
        for item in detached {
            let new_item = self.as_mut().push_zeroed();
            new_item.init_from_detached(item.borrow());
        }
    }
    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut pod: Pin<&mut Self> = NoatunContext.allocate_pod();
        pod.as_mut().init_from_detached(detached);
        pod
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }
    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_pod_mut(index) }
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

unsafe impl<T: Object + ?Sized + 'static> AnyBitPattern for DatabaseObjectHandle<T> {}
impl<T: Object + ?Sized + 'static> Object for DatabaseObjectHandle<T>
where
    T::Ptr: Pod,
{
    type Ptr = ThinPtr;
    type DetachedType = T::DetachedType;
    type DetachedOwnedType = T::DetachedOwnedType;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.get().detach()
    }

    fn clear(self: Pin<&mut Self>) {
        Self::getmut(self).clear()
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        unsafe {
            let target = T::allocate_from_detached(detached);
            let new_index = NoatunContext.index_of(&*target);
            NoatunContext.write_pod(
                new_index,
                Pin::new_unchecked(&mut self.get_unchecked_mut().object_index),
            );
        }
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut pod: Pin<&mut Self> = NoatunContext.allocate_pod();
        pod.as_mut().init_from_detached(detached);
        pod
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }
    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_object_mut(index) }
    }
}

impl<T: Object + ?Sized> Deref for DatabaseObjectHandle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        if self.object_index.is_null() {
            panic!("get() called on an uninitialized (null) DatabaseObjectHandle.");
        }
        unsafe { T::access(self.object_index) }
    }
}

impl<T: Object + ?Sized> DatabaseObjectHandle<T> {
    pub fn get(&self) -> &T {
        if self.object_index.is_null() {
            panic!("get() called on an uninitialized (null) DatabaseObjectHandle.");
        }
        unsafe { T::access(self.object_index) }
    }
    pub fn getmut(self: Pin<&mut Self>) -> Pin<&mut T> {
        let tself = unsafe { self.get_unchecked_mut() };
        if tself.object_index.is_null() {
            panic!("get_mut() called on an uninitialized (null) DatabaseObjectHandle.");
        }
        unsafe { T::access_mut(tself.object_index) }
    }

    pub fn new(value: T::Ptr) -> Self {
        Self {
            object_index: value,
            phantom: Default::default(),
        }
    }
    #[allow(clippy::mut_from_ref)]
    pub fn allocate<'a>(value: T) -> Pin<&'a mut Self>
    where
        T: Object<Ptr = ThinPtr>,
        T: AnyBitPattern,
    {
        let mut this = NoatunContext.allocate_pod::<DatabaseObjectHandle<T>>();
        let mut target = NoatunContext.allocate_pod::<T>();
        // TODO: This is a bug, isn't it? Should use NoatunContext.write!?
        // TODO: Maybe not? Maybe overwriting newly allocated info doesn't need tracked writes?
        unsafe {
            *target.as_mut().get_unchecked_mut() = value;
        }
        unsafe {
            this.as_mut().get_unchecked_mut().object_index = NoatunContext.index_of(&*target);
        }
        this
    }

    #[allow(clippy::mut_from_ref)]
    pub fn allocate_unsized<'a>(value: &T) -> Pin<&'a mut Self>
    where
        T: Object<Ptr = FatPtr> + 'static,
    {
        let size_bytes = std::mem::size_of_val(value);
        let mut this = NoatunContext.allocate_pod::<DatabaseObjectHandle<T>>();
        let target_dst_ptr = NoatunContext.allocate_raw(size_bytes, std::mem::align_of_val(value));

        let target_src_ptr = value as *const T as *const u8;

        //let (src_ptr, src_metadata): (*const u8, usize) = unsafe { transmute_copy(&value) };

        unsafe { std::ptr::copy(target_src_ptr, target_dst_ptr, size_bytes) };
        let thin_index = NoatunContext.index_of_ptr(target_dst_ptr);

        unsafe {
            this.as_mut().get_unchecked_mut().object_index =
                FatPtr::from(thin_index.start(), size_bytes)
        };
        this
    }
}




#[repr(C)]
#[derive(Clone,Copy,AnyBitPattern)]
//WARNING! this must be identical to first 3 fields of DatabaseVec
struct DatabaseHashBucket<K,V> {
    hash: u32,
    used: u32,
    key: K,
    v: V,
}

#[repr(C)]
#[derive(Clone,Copy)]
pub struct DatabaseHash<K: AnyBitPattern, V: FixedSizeObject> {
    length: usize,
    capacity: usize,
    data: usize,
    phantom_data: PhantomData<(K,V)>,
}

unsafe impl<K:AnyBitPattern,V:FixedSizeObject> Pod for DatabaseHash<K,V> {}
unsafe impl<K:AnyBitPattern,V:FixedSizeObject> Zeroable for DatabaseHash<K,V> {}

#[derive(Clone,Copy,Debug)]
struct BucketNr(usize);
impl BucketNr {
    fn from_u64(x: u64, cap: usize) -> (BucketNr,Meta) {
        (BucketNr((x as usize)%cap),
         Meta(((x as usize)/cap) as u8|128)
        )
    }
    fn advance(&mut self, capacity: usize) {
        self.0 += 1;
        self.0 %= capacity;
    }
}

#[derive(Clone,Copy,Debug, PartialEq, Zeroable, Pod)]
#[repr(transparent)]
struct Meta(u8);
impl Meta {
    fn deleted(&self) -> bool {
        self.0 == 1
    }
    fn populated(&self) -> bool {
        self.0 &128 != 0
    }
    fn empty(&self) -> bool {
        self.0 == 0
    }
}

struct WithConcat<I,V>(Option<I>, Option<V>);
impl<I:Iterator> WithConcat<I,I::Item> {
    fn new(iter: I, val: I::Item) -> Self {
        WithConcat(Some(iter), Some(val))
    }
}
impl<I:Iterator> Iterator for WithConcat<I,I::Item> {
    type Item = I::Item;

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut left_len = self.0.as_ref().map(|x|x.size_hint().0).unwrap_or(0);
        if let Some(_right) = &self.1 {
            left_len += 1;
        }
        (left_len, Some(left_len))
    }
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(i1) = self.0.as_mut() {
            let next = i1.next();
            if next.is_some() {
                return next;
            }
            self.0 = None;
        }
        if let Some(i2) = self.1.take() {
            return Some(i2);
        }
        None
    }
}
impl<I:Iterator> ExactSizeIterator for WithConcat<I,I::Item> {

}

enum ProbeRunResult {
    HashFull,
    /// Found a bucket with the given key, with a value present
    FoundPopulated(BucketNr, Meta),
    /// Found either an empty, or deleted
    FoundUnoccupied(BucketNr, Meta),
}

struct DatabaseHashIterator<'a, K:AnyBitPattern+Hash+PartialEq,V:FixedSizeObject> {
    hash_buckets: &'a [DatabaseHashBucket<K,V>],
    next_position: usize,
}
struct DatabaseHashOwningIterator<K:AnyBitPattern+Hash+PartialEq,V:FixedSizeObject> {
    hash_buckets: DatabaseHash<K,V>,
    next_position: usize,
}
impl<'a,K: AnyBitPattern+Hash+PartialEq, V: FixedSizeObject> Iterator for DatabaseHashIterator<'a,K,V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let pos = self.next_position;
            if pos >= self.hash_buckets.len() {
                return None;
            }
            self.next_position += 1;
            let bucket = &self.hash_buckets[pos];
            if bucket.used != 0 {
                return Some((&bucket.key, &bucket.v));
            }
        }

    }
}

impl<K: AnyBitPattern+Hash+PartialEq, V: FixedSizeObject> Iterator for DatabaseHashOwningIterator<K,V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let all_buckets = self.hash_buckets.all_buckets_mut();
        loop {
            let pos = self.next_position;
            if pos >= all_buckets.len() {
                return None;
            }
            self.next_position += 1;
            let bucket = &mut all_buckets[pos];
            if bucket.used != 0 {
                bucket.used = 0;
                return Some((
                                std::mem::replace(&mut bucket.key, K::zeroed()),
                                std::mem::replace(&mut bucket.v, V::zeroed()),
                            ));
            }
        }

    }
}
impl<K: AnyBitPattern+Hash+PartialEq, V: FixedSizeObject> DatabaseHash<K, V> {

    pub fn len(&self) -> usize {
        self.length
    }
    pub fn iter(&self) -> DatabaseHashIterator<K,V> {
        DatabaseHashIterator {
            hash_buckets: self.all_buckets(),
            next_position: 0,
        }
    }
    pub fn into_iter(self) -> DatabaseHashOwningIterator<K,V> {
        DatabaseHashOwningIterator {
            hash_buckets: self,
            next_position: 0,
        }
    }

    fn get_meta(&self, data_and_meta_ptr: *const u8, bucket: BucketNr) -> Meta {
        unsafe {
            Meta(data_and_meta_ptr.wrapping_add(bucket.0).read())
        }
    }
    // Returns a pointer to the hashmaps heap-block. This, in turn, contains
    // both the meta-data segment, and the actual individual buckets.
    fn get_mut_meta(&mut self, data_and_meta_ptr: *const u8, bucket: BucketNr) -> Pin<&mut Meta> {
        unsafe {
            Pin::new_unchecked(&mut *(data_and_meta_ptr.wrapping_add(bucket.0) as *mut Meta))
        }
    }
    fn all_buckets(&self) -> &[DatabaseHashBucket<K,V>] {
        unsafe { slice::from_raw_parts(
            self.data_and_meta_ptr().wrapping_add(self.capacity) as *const DatabaseHashBucket<K,V>,
            self.capacity
        ) }
    }
    fn all_buckets_mut(&mut self) -> &mut [DatabaseHashBucket<K,V>] {
        unsafe { slice::from_raw_parts_mut(
            self.data_and_meta_ptr().wrapping_add(self.capacity) as *mut DatabaseHashBucket<K,V>,
            self.capacity
        ) }
    }
    fn get_bucket<'a>(&self, data_and_meta_ptr: *mut u8, bucket: BucketNr) -> &'a mut DatabaseHashBucket<K,V> {
        unsafe { &mut *(data_and_meta_ptr.wrapping_add(self.capacity).wrapping_add(size_of::<DatabaseHashBucket<K,V>>()*bucket.0) as *mut DatabaseHashBucket<K,V> ) }
    }
    fn data_and_meta_ptr(&self) -> *mut u8 {
        NoatunContext.start_ptr_mut().wrapping_add(self.data)
    }
    fn probe(&self, key: &K) -> ProbeRunResult {
        if self.capacity ==  0 {
            return ProbeRunResult::HashFull;
        }
        let data_and_meta_ptr = self.data_and_meta_ptr() as *mut u8;
        let mut h = NoatunHasher::new();
        key.hash(&mut h);
        let (mut bucket_nr, key_meta) = BucketNr::from_u64(h.finish(), self.capacity);
        let mut visited_buckets = 0;
        let max_visited_buckets = self.capacity/2;
        let mut first_deleted = None;
        loop {
            let cur_bucket_meta = self.get_meta(data_and_meta_ptr, bucket_nr);
            if cur_bucket_meta.deleted() {
                if first_deleted.is_none() {
                    first_deleted = Some(bucket_nr);
                }
            } else if cur_bucket_meta == key_meta {
                let bucket = self.get_bucket(data_and_meta_ptr, bucket_nr);
                if bucket.key == *key {
                    return ProbeRunResult::FoundPopulated(bucket_nr, key_meta);
                }
            } else if cur_bucket_meta.empty() {
                return ProbeRunResult::FoundUnoccupied(first_deleted.unwrap_or(bucket_nr), key_meta);
            }
            visited_buckets += 1;
            if visited_buckets >= max_visited_buckets {
                return ProbeRunResult::HashFull
            }
            bucket_nr.advance(self.capacity);
        }
    }
    fn next_prime(capacity: usize) -> usize {
        static PRIMES:&[usize] =&[
            7,
            17,
            37,
            67,
            127,
            (1usize<<8)  -	5,
            (1usize<<9)  -	3,
            (1usize<<10) - 	3,
            (1usize<<11) - 	9,
            (1usize<<12) - 	3,
            (1usize<<13) - 	1,
            (1usize<<14) - 	3,
            (1usize<<15) - 	19,
            (1usize<<16) - 	15,
            (1usize<<17) - 	1,
            (1usize<<18) - 	5,
            (1usize<<19) - 	1,
            (1usize<<20) - 	3,
            (1usize<<21) - 	9,
            (1usize<<22) - 	3,
            (1usize<<23) - 	15,
            (1usize<<24) - 	3,
            (1usize<<25) - 	39,
            (1usize<<26) - 	5,
            (1usize<<27) - 	39,
            (1usize<<28) - 	57,
            (1usize<<29) - 	3,
            (1usize<<30) - 	35,
            (1usize<<31) - 	1,
            (1usize<<32) - 	5,
            (1usize<<33) - 	9,
            (1usize<<34) - 	41,
            (1usize<<35) - 	31,
            (1usize<<36) - 	5,
            (1usize<<37) - 	25,
            (1usize<<38) - 	45,
            (1usize<<39) - 	7,
            (1usize<<40) - 	87,
            (1usize<<41) - 	21,
            (1usize<<42) - 	11,
            (1usize<<43) - 	57,
            (1usize<<44) - 	17,
            (1usize<<45) - 	55,
            (1usize<<46) - 	21,
            (1usize<<47) - 	115,
            (1usize<<48) - 	59,
            (1usize<<49) - 	81,
            (1usize<<50) - 	27,
            (1usize<<51) - 	129,
            (1usize<<52) - 	47,
            (1usize<<53) - 	111,
            (1usize<<54) - 	33,
            (1usize<<55) - 	55,
            (1usize<<56) - 	5,
            (1usize<<57) - 	13,
            (1usize<<58) - 	27,
            (1usize<<59) - 	55,
            (1usize<<60) - 	93,
            (1usize<<61) - 	1,
            (1usize<<62) - 	57,
            (1usize<<63) - 	25,
        ];
        let (Ok(x)|Err(x)) = PRIMES.binary_search(&capacity);
        PRIMES[x.min(PRIMES.len()-1)]
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        match self.probe(&key) {
            ProbeRunResult::FoundPopulated(bucket, _) => {
                Some(&self.get_bucket(self.data_and_meta_ptr(), bucket).v)
            },
            _ => None
        }
    }
    pub fn insert(&mut self, key: K, val: &V::DetachedType) {
        let probe_result = self.probe(&key);
        match probe_result {
            ProbeRunResult::HashFull => {
                let mut new = Self::from_iter_internal(
                    self.into_iter(),
                    Self::next_prime(self.capacity+1)
                );
                // Will not give infinite recursion, since 'new' has a capacity of at least 1 more
                new.insert(key, val);
                self.replace_internal(new);
            }
            ProbeRunResult::FoundUnoccupied(bucket, meta)| //TODO: We _could_ use the knowledge that this is unoccupied, to avoid the zero-check in write_pod
            ProbeRunResult::FoundPopulated(bucket, meta) => {
                let data_and_meta_ptr = self.data_and_meta_ptr() as *mut u8;
                let bucket_obj = self.get_bucket(data_and_meta_ptr, bucket);
                let old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };
                V::init_from_detached(old_v, val);
                let bucket_meta = self.get_mut_meta(data_and_meta_ptr, bucket);
                NoatunContext.write_pod(meta, bucket_meta);
                if matches!(probe_result, ProbeRunResult::FoundUnoccupied(_, _)) {
                    let old_k= unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
                    NoatunContext.write_any(key, old_k);
                }
            }
        }
    }
    fn insert_impl(&mut self, key: K, val: V) {
        let probe_result = self.probe(&key);
        match probe_result {
            ProbeRunResult::HashFull => {
                let mut new = Self::from_iter_internal(
                    self.into_iter(),
                    Self::next_prime(self.capacity+1)
                );
                // Will not give infinite recursion, since 'new' has a capacity of at least 1 more
                new.insert_impl(key, val);
                self.replace_internal(new);
            }
            ProbeRunResult::FoundUnoccupied(bucket, meta)| //TODO: We _could_ use the knowledge that this is unoccupied, to avoid the zero-check in write_pod
            ProbeRunResult::FoundPopulated(bucket, meta) => {
                let data_and_meta_ptr = self.data_and_meta_ptr();
                let bucket_obj = self.get_bucket(data_and_meta_ptr, bucket);
                let old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };
                NoatunContext.write_any(val, old_v);
                let bucket_meta = self.get_mut_meta(data_and_meta_ptr, bucket);
                NoatunContext.write_pod(meta, bucket_meta);
                if matches!(probe_result, ProbeRunResult::FoundUnoccupied(_, _)) {
                    let old_k= unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
                    NoatunContext.write_any(key, old_k);
                }
            }
        }
    }
    fn with_capacity(capacity: usize) -> Self {

        let meta_size = capacity;

        let aligned_meta_size = meta_size.next_multiple_of(align_of::<DatabaseHashBucket<K,V>>());
        let meta_padding = aligned_meta_size - meta_size;
        let bucket_data_size = capacity * size_of::<DatabaseHashBucket<K,V>>();

        let data = NoatunContext.allocate_raw(aligned_meta_size + bucket_data_size, align_of::<DatabaseHashBucket<K,V>>());

        let aligned_data = data.wrapping_add(meta_padding);

        Self {
            length: 0,
            capacity,
            data: NoatunContext.index_of_ptr(aligned_data).0,
            phantom_data: PhantomData,
        }
    }

    fn replace_internal(&mut self, other: Self)  {
        NoatunContext.write_pod(other, unsafe { Pin::new_unchecked(self) })
    }

    // This does not write the handle itself into the noatun database!!
    fn from_iter_internal(i: impl Iterator<Item=(K, V)>, min_capacity: usize) -> Self {
        let capacity = i.size_hint().0;
        let mut temp = Self::with_capacity(capacity.max(min_capacity));
        for (item_key, item_val) in i {
            temp.insert_impl(item_key, item_val);
        }
        temp
    }
}

impl<K:AnyBitPattern+Hash+Eq,V:FixedSizeObject> Object for DatabaseHash<K,V> {
    type Ptr = ThinPtr;
    type DetachedType = HashMap<K,V::DetachedOwnedType>;
    type DetachedOwnedType = HashMap<K,V::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter().map(|(k,v)| (*k,v.detach())).collect()
    }

    fn clear(self: Pin<&mut Self>) {
        let length = unsafe  { self.map_unchecked_mut(|x|&mut x.length) };
        NoatunContext.write_pod(0, length);
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        let tself = unsafe  { self.get_unchecked_mut() };
        for (k,v) in detached {
            tself.insert(*k, v.borrow());
        }
        todo!()
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        todo!()
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        todo!()
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use datetime_literal::datetime;
    use super::DatabaseHash;
    use crate::{CutOffDuration, Database, DatabaseCell, Object};
    use crate::tests::DummyTestApp;

    #[test]
    fn test_hashmap_miri0() {
        let mut db: Database<DummyTestApp<DatabaseHash<u32, DatabaseCell<u32>>>> = Database::create_in_memory(
            10000,
            CutOffDuration::from_minutes(15),
            Some(datetime!(2021-01-01 Z).into()),
            None,
            (),
        )
            .unwrap();
        db.with_root_mut(|mut map| {
            let mut map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            map.0.insert(42, &42);

            let val = map.0.get(&42).unwrap();
            assert_eq!(val.get(), 42);

        })
            .unwrap();
    }
}