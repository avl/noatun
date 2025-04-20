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
use std::mem::MaybeUninit;
use std::ops::{Add, Deref, Range};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::slice;
use tracing::trace;
use crate::data_types::meta_finder::get_any_empty;
use crate::xxh3_vendored::NoatunHasher;

mod noatun_hash_impls;

#[derive(Copy, Clone, AnyBitPattern)]
#[repr(C)]
pub struct NoatunString {
    start: ThinPtr,
    length: usize,
    registrar: SequenceNr,
    padding: u32
}

impl Debug for NoatunString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl Hash for NoatunString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: &str = self.as_ref();
        // We can safely defer to the std hash method here, since
        // `Hash` is only used for per-process hashes. Long-lived hashes
        // use NoatunHash trait instead.
        s.hash(state);
    }
}

impl Eq for NoatunString {}
impl PartialEq for NoatunString {
    fn eq(&self, other: &Self) -> bool {
        let s: &str = self;
        let o : &str = other;
        s.eq(o)
    }
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
    pub(crate) fn assign_untracked(&mut self, value: &str) {
        if self.get().starts_with(value) {
            self.length = value.len();
            return;
        }

        let raw = NoatunContext.allocate_raw(value.len(), 1);
        let target = unsafe { slice::from_raw_parts_mut(raw, value.len()) };
        target.copy_from_slice(value.as_bytes());
        let raw_index = NoatunContext.index_of_ptr(raw);
        self.start = raw_index;
        self.length = value.len();
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
pub struct DatabaseCell<T> {
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
pub struct OpaqueCell<T> {
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

impl<T: FixedSizeObject+Debug> Debug for DatabaseVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
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
struct DatabaseHashBucket<K,V> {
    hash: u32,
    key: K,
    v: V,
}


//TODO: Rename to NoatunHash?
#[repr(C)]
#[derive(Clone,Copy)]
pub struct DatabaseHash<K: AnyBitPattern, V: FixedSizeObject> {
    length: usize,
    capacity: usize,
    data: usize,
    phantom_data: PhantomData<(K,V)>,
}

impl<K:AnyBitPattern+NoatunKey+PartialEq+Debug,V:FixedSizeObject+Debug> Debug for DatabaseHash<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

unsafe impl<K:AnyBitPattern,V:FixedSizeObject> Pod for DatabaseHash<K,V> {}
unsafe impl<K:AnyBitPattern,V:FixedSizeObject> Zeroable for DatabaseHash<K,V> {}

// Some of the stuff here is public doc(hidden) just so that a benchmark can get at it
#[doc(hidden)]
#[derive(Clone,Copy,Debug,PartialEq)]
pub struct BucketNr(pub usize);

#[doc(hidden)]
#[derive(Clone,Copy,Debug,PartialEq)]
pub struct MetaGroupNr(pub usize);

impl MetaGroup {
    #[inline]
    fn validate(&self) {
        #[cfg(debug_assertions)]
        {
            let first_empty = self.0.iter().position(|x|x.empty());
            if let Some(first_empty) = first_empty {
                for i in 0..32 {
                    assert!(!self.0[i].deleted());
                }
                for i in first_empty .. 32 {
                    assert!(self.0[i].empty());
                }
            }
        }
    }
}

impl MetaGroupNr {
    fn from_u64(x: u64, cap: usize) -> (MetaGroupNr,Meta) {
        let groupcount = cap.div_ceil(32);
        (MetaGroupNr((x as usize)%groupcount),
         Meta(((x as usize)/groupcount) as u8|128)
        )
    }

}


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
impl Add<usize> for BucketNr {
    type Output = BucketNr;

    fn add(self, rhs: usize) -> Self::Output {
        Self(self.0 + rhs)
    }
}

#[derive(Clone,Copy)]
pub struct BucketProbeSequence {
    cur_group: usize,
    group_capacity: usize,
    grops_visited: usize,
    group_step: usize,
}

impl BucketProbeSequence {
    pub fn new(start_group: MetaGroupNr, total_group_count: usize) -> BucketProbeSequence {
        BucketProbeSequence {
            cur_group: start_group.0,
            group_capacity: total_group_count,
            grops_visited: start_group.0,
            group_step: 1,
        }
    }
    pub fn probe_next(&mut self) -> Option<MetaGroupNr> {
        // step is >= capacity after approx self.capacity/2 iterations, since
        // step is incremented by 2
        if self.group_step >= self.group_capacity {
            return None;
        }
        let ret = self.cur_group;
        self.cur_group += self.group_step;
        if self.cur_group >= self.group_capacity {
            self.cur_group -= self.group_capacity;
            debug_assert!(self.cur_group < self.group_capacity);
        }
        self.group_step += 2;
        // The above formula results in visiting buckets 0, 1, 4, 9, 16, 25 etc...
        Some(MetaGroupNr(ret))
    }
}


/// Meta data for bucket
///
/// 0 = unoccupied
/// 1 = deleted
/// 2..=127 = invalid
/// >=128 = populated
#[derive(Clone,Copy,Debug, PartialEq, Zeroable, Pod)]
#[repr(transparent)]
#[doc(hidden)]
pub struct Meta(u8);

#[repr(align(32))]
#[doc(hidden)]
#[derive(Clone,Copy,Debug)]
pub struct MetaGroup(pub [Meta;32]);


enum ProbeResult {
    Found,
    NotFound,
    Full
}

/// Returns when end of probe sequence was reached, or when closure returns true.
#[doc(hidden)]
pub fn run_get_probe_sequence(metas: &[MetaGroup], max_buckets: usize, needle: Meta, mut f: impl FnMut(BucketNr) -> bool, mut probe: BucketProbeSequence) {

    loop {
        let Some(group_index) = probe.probe_next() else {
            return;
        };
        let group = &metas[group_index.0];

        let bucket_offset = BucketNr(group_index.0 * 32);
        if meta_finder::meta_get_group_find(group,
                                            if group_index.0 + 1 ==metas.len() {max_buckets-32*group_index.0} else {32},
                                            needle, |index|f(bucket_offset + index)) {
            return;
        }
    }
}

#[derive(Clone,Copy,PartialEq,Debug)]
pub enum ProbeRunResult {
    HashFull,
    /// Found a bucket with the given key, with a value present
    FoundPopulated(BucketNr, Meta),
    /// Found either an empty, or deleted
    FoundUnoccupied(BucketNr, Meta),
}

/// Closure should return true iff bucket with given index has the key we're probing for.
#[doc(hidden)]
pub fn run_insert_probe_sequence(metas: &[MetaGroup], max_buckets: usize, needle: Meta, mut f: impl FnMut(BucketNr) -> bool, mut probe: BucketProbeSequence) -> ProbeRunResult {
    let mut first_deleted = None;
    loop {
        let Some(group_index) = probe.probe_next() else {
            return ProbeRunResult::HashFull;
        };
        let bucket_offset = BucketNr(32*group_index.0);
        let group = &metas[group_index.0];

        match meta_finder::meta_insert_group_find(bucket_offset,  &mut first_deleted, group,
                                                  if group_index.0 + 1 ==metas.len() {max_buckets-32*group_index.0} else {32},
                                                  needle, |index|f(bucket_offset + index)) {
            Some(ProbeRunResult::FoundUnoccupied(_, meta)) if first_deleted.is_some() => {
                return ProbeRunResult::FoundUnoccupied(first_deleted.unwrap(), meta);
            }
            Some(x) => return x,
            _ => {}
        }
    }
}


#[cfg(target_feature = "avx2")]
#[doc(hidden)]
pub mod meta_finder {
    use std::ops::Range;
    use std::arch::x86_64::{__m256i, _mm256_set1_epi8, _mm256_movemask_epi8, _mm256_load_si256, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8, _mm256_or_si256};
    pub(super) const META_ALIGNMENT:usize = 32;
    use crate::data_types::{MetaGroup, ProbeRunResult, BucketNr, Meta};

    #[inline]
    pub fn get_any_empty(group: &MetaGroup) -> Option<usize> {
        unsafe {
            let group_reg = _mm256_load_si256(group.0.as_ptr() as *const __m256i);
            let zero_reg = _mm256_set1_epi8(0);
            let res = _mm256_cmpeq_epi8(group_reg, zero_reg);
            let mut bit_res: u32 = _mm256_movemask_epi8(res) as u32;
            if bit_res == 0 {
                return None;
            }
            Some(bit_res.trailing_zeros() as usize)
        }
    }

    // Returns true if empty was encountered
    #[doc(hidden)]
    #[inline]
    pub fn meta_insert_group_find(
        bucket_offset: BucketNr,
        first_deleted: &mut Option<BucketNr>,
        group: &MetaGroup,
        max_index: usize,
        needle: Meta, mut f: impl FnMut(usize) -> bool) -> Option<ProbeRunResult> {

        unsafe {
            let needle_reg = _mm256_set1_epi8(needle.0 as i8);
            let minus1 = _mm256_set1_epi8(-1);
            let mut hay = group.0.as_ptr() as *const __m256i;

            let hay_reg = _mm256_load_si256(hay);
            let cmp_res_needle = _mm256_cmpeq_epi8(needle_reg, hay_reg);
            let cmp_free_or_deleted = _mm256_cmpgt_epi8(hay_reg, minus1);

            let zero_or_needle = _mm256_or_si256(cmp_res_needle, cmp_free_or_deleted);

            let mut bit_res: u32 = _mm256_movemask_epi8(zero_or_needle) as u32;

            let mut temp_pos = 0;
            while bit_res != 0 {
                let next = bit_res.trailing_zeros();
                let index = next as usize + temp_pos;
                if index >= max_index {
                    return None;
                }
                if group.0[index].deleted() && first_deleted.is_none() {
                    *first_deleted = Some(bucket_offset + index);
                }
                else if group.0[index].empty() {
                    return Some(ProbeRunResult::FoundUnoccupied(bucket_offset + index, needle));
                }
                else if f(index) {
                    return Some(ProbeRunResult::FoundPopulated(bucket_offset+index, needle));
                }

                let step = next + 1;
                if step >= 32 {
                    break;
                }
                temp_pos += step as usize;
                bit_res >>= step;
            }

            None
        }
    }

    // Returns true if empty was encountered
    #[doc(hidden)]
    #[inline]
    pub fn meta_get_group_find(haystack: &MetaGroup, max_index:usize, needle: crate::data_types::Meta, mut f: impl FnMut(usize) -> bool) -> bool {
        unsafe {
            let needle = _mm256_set1_epi8(needle.0 as i8);
            let zero = _mm256_set1_epi8(0);
            let mut hay = haystack.0.as_ptr() as *const __m256i;

            let hay_reg = _mm256_load_si256(hay);
            let cmp_res_needle = _mm256_cmpeq_epi8(hay_reg, needle);
            let cmp_res_zero = _mm256_cmpeq_epi8(hay_reg, zero);

            let zero_or_needle = _mm256_or_si256(cmp_res_needle, cmp_res_zero);

            let mut bit_res: u32 = _mm256_movemask_epi8(zero_or_needle) as u32;

            let mut temp_pos = 0;
            while bit_res != 0 {
                let next = bit_res.trailing_zeros();
                let index = next as usize + temp_pos;
                if index >= max_index {
                    return false;
                }
                if haystack.0[index].empty() {
                    return true;
                }
                if f(index) {
                    return true;
                }

                let step = next + 1;
                if step >= 32 {
                    break;
                }
                temp_pos += step as usize;
                bit_res >>= step;
            }

            false
        }
    }
}
#[cfg(not(target_feature = "avx2"))]
#[doc(hidden)]
pub mod meta_finder {
    use crate::data_types::{BucketNr, Meta, MetaGroup, ProbeRunResult};

    #[inline]
    pub fn get_any_empty(group: &MetaGroup) -> Option<usize> {
        group.0.iter().enumerate().find(|x|x.1.empty()).map(|x|x.0)
    }


    /// Returns true and stops iteration if empty node found
    #[doc(hidden)]
    #[inline]
    /// Returns true if end of probe sequence was reached, or if closure returned true.
    pub fn meta_get_group_find(group: &MetaGroup, max_index:usize, needle: Meta, mut f: impl FnMut(usize) -> bool) -> bool {
        for (idx,meta) in group.0.iter().enumerate() {
            if idx >= max_index {
                return false;
            }
            if meta.empty() {
                return true;
            }
            if *meta == needle {
                if f(idx) {
                    return true;
                }
            }
        }
        false
    }

    #[doc(hidden)]
    #[inline]
    /// Closure must check if existing bucket has correct key, and return true if so.
    pub(super) fn meta_insert_group_find(
        group_offset: BucketNr,
        first_deleted: &mut Option<BucketNr>,
        group: &MetaGroup, max_index: usize, needle: Meta, mut f: impl FnMut(usize) -> bool) -> Option<ProbeRunResult> {
        for (idx, meta) in group.0.iter().enumerate() {
            if idx >= max_index {
                return None;
            }
            if first_deleted.is_none() && meta.deleted() {
                *first_deleted = Some(group_offset + idx);
            } else if meta.empty() {
                return Some(ProbeRunResult::FoundUnoccupied(group_offset + idx, needle));
            }
            else if *meta == needle {
                if f(idx) {
                    return Some(ProbeRunResult::FoundPopulated(group_offset + idx, needle));
                }
            }
        }
        None
    }
}




#[cfg(test)]
mod meta_tests {
    use crate::data_types::{meta_finder, Meta, MetaGroup, BucketNr, ProbeRunResult};
    use crate::data_types::meta_finder::get_any_empty;

    #[test]
    fn meta_check_empty() {
        let haystack = MetaGroup([Meta(129u8);32]);
        assert!(get_any_empty(&haystack).is_none());
    }
    #[test]
    fn meta_check_empty2() {
        let mut haystack = MetaGroup([Meta(129u8);32]);
        haystack.0[13] = Meta::EMPTY;
        assert_eq!(get_any_empty(&haystack), Some(13));
    }
    #[test]
    fn meta_get_finds_medium() {
        let mut haystack = MetaGroup([Meta(129u8);32]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[13] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        meta_finder::meta_get_group_find(&haystack, 32, needle, |pos|{found.push(pos);false});
        assert_eq!(found, vec![0,13]);
    }
    #[test]
    fn meta_get_finds_small() {
        let mut haystack = MetaGroup([Meta(129u8);32]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[13] = Meta::new(142u8);
        haystack.0[14] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        meta_finder::meta_get_group_find(&haystack, 14, needle, |pos|{found.push(pos);false});
        assert_eq!(found, vec![0,13]);
    }
    #[test]
    fn meta_insert_medium() {
        let mut haystack = MetaGroup([Meta(129u8);32]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[1] = Meta::DELETED;
        haystack.0[13] = Meta::new(142u8);
        haystack.0[31] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        let mut first_deleted = None;

        let result = meta_finder::meta_insert_group_find(BucketNr(32), &mut first_deleted, &haystack, 31, needle, |pos|{found.push(pos);false});
        assert_eq!(result, None);
        assert_eq!(first_deleted, Some(BucketNr(33)));
        assert_eq!(found, vec![0,13]);
    }

    #[test]
    fn meta_insert2() {
        let mut haystack = MetaGroup([Meta(129u8);32]);
        haystack.0[2] = Meta::DELETED;
        haystack.0[13] = Meta::new(142u8);
        haystack.0[14] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        let mut first_deleted = None;

        let result = meta_finder::meta_insert_group_find(BucketNr(32), &mut first_deleted, &haystack, 32, needle, |pos|{found.push(pos);true});
        assert_eq!(result, Some(ProbeRunResult::FoundPopulated(BucketNr(13+32), needle)));
        assert_eq!(first_deleted, Some(BucketNr(34)));
        assert_eq!(found, vec![13]);
    }
    #[test]
    fn meta_insert3() {
        let mut haystack = MetaGroup([Meta(129u8);32]);
        haystack.0[2] = Meta::DELETED;
        haystack.0[13] = Meta::EMPTY;
        haystack.0[14] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        let mut first_deleted = None;

        let result = meta_finder::meta_insert_group_find(BucketNr(32), &mut first_deleted, &haystack, 32, needle, |pos|{found.push(pos);true});
        assert_eq!(result, Some(ProbeRunResult::FoundUnoccupied(BucketNr(13+32), needle)));
        assert_eq!(first_deleted, Some(BucketNr(34)));
        assert_eq!(found, vec![]);
    }

    #[test]
    fn meta_get_finds_empty() {
        let mut haystack = MetaGroup([Meta::new(129u8);32]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[17] = Meta::new(142u8);
        haystack.0[18] = Meta::EMPTY;
        haystack.0[19] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        meta_finder::meta_get_group_find(&haystack,  32, needle, |pos|{found.push(pos);false});
        assert_eq!(found, vec![0,17]);
    }


}


impl Meta {
    const DELETED: Meta = Meta(1);
    const EMPTY: Meta = Meta(0);
    fn deleted(&self) -> bool {
        self.0 == 1
    }
    fn populated(&self) -> bool {
        self.0 &128 != 0
    }
    fn empty(&self) -> bool {
        self.0 == 0
    }
    #[doc(hidden)]
    pub const fn new(x: u8) -> Meta {
        Meta(x)
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


pub struct DatabaseHashIterator<'a, K:AnyBitPattern+ NoatunKey +PartialEq,V:FixedSizeObject> {
    hash_buckets: &'a [MaybeUninit<DatabaseHashBucket<K,V>>],
    metas: &'a [MetaGroup],
    next_position: usize,
}
struct DatabaseHashOwningIterator<'a, K:AnyBitPattern+ NoatunKey +PartialEq,V:FixedSizeObject> {
    hash_buckets: HashAccessContextMut<'a, K, V>,
    next_position: usize,
}
impl<'a,K: AnyBitPattern+ NoatunKey +PartialEq, V: FixedSizeObject> Iterator for DatabaseHashIterator<'a,K,V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let pos = self.next_position;
            if pos >= self.hash_buckets.len() {
                return None;
            }
            self.next_position += 1;
            let meta = get_meta(self.metas, BucketNr(pos));
            if meta.populated() {
                println!("Bucket {} had populated meta: {:?}", pos, meta);
                let bucket = unsafe { self.hash_buckets[pos].assume_init_ref() };
                return Some((&bucket.key, &bucket.v));
            }
        }

    }
}

impl<'a, K: AnyBitPattern+ NoatunKey +PartialEq, V: FixedSizeObject> Iterator for DatabaseHashOwningIterator<'a, K,V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {

        loop {
            let pos = self.next_position;
            if pos >= self.hash_buckets.buckets.len() {
                return None;
            }
            self.next_position += 1;
            let meta =  get_meta(self.hash_buckets.metas, BucketNr(pos));
            if meta.populated() {
                let bucket = unsafe { self.hash_buckets.buckets[pos].assume_init_mut() };
                let key_p = &mut bucket.key as *mut K;
                let v_p = &mut bucket.v as *mut V;
                return unsafe { Some((
                                key_p.read(),
                                v_p.read(),
                            )) };
            }
        }

    }
}

#[derive(Clone,Copy)]
struct HashAccessContext<'a,K: AnyBitPattern+ NoatunKey +PartialEq, V: FixedSizeObject> {
    metas: &'a [MetaGroup],
    buckets: &'a [MaybeUninit<DatabaseHashBucket<K,V>>],
}
struct HashAccessContextMut<'a,K: AnyBitPattern+ NoatunKey +PartialEq, V: FixedSizeObject> {
    metas: &'a mut [MetaGroup],
    buckets: &'a mut [MaybeUninit<DatabaseHashBucket<K,V>>],
}


fn get_meta_mut(metas: &mut [MetaGroup], bucket: BucketNr) -> &mut Meta {
    let group = bucket.0/32;
    let subindex = bucket.0%32;
    &mut metas[group].0[subindex]
}
fn get_meta(metas: &[MetaGroup], bucket: BucketNr) -> &Meta {
    let group = bucket.0/32;
    let subindex = bucket.0%32;
    &metas[group].0[subindex]
}


enum MetaMutAndEmpty<'a> {
    /// The meta group has all slots occupied
    NoEmpty(&'a mut Meta),
    /// There is an empty slot, and it is precisely after '&Meta'.
    HasEmptyAfterMeta(&'a mut Meta),
    /// There is an empty slot. The slot before the empty slot is `before_empty`
    HasEmpty {
        meta_bucket: BucketNr,
        meta: &'a mut Meta,
        before_empty_bucket: BucketNr,
        before_empty : &'a mut Meta
    }

}

/// Some(x) if the given bucket is part of a group with at least one empty slot.
/// x will be the first such empty slot.
fn get_meta_mut_and_emptyable(metas: &mut [MetaGroup], bucket: BucketNr) -> MetaMutAndEmpty {
    let group = bucket.0/32;
    let subindex = bucket.0%32;
    let group_obj = &mut metas[group];
    group_obj.validate();
    let any_empty = get_any_empty(group_obj);
    match any_empty {
        Some(empty) => {
            if empty == subindex + 1 {
                MetaMutAndEmpty::HasEmptyAfterMeta(&mut group_obj.0[subindex])
            } else {
                debug_assert_ne!(empty, 0);
                debug_assert_ne!(empty, 1);
                debug_assert_ne!(subindex, empty);
                debug_assert_ne!(subindex , empty - 1);
                debug_assert!(subindex < empty-1);
                println!("Group {}, subindex: {}, empty: {}", group, subindex, empty);
                group_obj.validate();
                let [meta, before_empty] = group_obj.0.get_disjoint_mut([subindex, empty-1]).unwrap();
                MetaMutAndEmpty::HasEmpty {
                    meta_bucket: bucket,
                    meta,
                    before_empty_bucket: BucketNr(32*group + empty-1),
                    before_empty
                }
            }
        }
        None => {
            MetaMutAndEmpty::NoEmpty(&mut group_obj.0[subindex])
        }
    }
}


impl<'a,K: AnyBitPattern+ NoatunKey +PartialEq, V: FixedSizeObject> HashAccessContextMut<'a,K,V> {
    fn readonly(&'a self) -> HashAccessContext<'a,K,V> {
        HashAccessContext {
            metas: self.metas,
            buckets: self.buckets,
        }
    }
}

impl<K: AnyBitPattern+ NoatunKey +PartialEq, V: FixedSizeObject> DatabaseHash<K, V> {

    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    pub fn iter(&self) -> DatabaseHashIterator<K,V> {
        let context = self.data_meta_len();
        DatabaseHashIterator {
            hash_buckets: context.buckets,
            metas: context.metas,
            next_position: 0,
        }
    }

    /// Note, this takes ownership of keys+values, but doesn't update self.
    /// 'self' _must_ be overwritten subsequently using `Self::replace_internal`.
    unsafe fn unsafe_into_iter<'a>(&mut self) -> DatabaseHashOwningIterator<'a, K,V> {
        DatabaseHashOwningIterator {
            hash_buckets: self.data_meta_len_mut_unsafe(),
            next_position: 0,
        }
    }
    fn data_meta_len_mut(&mut self) -> HashAccessContextMut< K,V> {
       self.data_meta_len_mut_unsafe()
    }
    fn data_meta_len_mut_unsafe<'a>(&mut self) -> HashAccessContextMut<'a, K,V> {
        let dptr = NoatunContext.start_ptr_mut().wrapping_add(self.data);
        let align = align_of::<DatabaseHashBucket<K,V>>().max(align_of::<MetaGroup>());
        let cap = self.capacity;
        if cap == 0 {
            return HashAccessContextMut {
                metas: &mut [],
                buckets: &mut [],
            };
        }
        let meta_group_count = cap.div_ceil(32);
        let aligned_meta_size = (size_of::<MetaGroup>()*meta_group_count).next_multiple_of(align);

        unsafe {
            let meta_groups = slice::from_raw_parts_mut(dptr as *mut MetaGroup, meta_group_count);
            let buckets = slice::from_raw_parts_mut(dptr.wrapping_add(aligned_meta_size) as *mut MaybeUninit<DatabaseHashBucket<K,V>>, cap);
            HashAccessContextMut {
                metas: meta_groups,
                buckets,
            }
        }
    }
    fn data_meta_len(&self) -> HashAccessContext<K,V> {
        let dptr = NoatunContext.start_ptr().wrapping_add(self.data);
        let align = align_of::<DatabaseHashBucket<K,V>>().max(align_of::<MetaGroup>());
        let cap = self.capacity;
        if cap == 0 {
            return HashAccessContext {
                metas: &[],
                buckets: &[],
            };
        }
        let meta_group_count = cap.div_ceil(32);
        let aligned_meta_size = (size_of::<MetaGroup>()*meta_group_count).next_multiple_of(align);

        unsafe {
            let meta_groups = slice::from_raw_parts(dptr as *const MetaGroup, meta_group_count);
            let buckets = slice::from_raw_parts(dptr.wrapping_add(aligned_meta_size) as *const MaybeUninit<DatabaseHashBucket<K,V>>, cap);
            HashAccessContext {
                metas: meta_groups,
                buckets,
            }
        }
    }

    /// Probe only useful for reading/updating existing bucket
    fn probe_read(database_context_data: HashAccessContext<K,V>, key: impl Borrow<K::DetachedType>) -> Option<BucketNr> {
        let key = key.borrow();
        if database_context_data.buckets.is_empty() {
            return None;
        }
        let HashAccessContext { metas, buckets } = database_context_data;
        let mut h = NoatunHasher::new();
        K::hash(key, &mut h);
        let cap = buckets.len();
        let (group_nr, key_meta) = MetaGroupNr::from_u64(h.finish(), cap);

        let mut result = None;
        //println!("Group-nr: {:?}, key_meta: {:?}", group_nr, key_meta);
        let probe = BucketProbeSequence::new(group_nr, cap.div_ceil(32));

        run_get_probe_sequence(metas, cap, key_meta, |bucket_nr|{
            //println!("Checking key for bucket {:?}", bucket_nr);
            if <K as NoatunKey>::eq(unsafe { buckets[bucket_nr.0].assume_init_ref().key.detach_key_ref() }, key) {
                result = Some(bucket_nr);
                true
            } else {
                false
            }
        }, probe);

        result


        /*
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
        }*/
    }


    /// General purpose bucket probe
    fn probe(context: HashAccessContext<K,V>, key: impl Borrow<K::DetachedType>) -> ProbeRunResult {
        let HashAccessContext { metas, buckets } = context;
        let cap = buckets.len();
        if cap ==  0 {
            return ProbeRunResult::HashFull;
        }
        let mut h = NoatunHasher::new();
        let key = key.borrow();
        K::hash(key, &mut h);

        let (meta_group_nr, key_meta) = MetaGroupNr::from_u64(h.finish(), cap);
        //let mut visited_buckets = 0;
        //let max_visited_buckets = self.capacity/2;
        //let mut first_deleted = None;
        let probe = BucketProbeSequence::new(meta_group_nr, cap.div_ceil(32));

        run_insert_probe_sequence(metas, cap, key_meta, |bucket_nr|{
            unsafe{<K as NoatunKey>::eq(buckets[bucket_nr.0].assume_init_ref().key.detach_key_ref(), key)}
        }, probe)

        /*

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
        }*/
    }
    fn next_suitable_capacity(capacity: usize) -> usize {
        if capacity == 0 {
            return 8;
        }
        if capacity < 8 {
            return 16;
        }
        if capacity < 16 {
            return 32;
        }
        static PRIMES:&[usize] =&[
            1,
            3,
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
        let group_count = capacity.div_ceil(32);
        let (Ok(x)|Err(x)) = PRIMES.binary_search(&group_count);
        32*PRIMES[x.min(PRIMES.len()-1)]
    }
    pub fn get(&self, key: impl Borrow<K::DetachedType>) -> Option<&V> {
        let context = self.data_meta_len();
        let bucket = Self::probe_read(context, key)?;
        unsafe {
            Some(&context.buckets[bucket.0].assume_init_ref().v)
        }
    }


    /// Return true if a value was removed
    pub fn remove(&mut self, key: impl Borrow<K::DetachedType>) -> bool {
        let context = self.data_meta_len_mut();
        let Some(bucket) = Self::probe_read(context.readonly(), key) else {
            return false;
        };


        unsafe {
            let val = Pin::new_unchecked(&mut context.buckets[bucket.0].assume_init_mut().v);
            val.clear();
        };

        match get_meta_mut_and_emptyable(context.metas, bucket) {
            MetaMutAndEmpty::NoEmpty(meta) => {
                println!("No empty found");
                NoatunContext.write_pod_internal(Meta::DELETED, meta);
            }
            MetaMutAndEmpty::HasEmptyAfterMeta(meta) => {
                println!("empty just after meta");
                NoatunContext.write_pod_internal(Meta::EMPTY, meta);
            }
            MetaMutAndEmpty::HasEmpty { meta_bucket, meta, before_empty_bucket, before_empty } => {
                println!("has empty");

                NoatunContext.copy_pod(before_empty, meta);
                NoatunContext.write_pod_internal(Meta::EMPTY, before_empty);

                let [meta_bucket_obj, before_empty_bucket_obj] = context.buckets.get_disjoint_mut([meta_bucket.0, before_empty_bucket.0]).unwrap();

                unsafe {
                    NoatunContext.copy_any(before_empty_bucket_obj.assume_init_ref(), meta_bucket_obj);
                }
            }
        }

        println!("Metas now: {:?}", context.metas);

        #[cfg(debug_assertions)]
        {
            for group in context.metas {
                group.validate();
            }
        }

        let new_length = self.length - 1;
        NoatunContext.write_pod_internal(new_length, &mut self.length);


        true
    }

    pub fn insert(&mut self, key: impl Borrow<K::DetachedType>, val: &V::DetachedType) {
        let key = key.borrow();
        let context = self.data_meta_len_mut();
        let probe_result = Self::probe(context.readonly(), key);
        match probe_result {
            ProbeRunResult::HashFull => {
                self.internal_change_capacity(
                    self.capacity + 2
                );
                // Will not give infinite recursion, since 'new' has a capacity of at least 2 more
                self.insert(key, val);
            }
            ProbeRunResult::FoundUnoccupied(bucket, meta)| //TODO: We _could_ use the knowledge that this is unoccupied, to avoid the zero-check in write_pod
            ProbeRunResult::FoundPopulated(bucket, meta) => {
                let bucket_obj = unsafe { context.buckets[bucket.0].assume_init_mut() };
                let old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };
                V::init_from_detached(old_v, val);
                if matches!(probe_result, ProbeRunResult::FoundUnoccupied(_, _)) {
                    let bucket_meta = get_meta_mut(context.metas, bucket);
                    NoatunContext.write_pod_internal(meta, bucket_meta);
                    let old_k= unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
                    old_k.init_from_detached(key);
                    let new_length = self.length + 1;
                    NoatunContext.write_pod_internal(new_length, &mut self.length);
                }
            }
        }
    }
    fn insert_impl(&mut self, key: K, val: V) {
        let context = self.data_meta_len_mut();
        let probe_result = Self::probe(context.readonly(), key.detach_key_ref());
        match probe_result {
            ProbeRunResult::HashFull => {
                self.internal_change_capacity(
                    self.capacity + 2
                );
                // Will not give infinite recursion, since 'new' has a capacity of at least 2 more
                self.insert_impl(key, val);
            }
            ProbeRunResult::FoundUnoccupied(bucket, meta)| //TODO: We _could_ use the knowledge that this is unoccupied, to avoid the zero-check in write_pod
            ProbeRunResult::FoundPopulated(bucket, meta) => {
                let bucket_obj = unsafe { context.buckets[bucket.0].assume_init_mut()};
                let old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };
                NoatunContext.write_any(val, old_v);
                let bucket_meta = get_meta_mut(context.metas, bucket);
                NoatunContext.write_pod_internal(meta, bucket_meta);
                if matches!(probe_result, ProbeRunResult::FoundUnoccupied(_, _)) {
                    let old_k= unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
                    NoatunContext.write_any(key, old_k);

                    let new_length = self.length + 1;
                    NoatunContext.write_pod_internal(new_length, &mut self.length);
                }
            }
        }
    }

    /// WARNING! This returns a non-pinned DatabaseHash instance that is not in
    /// the noatun db. This is a fickle thing, most operations on it yield garbage.
    fn initialize_with_capacity(&mut self, capacity: usize) {

        let meta_size = capacity.div_ceil(32); //TODO: Introduce constant

        let align = align_of::<DatabaseHashBucket<K,V>>().max(align_of::<MetaGroup>());
        let aligned_meta_size = (32*meta_size).next_multiple_of(align);
        let bucket_data_size = capacity * size_of::<DatabaseHashBucket<K,V>>();

        let data = NoatunContext.allocate_raw(aligned_meta_size + bucket_data_size, align);

        assert_eq!(data as usize % align , 0);


        let new = Self {
            length: 0,
            capacity,
            data: NoatunContext.index_of_ptr(data).0,
            phantom_data: PhantomData,
        };
        NoatunContext.write_pod(new, unsafe { Pin::new_unchecked(self) });
    }

    // This does not write the handle itself into the noatun database!!
    fn internal_change_capacity(&mut self, new_min_capacity: usize) {
        let i = unsafe { self.unsafe_into_iter() };
        let capacity = i.size_hint().0;
        let new_capacity = Self::next_suitable_capacity(capacity.max(new_min_capacity));
        debug_assert!(new_capacity >= new_min_capacity);
        debug_assert!(new_capacity >= capacity);

        self.initialize_with_capacity(new_capacity);

        for (item_key, item_val) in i {
            self.insert_impl(item_key, item_val);
        }
    }
}

/// This type represents an object that can be used as key into an NoatunHashmap.
/// Its functionality overlaps that of [`std::hash::Hash`]. However, it
/// offers guarantees not offered by said standard trait.
///
/// Specifically, implementations of [`NoatunKey`] for a specific type T must
/// always yield the same hash values. This guarantee must hold across program invocations,
/// across different machines and architectures.
///
/// The regular rust ecosystem generally does not offer this guarantee, even when using
/// something like `rustc-hash` `FxHash` or other stable hash implementation, for two reasons
///
/// 1) `rustc-hash` never explicitly guarantees that new versions will yield exactly the same values.
/// 2) Types which implement `Hash` may not always guarantee that future implementations return
///    the same values
///
/// For these reasons, noatun requires users to implement `NoatunHash` for all types used as hash
/// keys. If you know that the underlying `Hash` implementation is actually stable, you can of
/// course just forward to such an impl.
///
/// Also, the bit pattern must be stable.
/// TODO: This (and Object), should probably be unsafe traits. Soundness depends on the bit
/// representations being stable across recompilations/versions.
pub trait NoatunKey : AnyBitPattern + Sized + Debug {
    /// A 'detached' variant of Self.
    ///
    /// Detached types are meant to be ergonomic to work with, but may have representations
    /// that do not allow them to be stored in the mmap:ed db. For example, the detached
    /// type for `NoatunString` is simply `str`.
    ///
    /// For POD types without internal pointers, the detached version should typically be just
    /// `&'a Self`.
    type DetachedType:?Sized;
    type DetachedOwnedType: Eq + Hash + Borrow<Self::DetachedType>;

    fn hash<H>(tself: &Self::DetachedType, state: &mut H)
        where H: Hasher;

    /// Return a reference to a detached key. This method should be fast, usually
    /// just returning a reference to something in memory.
    fn detach_key_ref(&self) -> &Self::DetachedType;

    fn detach_key(&self) -> Self::DetachedOwnedType;

    fn eq(a: &Self::DetachedType, b: &Self::DetachedType) -> bool;

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType);
}



impl<K:AnyBitPattern+ NoatunKey +Hash+Eq,V:FixedSizeObject> Object for DatabaseHash<K,V> {
    type Ptr = ThinPtr;
    type DetachedType = HashMap<K::DetachedOwnedType,V::DetachedOwnedType>;
    type DetachedOwnedType = HashMap<K::DetachedOwnedType,V::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter().map(|(k,v)| (k.detach_key(),v.detach())).collect()
    }

    fn clear(self: Pin<&mut Self>) {
        let length = unsafe  { self.map_unchecked_mut(|x|&mut x.length) };
        NoatunContext.write_pod(0, length);
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        let tself = unsafe  { self.get_unchecked_mut() };
        for (k,v) in detached {
            tself.insert(k.borrow(), v.borrow());
        }
    }

    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        todo!()
    }

    unsafe fn access<'a>(_index: Self::Ptr) -> &'a Self {
        todo!()
    }

    unsafe fn access_mut<'a>(_index: Self::Ptr) -> Pin<&'a mut Self> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use datetime_literal::datetime;
    use super::{DatabaseHash, NoatunString};
    use crate::{CutOffDuration, Database, DatabaseCell, NoatunTime, Object};
    use crate::tests::{DummyTestApp};
    use std::time::Instant;
    use std::hint::black_box;
    use std::pin::Pin;
    use crate::database::DatabaseSettings;
    use crate::tests::DummyTestMessageApply;

    impl DummyTestMessageApply for DatabaseHash<u32, DatabaseCell<u32>> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            let x = (time.0 % (1u64<<32)) as u32;
            root.insert(x, &x);
        }
    }
    impl DummyTestMessageApply for DatabaseHash<NoatunString, DatabaseCell<u32>> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            let x = (time.0 % (1u64<<32)) as u32;
            root.insert(x.to_string(), &x);
        }
    }
    impl DummyTestMessageApply for DatabaseHash<NoatunString, NoatunString> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            let x = (time.0 % (1u64<<32)) as u32;
            root.insert(x.to_string(), &x.to_string());
        }
    }

    #[test]
    fn test_hashmap_miri0() {
        let mut db: Database<DummyTestApp<DatabaseHash<u32, DatabaseCell<u32>>>> = Database::create_in_memory(
            10000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                mock_time: Some(datetime!(2021-01-01 Z).into()),
                projection_time_limit: None,
                ..DatabaseSettings::default()
            },
            (),
        )
            .unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            map.0.insert(42, &42);

            let val = map.0.get(42).unwrap();
            assert_eq!(val.get(), 42);

            let vals : Vec<u32> = map.0.iter().map(|(k,_)| *k).collect();
            assert_eq!(vals, [42]);

        })
            .unwrap();
    }
    #[test]
    fn test_hashmap_miri_string_keys() {
        let mut db: Database<DummyTestApp<DatabaseHash<NoatunString, DatabaseCell<u32>>>> = Database::create_in_memory(
            10000,
            CutOffDuration::from_minutes(15),
           DatabaseSettings {
               mock_time: Some(datetime!(2021-01-01 Z).into()),
               ..Default::default()
            },
            (),
        )
            .unwrap();
        db.with_root_mut(|mut map| {
            map.0.insert("hello", &42);
            assert_eq!(map.0.get("hello").unwrap().value, 42);
            assert!(map.0.remove("hello"));
            assert!(map.0.get("hello").is_none());
        }).unwrap();
    }
    #[test]
    fn test_hashmap_miri_detach() {
        let mut db: Database<DummyTestApp<DatabaseHash<NoatunString, DatabaseCell<u32>>>> = Database::create_in_memory(
            10000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                mock_time: Some(datetime!(2021-01-01 Z).into()),
                ..Default::default()
            },
            (),
        )
            .unwrap();
        db.with_root_mut(|mut map| {
            map.0.insert("hello", &42);

            let reg_map:Vec<_> = map.0.detach().into_iter().collect();
            assert_eq!(&[("hello".to_string(), 42)], &*reg_map);
        }).unwrap();
    }
    #[test]
    fn test_hashmap_miri_detach2() {
        let mut db: Database<DummyTestApp<DatabaseHash<NoatunString, NoatunString>>> = Database::create_in_memory(
            10000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                mock_time: Some(datetime!(2021-01-01 Z).into()),
                ..Default::default()
            },
            ()
        )
            .unwrap();
        db.with_root_mut(|mut map| {
            map.0.insert("hello", "world");

            let reg_map:Vec<_> = map.0.detach().into_iter().collect();
            assert_eq!(&[("hello".to_string(), "world".to_string())], &*reg_map);
            map.0.remove("hello");
            let reg_map:Vec<_> = map.0.detach().into_iter().collect();
            assert!(reg_map.is_empty());


        }).unwrap();
    }
    #[test]
    fn test_hashmap_miri_delete() {
        let mut db: Database<DummyTestApp<DatabaseHash<u32, DatabaseCell<u32>>>> = Database::create_in_memory(
            10000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                mock_time: Some(datetime!(2021-01-01 Z).into()),
                ..Default::default()
            },
            (),
        )
            .unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            map.0.insert(42, &42);

            let val = map.0.get(42).unwrap();
            assert_eq!(val.get(), 42);

            let vals : Vec<u32> = map.0.iter().map(|(k,_)| *k).collect();
            assert_eq!(vals, [42]);

            assert!(!map.0.remove(41), "remove nonexisting key");

            assert!(map.0.remove(42));

            assert_eq!(map.0.iter().count(), 0);
        })
            .unwrap();

    }
    #[test]
    fn test_hashmap_miri_delete_many0() {
        let mut db: Database<DummyTestApp<DatabaseHash<u32, DatabaseCell<u32>>>> = Database::create_in_memory(
            100000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                mock_time: Some(datetime!(2021-01-01 Z).into()),
                ..Default::default()
            },
            (),
        )
            .unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            for i in 0..200 {
                map.0.insert(i, &i);
            }

            for i in 0..200 {
                map.0.get(i).expect("key exists before starting deletes");
            }

            for i in 0..200 {
                assert!(map.0.remove(i));
                for j in i+1 .. 200 {
                    map.0.get(j).expect(&format!("key {} exists after delete of {}", j, i));
                }
            }
        })
            .unwrap();
    }
    #[test]
    fn test_hashmap_miri_insert_many() {
        let mut db: Database<DummyTestApp<DatabaseHash<u32, DatabaseCell<u32>>>> = Database::create_in_memory(
            200000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                mock_time: Some(datetime!(2021-01-01 Z).into()),
                ..Default::default()
            },
            (),
        )
            .unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);
            for i in 0..300 {
                map.0.insert(i,  &i);
            }
            for i in 0..300 {
                let val = map.0.get(i).unwrap();
                assert_eq!(val.get(), i);
            }

        })
            .unwrap();
    }

    #[test]
    fn test_hashmap_lookup_speed_noatun() {
        let mut db: Database<DummyTestApp<DatabaseHash<u32, DatabaseCell<u32>>>> = Database::create_in_memory(
            5000000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                mock_time: Some(datetime!(2021-01-01 Z).into()),
                ..Default::default()
            },
            (),
        )
            .unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);
            for i in 0..10000 {
                map.0.insert(i,  &i);
            }
            let bef = Instant::now();
            for i in 0..10000 {
                let val = map.0.get(black_box(&i)).unwrap();
                black_box(val);
            }
            println!("noatun hashmap: 10000 lookups in {:?}", bef.elapsed());

        })
            .unwrap();
    }
    #[test]
    fn test_hashmap_lookup_speed_std() {
        let mut map = std::collections::HashMap::new();
        for i in 0..10000 {
            map.insert(i,  i);
        }
        let bef = Instant::now();
        for i in 0..10000 {
            let val = map.get(black_box(&i)).unwrap();
            black_box(val);
        }
        println!("std hashmap: 10000 lookups in {:?}", bef.elapsed());
    }


}