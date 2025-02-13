use crate::sequence_nr::SequenceNr;
use crate::{
    DatabaseContextData, FatPtr, FixedSizeObject, NoatunContext, Object, Pointer, ThinPtr, CONTEXT,
};
use bytemuck::{AnyBitPattern, Pod, Zeroable};
use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, Range};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::slice;

#[derive(Copy, Clone, Debug, AnyBitPattern)]
#[repr(C)]
pub struct NoatunString {
    start: ThinPtr,
    length: usize,
}

impl Object for NoatunString {
    type Ptr = ThinPtr;
    type DetachedType = str;
    type DetachedOwnedType = String;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.get().to_string()
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
        if self.length == 0 {
            return "";
        }
        let start_ptr = NoatunContext.start_ptr_mut().wrapping_add(self.start.0);
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
        if c.is_null() {
            let tself = unsafe { self.get_unchecked_mut() };
            tself.value = new_value;
            return;
            //unreachable!("Attempt to modify DatabaseCell without a mutable context.");
        }
        let c = unsafe { &mut *c };
        let tself = unsafe { self.get_unchecked_mut() };
        //let _index = c.index_of(tself);
        //context.write(index, bytes_of(&new_value));
        c.write_pod_ptr(new_value, addr_of_mut!(tself.value));
        c.update_registrar_ptr(addr_of_mut!(tself.registrar));
    }
}

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

impl<T: Pod> Object for DatabaseCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;
    type DetachedOwnedType = T;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.value
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
pub struct DatabaseVec<T: FixedSizeObject> {
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
        //TODO: Get rid of this transmute. Why is it even neded?
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
            DatabaseVec {
                length: new_len,
                capacity: new_capacity,
                data: dest_index.0,
                length_registrar: SequenceNr::default(),
                phantom_data: Default::default(),
            },
            unsafe { Pin::new_unchecked(self) },
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
            NoatunContext.write_pod_ptr(tself.length - 1, addr_of_mut!(tself.length));
            return;
        }
        let src_ptr = ThinPtr(tself.data + (tself.length - 1) * size_of::<T>());
        let dst_ptr = ThinPtr(tself.data + index * size_of::<T>());
        NoatunContext.copy(FatPtr::from(src_ptr.0, size_of::<T>()), dst_ptr);

        NoatunContext.write_pod_ptr(tself.length - 1, addr_of_mut!(tself.length));
    }

    pub fn retain(&mut self, mut f: impl FnMut(Pin<&mut T>) -> bool) {
        let mut read_offset = 0;
        let mut write_offset = 0;
        let mut new_len = self.length;

        while read_offset < self.length {
            let read_ptr = ThinPtr(self.data + read_offset * size_of::<T>());
            let val = unsafe { T::access_mut(read_ptr) };
            let retain = f(val);
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
    pub fn get_mut(&mut self) -> Pin<&mut T> {
        if self.object_index.is_null() {
            panic!("get_mut() called on an uninitialized (null) DatabaseObjectHandle.");
        }
        unsafe { T::access_mut(self.object_index) }
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
