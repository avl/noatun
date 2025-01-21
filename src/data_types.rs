use crate::sequence_nr::SequenceNr;
use crate::{
    Database, DatabaseContextData, FatPtr, FixedSizeObject, NoatunContext, Object, Pointer,
    ThinPtr, CONTEXT,
};
use bytemuck::{Pod, Zeroable};
use sha2::digest::typenum::Zero;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem::transmute_copy;
use std::ops::{Deref, Index, Range};
use std::pin::Pin;
use std::ptr::addr_of_mut;

#[derive(Copy, Clone, Debug)]
#[repr(C, packed)]
pub struct DatabaseOption<T: Copy> {
    value: T,
    // TODO: This is needed to have correct alignment
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
            NoatunContext.write_pod(1, &mut self.present);
        } else {
            NoatunContext.write_pod(0, &mut self.present);
        }

        c.update_registrar_ptr(addr_of_mut!(self.registrar), false);
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
unsafe impl<T> Pod for DatabaseOption<T> where T: Pod {}
unsafe impl<T: Copy> Zeroable for DatabaseOption<T> where T: Zeroable {}

#[derive(Copy, Clone)]
#[repr(C, packed)]
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

//TODO: Document. Also rename this or DatabaseCell, the names should harmonize.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct OpaqueCell<T: Copy> {
    value: T,
    registrar: SequenceNr,
}

// TODO: The below (and same for DatabaseCell) are not actually sound. Pod requires
// that there's no padding, but here, there will be padding for align_of<T> > 4.
// There could be padding needed. Solution: We must stop using bytemuck for
// Noatun internals.
unsafe impl<T: Pod> Zeroable for OpaqueCell<T> {}
unsafe impl<T: Pod> Pod for OpaqueCell<T> {}

pub trait DatabaseCellArrayExt<T: Pod> {
    fn observe(&self) -> Vec<T>;
}
impl<T: Pod> DatabaseCellArrayExt<T> for &[DatabaseCell<T>] {
    fn observe(&self) -> Vec<T> {
        //TODO: Rename
        self.iter().map(|x| x.get()).collect()
    }
}

unsafe impl<T> Zeroable for DatabaseCell<T> where T: Pod {}

unsafe impl<T> Pod for DatabaseCell<T> where T: Pod {}
impl<T: Pod> DatabaseCell<T> {
    pub fn get(&self) -> T {
        NoatunContext.observe_registrar(self.registrar);
        self.value
    }
    /*pub fn get_ref(&self, context: &DatabaseContextData) -> &T {
        context.observe_registrar(self.registrar);
        &self.value
    }*/
    pub fn set(&mut self, new_value: T) {
        let c = CONTEXT.get();
        if c.is_null() {
            self.value = new_value;
            return;
            //unreachable!("Attempt to modify DatabaseCell without a mutable context.");
        }
        let c = unsafe { &mut *c };
        let index = c.index_of(self);
        //context.write(index, bytes_of(&new_value));
        c.write_pod_ptr(new_value, addr_of_mut!(self.value));
        c.update_registrar_ptr(addr_of_mut!(self.registrar), false);
    }
}

impl<T: Pod> Object for OpaqueCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;

    unsafe fn init_from_detached(&mut self, detached: Self::DetachedType) {
        self.set(detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: Self::DetachedType) -> &'a mut Self {
        let ret: &mut Self = NoatunContext.allocate_pod();
        ret.init_from_detached(detached);
        ret
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> &'a mut Self {
        unsafe { NoatunContext.access_pod_mut(index) }
    }
}

impl<T: Pod> OpaqueCell<T> {
    pub fn set(&mut self, new_value: T) {
        let index = NoatunContext.index_of(self);
        //context.write(index, bytes_of(&new_value));
        NoatunContext.write_pod(new_value, &mut self.value);
        NoatunContext.update_registrar(&mut self.registrar, true);
    }
}

impl<T: Pod> DatabaseCell<T> {
    #[allow(clippy::mut_from_ref)]
    pub fn allocate<'a>() -> &'a mut Self {
        let memory = unsafe { NoatunContext.allocate_pod::<DatabaseCell<T>>() };
        unsafe { &mut *(memory as *mut _) }
    }
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

    unsafe fn init_from_detached(&mut self, detached: Self::DetachedType) {
        self.set(detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: Self::DetachedType) -> &'a mut Self {
        let ret: &mut Self = NoatunContext.allocate_pod();
        ret.init_from_detached(detached);
        ret
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> &'a mut Self {
        unsafe { NoatunContext.access_pod_mut(index) }
    }
}
#[repr(C)]
pub struct RawDatabaseVec<T> {
    length: usize,
    capacity: usize,
    data: usize,
    phantom_data: PhantomData<T>,
}
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
unsafe impl<T> Pod for RawDatabaseVec<T> where T: 'static {}

impl<T: 'static> RawDatabaseVec<T> {
    fn realloc_add(&mut self, ctx: &DatabaseContextData, new_capacity: usize, new_len: usize) {
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
            self,
        )
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    pub fn grow(&mut self, ctx: &DatabaseContextData, new_length: usize) {
        if new_length <= self.length {
            return;
        }
        if self.capacity < new_length {
            self.realloc_add(ctx, 2 * new_length, new_length);
        } else {
            ctx.write_pod(new_length, &mut self.length);
        }
    }
    #[allow(clippy::mut_from_ref)]
    pub fn allocate(ctx: &DatabaseContextData) -> &mut DatabaseVec<T> {
        unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
    }
}
impl<T: Pod + 'static> RawDatabaseVec<T> {
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
    pub(crate) fn get_mut(&self, ctx: &DatabaseContextData, index: usize) -> &mut T {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        let t = unsafe { ctx.access_pod_mut(ThinPtr(offset)) };
        t
    }
    pub(crate) fn write_untracked(&mut self, ctx: &DatabaseContextData, index: usize, val: T) {
        let offset = self.data + index * size_of::<T>();
        unsafe {
            ctx.write_pod(val, ctx.access_pod_mut(ThinPtr(offset)));
        };
    }
    pub(crate) fn push_untracked(&mut self, ctx: &DatabaseContextData, t: T) -> ThinPtr
    where
        T: Pod,
    {
        if self.length >= self.capacity {
            self.realloc_add(ctx, (self.capacity + 1) * 2, self.length + 1);
        } else {
            ctx.write_pod(self.length + 1, &mut self.length);
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
    type DetachedType = Vec<T::DetachedType>;

    unsafe fn init_from_detached(&mut self, detached: Self::DetachedType) {
        panic!("init_from_detached is not implemented for RawDatabaseVec");
    }
    unsafe fn allocate_from_detached<'a>(detached: Self::DetachedType) -> &'a mut Self {
        panic!("allocate_from_detached is not implemented for RawDatabaseVec");
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }
    unsafe fn access_mut<'a>(index: Self::Ptr) -> &'a mut Self {
        unsafe { NoatunContext.access_pod_mut(index) }
    }
}

#[repr(C, packed)]
pub struct DatabaseVec<T> {
    length: usize,
    capacity: usize,
    data: usize,
    length_registrar: SequenceNr,
    phantom_data: PhantomData<T>,
}

impl<T> Debug for DatabaseVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DatabaseVec({})", { self.length })
    }
}

unsafe impl<T> Zeroable for DatabaseVec<T> {}

impl<T> Copy for DatabaseVec<T> {}

impl<T> Clone for DatabaseVec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T> Pod for DatabaseVec<T> where T: 'static {}

pub struct DatabaseVecIterator<'a, T> {
    vec: &'a DatabaseVec<T>,
    index: usize,
}

pub struct DatabaseVecIteratorMut<'a, T> {
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
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vec.length {
            return None;
        }
        let index = self.index;
        self.index += 1;
        //TODO: Get rid of this transmute. Why is it even neded?
        let vec = unsafe { Pin::new_unchecked(self.vec.as_mut().get_unchecked_mut()) };
        Some(unsafe {
            std::mem::transmute::<&mut T, &mut T>(
                DatabaseVec::getmut(vec, index).get_unchecked_mut(),
            )
        })
    }
}

impl<T: 'static> DatabaseVec<T> {
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
            self,
        )
    }
    #[allow(clippy::mut_from_ref)]
    pub fn new<'a>() -> &'a mut DatabaseVec<T> {
        unsafe { NoatunContext.allocate_pod::<DatabaseVec<T>>() }
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
        unsafe { Pin::new_unchecked(t) }
    }
    fn get_mut_internal(&mut self, index: usize) -> Pin<&mut T> {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        let t = unsafe { T::access_mut(ThinPtr(offset)) };
        unsafe { Pin::new_unchecked(t) }
    }
    pub(crate) fn write(&mut self, index: usize, val: T) {
        let offset = self.data + index * size_of::<T>();
        unsafe {
            let dest = T::access_mut(ThinPtr(offset));
            NoatunContext.write_pod(val, dest);
        };
    }

    pub fn push_zeroed(self: Pin<&mut Self>) -> Pin<&mut T> {
        let tself = unsafe { self.get_unchecked_mut() };
        if tself.length >= tself.capacity {
            tself.realloc_add((tself.capacity + 1) * 2, tself.length + 1);
        } else {
            NoatunContext.write_pod_ptr(tself.length + 1, addr_of_mut!(tself.length));
        }
        NoatunContext.update_registrar(&mut tself.length_registrar, false);
        tself.get_mut_internal(tself.length - 1)
    }

    pub fn shift_remove(&mut self, index: usize) {
        if index >= self.length {
            return;
        }
        if index == self.length - 1 {
            NoatunContext.write_pod_ptr(self.length - 1, addr_of_mut!(self.length));
            return;
        }
        let src_ptr = ThinPtr(self.data + (self.length - 1) * size_of::<T>());
        let dst_ptr = ThinPtr(self.data + index * size_of::<T>());
        NoatunContext.copy(FatPtr::from(src_ptr.0, size_of::<T>()), dst_ptr);
        NoatunContext.write_pod_ptr(self.length - 1, addr_of_mut!(self.length));
    }

    pub fn retain(&mut self, mut f: impl FnMut(&mut T) -> bool) {
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

    pub fn push(mut self: Pin<&mut Self>, t: <T as Object>::DetachedType) {
        self.as_mut().push_zeroed();

        let tself = unsafe { self.get_unchecked_mut() };
        let index = tself.length - 1;
        let offset = ThinPtr(tself.data + index * size_of::<T>());
        unsafe {
            <T as Object>::access_mut(offset).init_from_detached(t);
        }
    }
}

impl<T> Object for DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;
    type DetachedType = Vec<T::DetachedType>;

    unsafe fn init_from_detached(&mut self, detached: Self::DetachedType) {
        for item in detached {
            let new_item = Pin::new_unchecked(&mut *self).push_zeroed();
            let new_item = unsafe { new_item.get_unchecked_mut() };
            new_item.init_from_detached(item);
        }
    }
    unsafe fn allocate_from_detached<'a>(detached: Self::DetachedType) -> &'a mut Self {
        let pod: &mut Self = NoatunContext.allocate_pod();
        pod.init_from_detached(detached);
        pod
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }
    unsafe fn access_mut<'a>(index: Self::Ptr) -> &'a mut Self {
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

unsafe impl<T: Object + ?Sized + 'static> Pod for DatabaseObjectHandle<T> {}
impl<T: Object + ?Sized + 'static> Object for DatabaseObjectHandle<T>
where
    T::Ptr: Pod,
{
    type Ptr = ThinPtr;
    type DetachedType = T::DetachedType;

    unsafe fn init_from_detached(&mut self, detached: Self::DetachedType) {
        let target = T::allocate_from_detached(detached);
        let new_index = NoatunContext.index_of(target);
        NoatunContext.write_pod(new_index, &mut self.object_index);
    }

    unsafe fn allocate_from_detached<'a>(detached: Self::DetachedType) -> &'a mut Self {
        let pod: &mut Self = NoatunContext.allocate_pod();
        pod.init_from_detached(detached);
        pod
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }
    unsafe fn access_mut<'a>(index: Self::Ptr) -> &'a mut Self {
        unsafe { NoatunContext.access_pod_mut(index) }
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
        unsafe { Pin::new_unchecked(T::access_mut(self.object_index)) }
    }

    pub fn new(value: T::Ptr) -> Self {
        Self {
            object_index: value,
            phantom: Default::default(),
        }
    }
    #[allow(clippy::mut_from_ref)]
    pub fn allocate<'a>(value: T) -> &'a mut Self
    where
        T: Object<Ptr = ThinPtr>,
        T: Pod,
    {
        let this = unsafe { NoatunContext.allocate_pod::<DatabaseObjectHandle<T>>() };
        let target = unsafe { NoatunContext.allocate_pod::<T>() };
        *target = value;
        this.object_index = NoatunContext.index_of(target);
        this
    }

    #[allow(clippy::mut_from_ref)]
    pub fn allocate_unsized<'a>(value: &T) -> &'a mut Self
    where
        T: Object<Ptr = FatPtr> + 'static,
    {
        let size_bytes = std::mem::size_of_val(value);
        let this = unsafe { NoatunContext.allocate_pod::<DatabaseObjectHandle<T>>() };
        let target_dst_ptr =
            unsafe { NoatunContext.allocate_raw(size_bytes, std::mem::align_of_val(value)) };

        let target_src_ptr = value as *const T as *const u8;

        let (src_ptr, src_metadata): (*const u8, usize) = unsafe { transmute_copy(&value) };

        unsafe { std::ptr::copy(target_src_ptr, target_dst_ptr, size_bytes) };
        let thin_index = NoatunContext.index_of_ptr(target_dst_ptr);

        this.object_index = FatPtr::from(thin_index.start(), size_bytes);
        this
    }
}
