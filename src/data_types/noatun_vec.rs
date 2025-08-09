use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::{Index, Range};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::slice;
use crate::{get_context_mut_ptr, FatPtr, FixedSizeObject, NoatunContext, NoatunStorable, Object, Pointer, SchemaHasher, ThinPtr};
use crate::data_types::context::{ContextGetter, ThreadLocalContext};
use crate::projection_store::DatabaseContextData;
use crate::sequence_nr::SequenceNr;

/// Like NoatunVec, but for crate internal use. Does not track
/// accesses. (Does not track dependencies between messages).
#[repr(C)]
pub(crate) struct RawDatabaseVec<T> {
    length: usize,
    capacity: usize,
    data: usize,
    phantom_data: PhantomData<T>,
}

unsafe impl<T: NoatunStorable> NoatunStorable for RawDatabaseVec<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::RawDatabaseVec/1");
        T::hash_schema(hasher);
    }
}

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

/*impl<T:Clone> Clone for RawDatabaseVec<T> {
fn clone(&self) -> Self {
    *self
}
}*/

impl<T> Debug for RawDatabaseVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawDatabaseVec({})", self.length)
    }
}

impl<T: NoatunStorable + 'static> RawDatabaseVec<T> {
    fn realloc_add(&mut self, ctx: &mut DatabaseContextData, new_capacity: usize, new_len: usize) {
        debug_assert!(new_capacity >= new_len);
        debug_assert!(new_capacity >= self.capacity);
        debug_assert!(new_len >= self.length);

        let dest = ctx.allocate_raw(new_capacity * size_of::<T>(), align_of::<T>());
        let dest_index = ctx.index_of_ptr(dest);

        if self.length > 0 {
            let old_ptr = FatPtr::from_idx_count(self.data, size_of::<T>() * self.length);
            // old_ptr is a u8-pointer
            ctx.copy_bytes(old_ptr, dest_index);
        }

        ctx.write_storable(
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
            ctx.write_storable(new_length, unsafe { Pin::new_unchecked(&mut self.length) });
        }
    }
}
impl<T: NoatunStorable + 'static> RawDatabaseVec<T> {
    pub fn retain(&mut self, ctx: &mut DatabaseContextData, mut f: impl FnMut(&mut T) -> bool) {
        let mut read_offset = 0;
        let mut write_offset = 0;
        let mut new_len = self.length;

        while read_offset < self.length {
            let read_ptr = ThinPtr(self.data + read_offset * size_of::<T>());
            let val: Pin<&mut T> = unsafe { ctx.access_storable_mut(read_ptr) };
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
        NoatunContext.write_ptr(new_len, addr_of_mut!(self.length));
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
        unsafe { ctx.access_storable(ThinPtr(offset)) }
    }

    /// SAFETY requirements:
    /// This method gives out mutable references from shared references.
    /// You must make sure to only access every element mutably at most once concurrently.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_mut(&self, ctx: &DatabaseContextData, index: usize) -> Pin<&mut T> {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        let t = unsafe { ctx.access_storable_mut(ThinPtr(offset)) };
        t
    }
    pub(crate) fn swap_remove(&mut self, ctx: &mut DatabaseContextData, index: usize) {
        assert!(index < self.length);
        if index + 1 == self.length {
            ctx.write_storable(index, unsafe { Pin::new_unchecked(&mut self.length) });
        } else {
            let last_index = self.length - 1;
            ctx.write_storable(last_index, unsafe { Pin::new_unchecked(&mut self.length) });

            let src_offset = self.data + last_index * size_of::<T>();
            let src_obj = ThinPtr(src_offset);

            let dst_offset = self.data + index * size_of::<T>();
            let dst_obj = ThinPtr(dst_offset);

            ctx.copy_bytes_len(src_obj, dst_obj, size_of::<T>());
        }
    }
    pub(crate) fn write_untracked(&mut self, ctx: &mut DatabaseContextData, index: usize, val: T) {
        let offset = self.data + index * size_of::<T>();
        unsafe {
            ctx.write_storable(val, ctx.access_storable_mut(ThinPtr(offset)));
        };
    }
    pub(crate) fn pop(&mut self, ctx: &mut DatabaseContextData) -> Option<T>
    where
        T: Clone,
    {
        if self.length == 0 {
            return None;
        }
        let ret = self.get(ctx, self.length - 1).clone();
        ctx.write_storable(self.length - 1, Pin::new(&mut self.length));
        Some(ret)
    }
    pub(crate) fn push_untracked(&mut self, ctx: &mut DatabaseContextData, t: T) -> ThinPtr
    where
        T: NoatunStorable,
    {
        if self.length >= self.capacity {
            self.realloc_add(ctx, (self.capacity + 1) * 2, self.length + 1);
        } else {
            ctx.write_storable(self.length + 1, Pin::new(&mut self.length));
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
        unimplemented!("RawDatabaseVec does not support detach")
    }

    fn destroy(self: Pin<&mut Self>) {
        // The Raw type is special, is isn't tracked
    }

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {
        panic!("init_from_detached is not implemented for RawDatabaseVec");
    }
    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        panic!("allocate_from_detached is not implemented for RawDatabaseVec");
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        Self::hash_schema(hasher);
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
//WARNING! this must be identical to first 3 fields of DatabaseVec
struct DatabaseVecLengthCapData {
    length: usize,
    capacity: usize,
    data: usize,
}

unsafe impl NoatunStorable for DatabaseVecLengthCapData {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::DatabaseVecLengthCapData/1");
    }
}


//TODO(future): Merge with RawDatabaseVec?
/// Untracked vec, for internal use
#[repr(C)]
pub(crate) struct NoatunVecRaw<T: FixedSizeObject, C: ContextGetter> {
    //WARNING! These first 3 fields must be identical to DatabaseVecLengthCapData
    length: usize,
    capacity: usize,
    data: usize,
    phantom_data: PhantomData<(T, C)>,
}

impl<T: FixedSizeObject, C: ContextGetter> Debug for NoatunVecRaw<T, C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoatunVecRaw")
    }
}

impl<T: FixedSizeObject, C: ContextGetter> Default for NoatunVecRaw<T, C> {
    fn default() -> Self {
        Self {
            length: 0,
            capacity: 0,
            data: 0,
            phantom_data: Default::default(),
        }
    }
}

impl<T: FixedSizeObject> NoatunVecRaw<T, ThreadLocalContext> {
    // This only works with thread local context, since it calls destroy, that may
    // reach user overridden code, that will use NoatunContext thread local global directly.
    pub(crate) fn destroy_items(&mut self, ctx: &mut ThreadLocalContext) {
        for i in 0..self.length {
            let mut val = self.get_index_mut_pin(i, ctx);
            val.as_mut().destroy();
            NoatunContext.zero_storable(val);
        }
        let ctx = ctx.get_context_mut();
        unsafe {
            ctx.write_storable(0, Pin::new_unchecked(&mut self.length));
        }
    }
}

impl<T: FixedSizeObject, C: ContextGetter> NoatunVecRaw<T, C> {
    pub(crate) fn len(&self) -> usize {
        self.length
    }

    pub(crate) fn to_vec(&self, ctx: &C) -> Vec<T>
    where
        T: Clone,
    {
        self.iter(ctx).cloned().collect()
    }

    pub(crate) fn iter<'a>(&'a self, c: &'a C) -> NoatunVecIterator<'a, T, C> {
        NoatunVecIterator {
            vec: self,
            context_getter: c,
            index: 0,
        }
    }

    #[inline]
    pub(crate) fn get_index(&self, index: usize, ctx_getter: &C) -> &T {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        unsafe {
            ctx_getter
                .get_context()
                .access_storable::<T>(ThinPtr(offset))
        }
    }

    #[inline]
    pub(crate) fn try_get_index(&self, index: usize, ctx_getter: &C) -> Option<&T> {
        if index >= self.length {
            return None;
        }
        let offset = self.data + index * size_of::<T>();
        Some(unsafe {
            ctx_getter
                .get_context()
                .access_storable::<T>(ThinPtr(offset))
        })
    }

    #[inline]
    pub(crate) fn get_index_mut(&mut self, index: usize, ctx: &mut C) -> &mut T {
        assert!(index < self.length);
        let offset = self.data + index * size_of::<T>();
        let t: &mut T = unsafe { ctx.get_context_mut().access_thin_mut(ThinPtr(offset)) };
        t
    }

    #[inline]
    pub(crate) fn get_index_mut_pin(&mut self, index: usize, ctx: &mut C) -> Pin<&mut T> {
        unsafe { Pin::new_unchecked(self.get_index_mut(index, ctx)) }
    }

    #[inline]
    pub(crate) fn try_get_index_mut(
        &mut self,
        index: usize,
        ctx: &mut DatabaseContextData,
    ) -> Option<&mut T> {
        if index >= self.length {
            return None;
        }
        let offset = self.data + index * size_of::<T>();
        let t = unsafe { ctx.access_thin_mut::<T>(ThinPtr(offset)) };
        Some(t)
    }

    pub(crate) fn write(&mut self, index: usize, val: T, ctx: &mut C) {
        let offset = self.data + index * size_of::<T>();
        unsafe {
            let ctx = ctx.get_context_mut();
            let dest = ctx.access_thin_mut::<T>(ThinPtr(offset));
            ctx.write_storable(val, Pin::new_unchecked(dest));
        };
    }

    /// Doesn't zero memory.
    pub fn clear_fast(&mut self, ctx: &mut DatabaseContextData) {
        unsafe { ctx.write_storable(0, Pin::new_unchecked(&mut self.length)) }
    }

    pub(crate) fn retain(
        &mut self,
        mut f: impl FnMut(Pin<&mut T>) -> bool,
        ctx0: &mut C,
        mut destroy: impl FnMut(Pin<&mut T>),
    ) {
        let mut ctx = ctx0.get_context_mut();

        let mut read_offset = 0;
        let mut new_count = self.length;
        let mut write_offset = 0;

        while read_offset < self.length {
            let read_ptr = ThinPtr(self.data + read_offset * size_of::<T>());
            let mut val = unsafe { Pin::new_unchecked(ctx.access_thin_mut::<T>(read_ptr)) };
            let retain = f(val.as_mut());
            if !retain {
                destroy(val);
                ctx = ctx0.get_context_mut();
                new_count -= 1;
                read_offset += 1;
            } else {
                if read_offset != write_offset {
                    let write_ptr = ThinPtr(self.data + write_offset * size_of::<T>());

                    ctx.copy_bytes_len(read_ptr, write_ptr, size_of::<T>());
                }
                read_offset += 1;
                write_offset += 1;
            }
        }
        assert_eq!(write_offset, new_count);
        self.zero(new_count..self.length, ctx);
        unsafe { ctx.write_storable(new_count, Pin::new_unchecked(&mut self.length)) };
    }

    pub(crate) fn realloc_add(&mut self, new_capacity: usize, new_len: usize, ctx: &mut C) {
        let ctx = ctx.get_context_mut();
        debug_assert!(new_capacity >= new_len);
        debug_assert!(new_capacity >= self.capacity);
        debug_assert!(new_len >= self.length);
        let dest = ctx.allocate_raw(new_capacity * size_of::<T>(), align_of::<T>());

        unsafe {
            let new_slice = slice::from_raw_parts(dest, new_capacity * size_of::<T>());
            assert!(new_slice.iter().all(|x| *x == 0));
        }

        let dest_index = ctx.index_of_ptr(dest);

        if self.length > 0 {
            //bytes
            let old_ptr = FatPtr::from_idx_count(self.data, size_of::<T>() * self.length);
            ctx.copy_bytes(old_ptr, dest_index);
        }

        ctx.write_storable(
            DatabaseVecLengthCapData {
                length: new_len,
                capacity: new_capacity,
                data: dest_index.0,
            },
            unsafe {
                Pin::new_unchecked(std::mem::transmute::<
                    &mut Self,
                    &mut DatabaseVecLengthCapData,
                >(self))
            },
        )
    }

    pub(crate) fn push(&mut self, item: T, ctx: &mut C)
    where
        T: Unpin,
    {
        let place = self.push_zeroed(ctx);
        let ctx = ctx.get_context_mut();
        ctx.write_storable(item, place);
    }

    pub(crate) fn push_zeroed(&mut self, ctx: &mut C) -> Pin<&mut T> {
        if self.length >= self.capacity {
            self.realloc_add((self.capacity + 1) * 2, self.length + 1, ctx);
        } else {
            let ctx = ctx.get_context_mut();
            unsafe {
                ctx.write_storable_ptr(self.length + 1, addr_of_mut!(self.length));
            }
        }

        let obj = self.get_index_mut_pin(self.length - 1, ctx);

        let bytes: &[u8] = crate::bytes_of(&*obj);
        assert!(bytes.iter().all(|x| *x == 0));

        obj
    }

    pub(crate) fn ensure_size(&mut self, at_least: usize, ctx: &mut C) {
        if self.length >= at_least {
            return;
        }

        if at_least > self.capacity {
            self.realloc_add((at_least + 1) * 2, at_least, ctx);
        } else {
            unsafe {
                ctx.get_context_mut()
                    .write_storable_ptr(at_least, addr_of_mut!(self.length));
            }
        }
    }

    pub fn zero(&mut self, range: Range<usize>, ctx: &mut DatabaseContextData) {
        let fat_ptr = FatPtr::from_idx_count(
            self.data + (range.start) * size_of::<T>(),
            (range.end - range.start) * size_of::<T>(),
        );
        ctx.zero(fat_ptr);
    }

    pub(crate) fn swap_remove(
        &mut self,
        index: usize,
        ctx0: &mut C,
        mut destroy: impl FnMut(Pin<&mut T>),
    ) {
        let ctx = ctx0.get_context_mut();
        if index == self.length - 1 {
            unsafe {
                ctx.write_storable_ptr(self.length - 1, addr_of_mut!(self.length));
            }
            let dst_ptr = ThinPtr(self.data + index * size_of::<T>());
            unsafe {
                destroy(Pin::new_unchecked(ctx.access_thin_mut::<T>(dst_ptr)));
            }
            let ctx = ctx0.get_context_mut();
            self.zero(index..index + 1, ctx);
            return;
        }
        let src_ptr = ThinPtr(self.data + (self.length - 1) * size_of::<T>());
        let dst_ptr = ThinPtr(self.data + index * size_of::<T>());
        unsafe {
            destroy(Pin::new_unchecked(ctx.access_thin_mut::<T>(dst_ptr)));
        }
        let ctx = ctx0.get_context_mut();
        ctx.copy_bytes(FatPtr::from_idx_count(src_ptr.0, size_of::<T>()), dst_ptr);
        //NoatunContext.copy_ptr(FatPtr::from_idx_count(src_ptr.0, 1), dst_ptr);

        //NoatunContext.write_ptr(self.length - 1, addr_of_mut!(self.length));
        self.zero(self.length - 1..self.length, ctx);
        unsafe {
            ctx.write_storable_ptr(self.length - 1, addr_of_mut!(self.length));
        }
    }
}

/// Noatun version of Vec.
///
/// Supports inserts, remove and iteration.
///
/// NOTE! This type is mostly available for completeness and because it can be convenient for
/// demos and examples. However, because of the semantics of Noatun, every message that writes
/// or removes elements from a NoatunVec will form a single causal chain. This means none of them
/// can be pruned until the vector is cleared, even if early messages no longer appear to have
/// any impact on the database state.
///
/// In most cases, applications should use [`NoatunHashMap`] or OpaqueNoatunVec
#[repr(C)]
pub struct NoatunVec<T: FixedSizeObject> {
    raw: NoatunVecRaw<T, ThreadLocalContext>,
    length_registrar: SequenceNr,
    clear_registrar: SequenceNr,
    phantom_data: PhantomData<T>,
}

unsafe impl<T: FixedSizeObject> NoatunStorable for NoatunVec<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunVec/1");
        <T as NoatunStorable>::hash_schema(hasher);
    }
}

impl<T: FixedSizeObject + Debug> Debug for NoatunVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

pub struct NoatunVecIterator<'a, T: FixedSizeObject, C: ContextGetter> {
    vec: &'a NoatunVecRaw<T, C>,
    context_getter: &'a C,
    index: usize,
}

pub struct NoatunVecIteratorMut<'a, T: FixedSizeObject> {
    vec: Pin<&'a mut NoatunVec<T>>,
    index: usize,
}

impl<'a, T: FixedSizeObject + 'static, C: ContextGetter> Iterator for NoatunVecIterator<'a, T, C> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vec.length {
            return None;
        }
        let index = self.index;
        self.index += 1;
        Some(self.vec.get_index(index, self.context_getter))
    }
}
impl<'a, T: FixedSizeObject + 'static> Iterator for NoatunVecIteratorMut<'a, T> {
    type Item = Pin<&'a mut T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vec.raw.length {
            return None;
        }
        let index = self.index;
        self.index += 1;
        // Transmute is needed, since the rust typesystem "doesn't know" that all the
        // mutable references to `'a` that we're giving out, are in fact disjoint.
        Some(unsafe {
            Pin::new_unchecked(std::mem::transmute::<&mut T, &mut T>(
                NoatunVec::get_index_mut(self.vec.as_mut(), index).get_unchecked_mut(),
            ))
        })
    }
}

impl<T: FixedSizeObject + 'static> NoatunVec<T> {
    pub fn iter(&self) -> NoatunVecIterator<'_, T, ThreadLocalContext> {
        NoatunContext.observe_registrar(self.length_registrar);
        NoatunVecIterator {
            vec: &self.raw,
            context_getter: &ThreadLocalContext,
            index: 0,
        }
    }
    pub fn iter_mut(self: Pin<&mut Self>) -> NoatunVecIteratorMut<'_, T> {
        NoatunContext.observe_registrar(self.length_registrar);
        NoatunVecIteratorMut {
            vec: self,
            index: 0,
        }
    }
    #[allow(clippy::mut_from_ref)]
    pub fn new<'a>() -> Pin<&'a mut NoatunVec<T>> {
        unsafe { NoatunContext.allocate::<NoatunVec<T>>() }
    }
}

impl<T: FixedSizeObject> Index<usize> for NoatunVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get_index(index)
    }
}

impl<T> NoatunVec<T>
where
    T: FixedSizeObject + 'static,
{
    pub fn len(&self) -> usize {
        NoatunContext.observe_registrar(self.length_registrar);
        self.raw.length
    }
    pub fn is_empty(&self) -> bool {
        NoatunContext.observe_registrar(self.length_registrar);
        self.raw.length == 0
    }
    pub fn get_index(&self, index: usize) -> &T {
        NoatunContext.observe_registrar(self.length_registrar);
        self.raw.get_index(index, &ThreadLocalContext)
    }

    pub fn get_index_mut(self: Pin<&mut Self>, index: usize) -> Pin<&mut T> {
        NoatunContext.observe_registrar(self.length_registrar);
        let tself = unsafe { self.get_unchecked_mut() };
        tself.raw.get_index_mut_pin(index, &mut ThreadLocalContext)
    }
    fn get_mut_internal(&mut self, index: usize) -> Pin<&mut T> {
        self.raw.get_index_mut_pin(index, &mut ThreadLocalContext)
    }
    /*pub(crate) fn write(&mut self, index: usize, val: T) {
        self.raw.write(index, val)
    }*/

    pub fn clear(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        tself.raw.destroy_items(&mut ThreadLocalContext);
        NoatunContext.update_registrar(&mut tself.length_registrar, false);
        NoatunContext.update_registrar(&mut tself.clear_registrar, true);
    }

    pub fn push_zeroed(self: Pin<&mut Self>) -> Pin<&mut T> {
        let tself = unsafe { self.get_unchecked_mut() };
        NoatunContext.observe_registrar(tself.length_registrar);
        NoatunContext.update_registrar(&mut tself.length_registrar, false);
        tself.raw.push_zeroed(&mut ThreadLocalContext)
    }

    pub fn swap_remove(self: Pin<&mut Self>, index: usize) {
        if index >= self.raw.length {
            return;
        }
        let tself = unsafe { self.get_unchecked_mut() };
        NoatunContext.observe_registrar(tself.length_registrar);
        NoatunContext.update_registrar(&mut tself.length_registrar, false);

        tself
            .raw
            .swap_remove(index, &mut ThreadLocalContext, |x| x.destroy());
    }

    pub fn retain(self: Pin<&mut Self>, mut f: impl FnMut(Pin<&mut T>) -> bool) {
        struct PanicHandler<'a, T: FixedSizeObject + 'static> {
            vec: &'a mut NoatunVec<T>,
            new_count: usize,
            read_offset: usize,
            write_offset: usize,
        }
        impl<T: FixedSizeObject + 'static> Drop for PanicHandler<'_, T> {
            fn drop(&mut self) {
                while self.read_offset < self.vec.raw.length {
                    let read_ptr = ThinPtr(self.vec.raw.data + self.read_offset * size_of::<T>());
                    if self.read_offset != self.write_offset {
                        let write_ptr =
                            ThinPtr(self.vec.raw.data + self.write_offset * size_of::<T>());
                        NoatunContext.copy_sized(read_ptr, write_ptr, size_of::<T>());
                    }
                    self.read_offset += 1;
                    self.write_offset += 1;
                }
                let old_count = self.vec.raw.length;
                NoatunContext.write_ptr(self.new_count, addr_of_mut!(self.vec.raw.length));
                self.vec.raw.zero(self.write_offset..old_count, unsafe {
                    &mut *get_context_mut_ptr()
                });
            }
        }

        let self_mut = unsafe { self.get_unchecked_mut() };
        NoatunContext.observe_registrar(self_mut.length_registrar);
        NoatunContext.update_registrar(&mut self_mut.length_registrar, false);
        let mut panic_handler = PanicHandler {
            new_count: self_mut.raw.length,
            read_offset: 0,
            vec: self_mut,
            write_offset: 0,
        };

        while panic_handler.read_offset < panic_handler.vec.raw.length {
            let read_ptr =
                ThinPtr(panic_handler.vec.raw.data + panic_handler.read_offset * size_of::<T>());
            let mut val = unsafe { read_ptr.access_mut::<T>() };
            let retain = f(val.as_mut());
            if !retain {
                val.destroy();
                panic_handler.new_count -= 1;
                panic_handler.read_offset += 1;
            } else {
                if panic_handler.read_offset != panic_handler.write_offset {
                    let write_ptr = ThinPtr(
                        panic_handler.vec.raw.data + panic_handler.write_offset * size_of::<T>(),
                    );
                    NoatunContext.copy_sized(read_ptr, write_ptr, size_of::<T>());
                }
                panic_handler.read_offset += 1;
                panic_handler.write_offset += 1;
            }
        }
    }

    pub fn push(mut self: Pin<&mut Self>, t: impl Borrow<<T as Object>::DetachedType>) {
        self.as_mut().push_zeroed();

        let index = self.raw.length - 1;
        self.get_index_mut(index).init_from_detached(t.borrow());
        /*let offset = ThinPtr(tself.raw.data + index * size_of::<T>());
        unsafe {
            offset.access_mut::<T>().init_from_detached(t.borrow());
        }*/
    }
}

/// Like [`NoatunVec`] but does not support all operations during message application.
///
/// Specifically, it doesn't support iteration, and doesn't have a `len` method.
///
/// This means less meta data needs to be kept, which avoid building subtle dependencies
/// between messages. This means messages can be pruned more effectively.
///
/// An example:
///
/// Adding an item to [`NoatunVec`] using [`NoatunVec::push`] changes the length of the
/// collection. This change is observable, so metadata about the length must be kept.
/// However, the resulting length after [`NoatunVec::push`] depends on the previous length,
/// which in turn depends on all previous push/pop operations. This creates a long
/// dependency chain of messages that cannot be pruned simply because doing so would change
/// the length of the collection.
///
/// [`Self`] does not support iteration or retrieving the length, so pushing a message
/// does not depend on all previous operations.
///
pub struct OpaqueNoatunVec<T: FixedSizeObject> {
    raw: NoatunVecRaw<T, ThreadLocalContext>,
    clear_registrar: SequenceNr,
}

impl<T: FixedSizeObject> Debug for OpaqueNoatunVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OpaqueNoatunVec")
    }
}

unsafe impl<T: FixedSizeObject> NoatunStorable for OpaqueNoatunVec<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::OpaqueNoatunVec/1");
        <T as NoatunStorable>::hash_schema(hasher);
    }
}

impl<T: FixedSizeObject> Index<usize> for OpaqueNoatunVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get_item(index).expect("index out of bounds")
    }
}

impl<T: FixedSizeObject> OpaqueNoatunVec<T> {
    /// Clear all elements of the vector.
    ///
    /// This does not cause any observation of the vector.
    ///
    /// The current message is recorded as the "most recent clearer" of the vector.
    pub fn clear(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        NoatunContext.update_registrar_ptr(addr_of_mut!(tself.clear_registrar), true);
        tself.raw.destroy_items(&mut ThreadLocalContext);
    }

    /// Iterate over the elements of the vector.
    ///
    /// This is not allowed from within [`Message::apply`] (because this is an opaque data-type).
    pub fn iter(&self) -> NoatunVecIterator<'_, T, ThreadLocalContext> {
        NoatunContext.assert_opaque_access_allowed("OpaqueNoatunVec", "NoatunVec");
        NoatunVecIterator {
            vec: &self.raw,
            context_getter: &ThreadLocalContext,
            index: 0,
        }
    }
    /// Get the length of the vector.
    ///
    /// This is not allowed from within [`Message::apply`] (because this is an opaque data-type).
    pub fn len(&self) -> usize {
        NoatunContext.assert_opaque_access_allowed("OpaqueNoatunVec", "NoatunVec");
        self.raw.length
    }
    /// Returns a reference to the element at position 'index' or None if out of bounds.
    ///
    /// This panics if called from within [`Message::apply`]. It must only be used
    /// from within [`Database::with_root`] or similar locations. Opaque data types
    /// are not visible during message application.
    #[inline]
    pub fn get_item(&self, index: usize) -> Option<&T> {
        if index >= self.raw.length {
            return None;
        }
        NoatunContext.assert_opaque_access_allowed("OpaqueNoatunVec", "NoatunVec");
        Some(self.raw.get_index(index, &ThreadLocalContext))
    }

    /// Write the given value to the given index.
    ///
    /// If the vector isn't large enough, it will be extended with zeroed elements until it is.
    ///
    /// Note, this method does not cause any observation of the vector.
    pub fn set_item_infallible(
        self: Pin<&mut Self>,
        index: usize,
        val: impl Borrow<<T as Object>::DetachedType>,
    ) {
        let tself = unsafe { self.get_unchecked_mut() };
        if index >= tself.raw.length {
            let new_length = index + 1;
            if new_length > tself.raw.capacity {
                // Reallocate
                tself
                    .raw
                    .realloc_add((new_length + 1) * 2, new_length, &mut ThreadLocalContext);
            } else {
                // Just increase length
                NoatunContext.write_ptr(new_length, addr_of_mut!(tself.raw.length));
            }
        }
        let offset = ThinPtr(tself.raw.data + index * size_of::<T>());
        unsafe {
            let item_data = offset.access_mut::<T>();
            item_data.init_from_detached(val.borrow());
        }
    }
    pub fn push(self: Pin<&mut Self>, t: impl Borrow<<T as Object>::DetachedType>) {
        let tself = unsafe { self.get_unchecked_mut() };
        tself.raw.push_zeroed(&mut ThreadLocalContext);

        let index = tself.raw.length - 1;
        tself
            .raw
            .get_index_mut_pin(index, &mut ThreadLocalContext)
            .init_from_detached(t.borrow());
    }
}

impl<T> Object for OpaqueNoatunVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;
    type DetachedType = [T::DetachedOwnedType];
    type DetachedOwnedType = Vec<T::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter().map(|x| x.detach()).collect()
    }

    fn destroy(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        NoatunContext.clear_registrar_ptr(addr_of_mut!(tself.clear_registrar), true);
        tself.raw.destroy_items(&mut ThreadLocalContext)
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        let tself = unsafe { self.get_unchecked_mut() };
        use std::borrow::Borrow;
        for item in detached {
            let new_item = tself.raw.push_zeroed(&mut ThreadLocalContext);
            new_item.init_from_detached(item.borrow());
        }
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut pod: Pin<&mut Self> = NoatunContext.allocate();
        pod.as_mut().init_from_detached(detached);
        pod
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        <Self as NoatunStorable>::hash_schema(hasher);
    }
}

impl<T> Object for NoatunVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;
    type DetachedType = [T::DetachedOwnedType];
    type DetachedOwnedType = Vec<T::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter().map(|x| x.detach()).collect()
    }

    fn destroy(self: Pin<&mut Self>) {
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
        let mut pod: Pin<&mut Self> = NoatunContext.allocate();
        pod.as_mut().init_from_detached(detached);
        pod
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        <Self as NoatunStorable>::hash_schema(hasher);
    }
}

