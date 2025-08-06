#![allow(non_local_definitions)]
use crate::data_types::meta_finder::get_any_empty;
use crate::sequence_nr::SequenceNr;
use crate::xxh3_vendored::NoatunHasher;
use crate::{
    get_context_mut_ptr, get_context_ptr, DatabaseContextData, FatPtr, FixedSizeObject, MessageId,
    NoatunContext, NoatunStorable, Object, Pointer, SchemaHasher, ThinPtr, CONTEXT,
};
use savefile_derive::Savefile;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Add, AddAssign, Deref, Index, Range, SubAssign};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::slice;
use tracing::trace;

mod noatun_hash_impls;

#[derive(Copy, Clone)]
#[repr(C)]
pub struct NoatunString {
    start: ThinPtr,
    length: usize,
    registrar: SequenceNr,
    padding: u32,
}

unsafe impl NoatunStorable for NoatunString {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunString/1")
    }
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
        let o: &str = other;
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

    fn destroy(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        NoatunContext.clear_registrar_ptr(&mut tself.registrar, false);
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        self.assign(detached);
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut temp: Pin<&mut Self> = NoatunContext.allocate();
        temp.as_mut().assign(detached);
        temp
    }

    fn hash_object_schema(hasher: &mut SchemaHasher) {
        <Self as NoatunStorable>::hash_schema(hasher);
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
                NoatunContext.write_internal(value.len(), &mut tself.length);
            }
            return;
        }

        let raw = NoatunContext.allocate_raw(value.len(), 1);
        let target = unsafe { slice::from_raw_parts_mut(raw, value.len()) };
        target.copy_from_slice(value.as_bytes());
        let raw_index = NoatunContext.index_of_ptr(raw);
        NoatunContext.write_internal(raw_index, &mut tself.start);
        NoatunContext.write_internal(value.len(), &mut tself.length);
        NoatunContext.update_registrar_ptr(addr_of_mut!(tself.registrar), false);
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

#[derive(Copy, Clone, Debug, Savefile)]
#[repr(C)]
pub struct NoatunOption<T: NoatunStorable> {
    value: T,
    present: u8,
}

unsafe impl<T: NoatunStorable> NoatunStorable for NoatunOption<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunOption/1");
        T::hash_schema(hasher);
    }
}

impl<T: NoatunStorable> NoatunOption<T> {
    pub fn into_option(self) -> Option<T> {
        self.into()
    }
    pub fn is_some(&self) -> bool {
        self.present == 1
    }
    pub fn is_none(&self) -> bool {
        self.present == 0
    }
}

impl<T: NoatunStorable> From<Option<T>> for NoatunOption<T> {
    fn from(value: Option<T>) -> Self {
        match value {
            None => Self {
                value: T::zeroed(),
                present: 0,
            },
            Some(t) => Self {
                value: t,
                present: 1,
            },
        }
    }
}
impl<T: NoatunStorable> From<NoatunOption<T>> for Option<T> {
    fn from(value: NoatunOption<T>) -> Self {
        if value.present == 0 {
            None
        } else {
            Some(value.value)
        }
    }
}

/// A wrapper around a regular plain old data type. T must implement
/// `NoatunStorable`, which basically means it must be a Copy type that does
/// not contain any pointers.
#[repr(C)]
pub struct NoatunCell<T> {
    value: T,
    /// The most recent message that did a write to this cell
    registrar: SequenceNr,
}

#[repr(C)]
pub struct OpaqueNoatunCell<T> {
    value: T,
    /// The most recent message that did a write to this cell
    registrar: SequenceNr,
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

/*impl<T: NoatunStorable> NoatunCell<T> {
    pub fn new(value: T) -> NoatunCell<T> {
        NoatunCell {
            value,
            registrar: Default::default(),
        }
    }
}*/

impl<T: NoatunStorable + Debug + Copy> Object for NoatunCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;
    type DetachedOwnedType = T;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.value
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
        <Self as NoatunStorable>::hash_schema(hasher);
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
        <Self as NoatunStorable>::hash_schema(hasher);
    }
}

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
        <Self as NoatunStorable>::hash_schema(hasher);
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

#[repr(transparent)]
pub(crate) struct NoatunUntrackedCell<T: NoatunStorable>(pub(crate) T);

impl<T: NoatunStorable + Clone> Clone for NoatunUntrackedCell<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: NoatunStorable + Copy> Copy for NoatunUntrackedCell<T> {}

impl<T: NoatunStorable + Debug> Debug for NoatunUntrackedCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoatunUntrackedCell({:?})", self.0)
    }
}

unsafe impl<T: NoatunStorable> NoatunStorable for NoatunUntrackedCell<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunUntrackedCell/1");
        T::hash_schema(hasher);
    }
}

impl<T: NoatunStorable> Deref for NoatunUntrackedCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: NoatunStorable + Copy> Object for NoatunUntrackedCell<T> {
    type Ptr = ThinPtr;
    type DetachedType = T;
    type DetachedOwnedType = T;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.0
    }

    fn destroy(self: Pin<&mut Self>) {}

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        unsafe {
            let value = self.get_unchecked_mut();
            *value = NoatunUntrackedCell(*detached);
        }
    }

    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        unimplemented!("NoatunUntrackedCell does not support heap allocation")
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        <Self as NoatunStorable>::hash_schema(hasher);
    }
}

mod context {
    use crate::projection_store::DatabaseContextData;

    pub struct ThreadLocalContext;
    pub trait ContextGetter {
        fn get_context(&self) -> &DatabaseContextData;
        fn get_context_mut(&mut self) -> &mut DatabaseContextData;
    }
}
use context::{ContextGetter, ThreadLocalContext};

impl ContextGetter for ThreadLocalContext {
    fn get_context(&self) -> &DatabaseContextData {
        let context_ptr = get_context_ptr();
        unsafe { &*context_ptr }
    }

    fn get_context_mut(&mut self) -> &mut DatabaseContextData {
        let context_ptr = get_context_mut_ptr();
        unsafe { &mut *context_ptr }
    }
}
impl ContextGetter for DatabaseContextData {
    fn get_context(&self) -> &DatabaseContextData {
        self
    }

    fn get_context_mut(&mut self) -> &mut DatabaseContextData {
        self
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
                //dst_ptr.access_mut::<T>().destroy();
            }
            let ctx = ctx0.get_context_mut();
            self.zero(index..index + 1, ctx);
            return;
        }
        let src_ptr = ThinPtr(self.data + (self.length - 1) * size_of::<T>());
        let dst_ptr = ThinPtr(self.data + index * size_of::<T>());
        unsafe {
            destroy(Pin::new_unchecked(ctx.access_thin_mut::<T>(dst_ptr)));
            //dst_ptr.access_mut::<T>().destroy();
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

#[repr(transparent)]
pub struct NoatunBox<T: Object + ?Sized> {
    object_index: T::Ptr,
    phantom: PhantomData<T>,
}

unsafe impl<T: Object + ?Sized + 'static> NoatunStorable for NoatunBox<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunBox/1");
        T::hash_object_schema(hasher);
    }
}

impl<T: Object + ?Sized> Copy for NoatunBox<T> {}

impl<T: Object + ?Sized> Clone for NoatunBox<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Object + ?Sized + 'static> Object for NoatunBox<T>
where
    T::Ptr: NoatunStorable,
{
    type Ptr = ThinPtr;
    type DetachedType = T::DetachedType;
    type DetachedOwnedType = T::DetachedOwnedType;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.get_inner().detach()
    }

    fn destroy(self: Pin<&mut Self>) {
        Self::get_inner_mut(self).destroy()
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        unsafe {
            let target = T::allocate_from_detached(detached);
            let new_index = NoatunContext.index_of(&*target);
            NoatunContext.write(
                new_index,
                Pin::new_unchecked(&mut self.get_unchecked_mut().object_index),
            );
        }
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut pod: Pin<&mut Self> = NoatunContext.allocate_obj();
        pod.as_mut().init_from_detached(detached);
        pod
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        <Self as NoatunStorable>::hash_schema(hasher);
    }
}

impl<T: Object + ?Sized> Deref for NoatunBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        if self.object_index.is_null() {
            panic!("get() called on an uninitialized (null) NoatunBox.");
        }
        unsafe { self.object_index.access::<T>() }
    }
}

impl<T: Object + ?Sized> NoatunBox<T> {
    /// Synonym of `deref`. You usually don't have to call this.
    pub fn get_inner(&self) -> &T {
        if self.object_index.is_null() {
            panic!("get() called on an uninitialized (null) NoatunBox.");
        }
        unsafe { self.object_index.access::<T>() }
    }

    /// This could have been a `DerefMut` impl, but unfortunately that doesn't work
    /// with a Pin self
    pub fn get_inner_mut(self: Pin<&mut Self>) -> Pin<&mut T> {
        let tself = unsafe { self.get_unchecked_mut() };
        if tself.object_index.is_null() {
            panic!("get_mut() called on an uninitialized (null) NoatunBox.");
        }
        unsafe { tself.object_index.access_mut::<T>() }
    }

    pub fn new(value: T::Ptr) -> Self {
        Self {
            object_index: value,
            phantom: Default::default(),
        }
    }

    pub fn assign(self: Pin<&mut Self>, value: &T::DetachedType) {
        let tself = unsafe { self.get_unchecked_mut() };
        let target = unsafe { T::allocate_from_detached(value) };
        let index = NoatunContext.index_of(&*target);
        NoatunContext.write_internal(index, &mut tself.object_index);
    }

    #[allow(clippy::mut_from_ref)]
    pub unsafe fn allocate<'a>(value: T) -> Pin<&'a mut Self>
    where
        T: Object<Ptr = ThinPtr>,
        T: NoatunStorable,
    {
        let mut this = NoatunContext.allocate::<NoatunBox<T>>();
        let mut target = NoatunContext.allocate::<T>();
        unsafe {
            *target.as_mut().get_unchecked_mut() = value;
        }
        unsafe {
            this.as_mut().get_unchecked_mut().object_index = NoatunContext.index_of(&*target);
        }
        this
    }

    /*#[allow(clippy::mut_from_ref)]
    pub unsafe fn allocate_unsized<'a>(value: &T) -> Pin<&'a mut Self>
    where
        T: Object<Ptr = FatPtr> + 'static,
    {
        let size_bytes = std::mem::size_of_val(value);
        let mut this = NoatunContext.allocate::<NoatunBox<T>>();
        let target_dst_ptr = NoatunContext.allocate_raw(size_bytes, std::mem::align_of_val(value));

        let target_src_ptr = value as *const T as *const u8;

        //let (src_ptr, src_metadata): (*const u8, usize) = unsafe { transmute_copy(&value) };

        unsafe { std::ptr::copy(target_src_ptr, target_dst_ptr, size_bytes) };
        let thin_index = NoatunContext.index_of_ptr(target_dst_ptr);

        unsafe {
            this.as_mut().get_unchecked_mut().object_index =
                FatPtr::from_idx_count(thin_index.start(), count)
        };
        this
    }*/
}

const HASH_META_GROUP_SIZE: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
struct NoatunHashBucket<K, V> {
    hash: u32,
    key: K,
    v: V,
}

unsafe impl<K: NoatunStorable, V: NoatunStorable> NoatunStorable for NoatunHashBucket<K, V> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunHashBucket/1");
        K::hash_schema(hasher);
        V::hash_schema(hasher);
    }
}

/// A collection very similar to std HashMap, for Noatun databases.
///
/// This is expected to be the primary collection type used by Noatun applications.
///
/// # Note regarding [`NoatunHashMap::len`].
/// The len method is not available during message application. The reason is that
/// the value of len logically depends on all previous inserts and removes. Each message
/// calling len would thus gain a dependency on a (potentially) very large number of messages.
/// Also, this set is not presently tracked by Noatun.
///
/// NoatunHashMap still does expose [`NoatunHashMap::iter`] which, of course, can be iterated
/// and the length of the hashmap thus calculated anyway. Doing this does not cause unsafety,
/// but may cause unexpected results. Consider the following situation with three messages:
///
/// T=1: Key 'A' is inserted to hashmap
///
/// T=2: Hashmap is iterated over, and the calculated length (=1) is stored into field X.
///
/// T=3: Key 'A' is removed from hashmap
///
/// Since the iteration does not cause an observation to be recorded, the fact that the
/// hashmap length was observed at T=2 is lost. This means that the first message will be
/// pruned when the last message is inserted. The final value of field X will thus be 0,
/// not 1.
///
/// Let's see what happens if we don't iterate, but instead check for presence of the key 'A':
///
/// T=1: Key 'A' is inserted to hashmap
///
/// T=2: [`NoatunHashMap::contains_key`] is called with parameter 'A' and the presence of 'A'  is stored
/// into field X.
///
/// T=3: Key 'A' is removed from hashmap
///
/// In this case, at T=2 Noatun will record the dependency by the second message on the first
/// message. The first message will not be pruned when 'A' is removed from the hashmap. Not until
/// the field X is overwritten (without dependency on the previous value) will the first message
/// be pruned.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NoatunHashMap<K: NoatunStorable, V: FixedSizeObject> {
    length: usize,
    capacity: usize,
    data: usize,

    // By having a registrar for any "clear" calls, we can avoid
    // accumulating multiple "tombstone" messages when clearing hashmaps.
    // The downside is that each hashmap retains a clearing message
    // even _after_ the cutoff (since it remains in the db).
    // Observes any "clear" calls. This allows us to not register
    // such actions as 'tombstones'.
    clear_registrar: SequenceNr,

    phantom_data: PhantomData<(K, V)>,
}

unsafe impl<K: NoatunStorable, V: FixedSizeObject> NoatunStorable for NoatunHashMap<K, V> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunHashMap/1");
        K::hash_schema(hasher);
        <V as NoatunStorable>::hash_schema(hasher);
    }
}

impl<K: NoatunStorable + NoatunKey + PartialEq + Debug, V: FixedSizeObject + Debug> Debug
    for NoatunHashMap<K, V>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

// Some of the stuff here is public doc(hidden) just so that a benchmark can get at it
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BucketNr(pub usize);

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetaGroupNr(pub usize);

impl MetaGroup {
    #[inline]
    fn validate(&self) {
        #[cfg(debug_assertions)]
        {
            let first_empty = self.0.iter().position(|x| x.empty());
            if let Some(first_empty) = first_empty {
                for i in 0..HASH_META_GROUP_SIZE {
                    assert!(!self.0[i].deleted());
                }
                for i in first_empty..HASH_META_GROUP_SIZE {
                    assert!(self.0[i].empty());
                }
            }
        }
    }
}

impl MetaGroupNr {
    fn from_u64(x: u64, cap: usize) -> (MetaGroupNr, Meta) {
        let groupcount = cap.div_ceil(HASH_META_GROUP_SIZE);
        (
            MetaGroupNr((x as usize) % groupcount),
            Meta(((x as usize) / groupcount) as u8 | 128),
        )
    }
}

impl BucketNr {
    fn from_u64(x: u64, cap: usize) -> (BucketNr, Meta) {
        (
            BucketNr((x as usize) % cap),
            Meta(((x as usize) / cap) as u8 | 128),
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

#[derive(Clone, Copy)]
pub struct BucketProbeSequence {
    cur_group: usize,
    group_capacity: usize,
    groups_visited: usize,
    group_step: usize,
}

impl BucketProbeSequence {
    pub fn new(start_group: MetaGroupNr, total_group_count: usize) -> BucketProbeSequence {
        BucketProbeSequence {
            cur_group: start_group.0,
            group_capacity: total_group_count,
            groups_visited: start_group.0,
            group_step: 1,
        }
    }
    pub fn probe_next(&mut self) -> Option<MetaGroupNr> {
        // step is >= capacity after approx self.capacity/2 iterations, since
        // step is incremented by 2
        if self.group_step > self.group_capacity {
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
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
#[doc(hidden)]
pub struct Meta(u8);

unsafe impl NoatunStorable for Meta {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::data_types::Meta/1");
    }
}

#[repr(align(32))]
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct MetaGroup(pub [Meta; HASH_META_GROUP_SIZE]);

enum ProbeResult {
    Found,
    NotFound,
    Full,
}

/// Returns when end of probe sequence was reached, or when closure returns true.
#[doc(hidden)]
pub fn run_get_probe_sequence(
    metas: &[MetaGroup],
    max_buckets: usize,
    needle: Meta,
    mut f: impl FnMut(BucketNr) -> bool,
    mut probe: BucketProbeSequence,
) {
    loop {
        let Some(group_index) = probe.probe_next() else {
            return;
        };
        let group = &metas[group_index.0];

        let bucket_offset = BucketNr(group_index.0 * HASH_META_GROUP_SIZE);
        if meta_finder::meta_get_group_find(
            group,
            if group_index.0 + 1 == metas.len() {
                max_buckets - HASH_META_GROUP_SIZE * group_index.0
            } else {
                HASH_META_GROUP_SIZE
            },
            needle,
            |index| f(bucket_offset + index),
        ) {
            return;
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ProbeRunResult {
    HashFull,
    /// Found a bucket with the given key, with a value present
    FoundPopulated(BucketNr, Meta),
    /// Found either an empty, or deleted
    FoundUnoccupied(BucketNr, Meta),
}

impl ProbeRunResult {
    fn bucket_meta(&self) -> (BucketNr, Meta) {
        match self {
            ProbeRunResult::HashFull => {
                panic!("unexpected error, HashFull")
            }
            ProbeRunResult::FoundPopulated(bucket, meta)
            | ProbeRunResult::FoundUnoccupied(bucket, meta) => (*bucket, *meta),
        }
    }
}

/// Closure should return true iff bucket with given index has the key we're probing for.
#[doc(hidden)]
pub fn run_insert_probe_sequence(
    metas: &[MetaGroup],
    max_buckets: usize,
    needle: Meta,
    mut f: impl FnMut(BucketNr) -> bool,
    mut probe: BucketProbeSequence,
) -> ProbeRunResult {
    let mut first_deleted = None;
    loop {
        let Some(group_index) = probe.probe_next() else {
            return ProbeRunResult::HashFull;
        };
        let bucket_offset = BucketNr(HASH_META_GROUP_SIZE * group_index.0);
        let group = &metas[group_index.0];

        match meta_finder::meta_insert_group_find(
            bucket_offset,
            &mut first_deleted,
            group,
            if group_index.0 + 1 == metas.len() {
                max_buckets - HASH_META_GROUP_SIZE * group_index.0
            } else {
                HASH_META_GROUP_SIZE
            },
            needle,
            |index| f(bucket_offset + index),
        ) {
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
    use super::HASH_META_GROUP_SIZE;
    use crate::data_types::{BucketNr, Meta, MetaGroup, ProbeRunResult};
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8, _mm256_load_si256, _mm256_movemask_epi8,
        _mm256_or_si256, _mm256_set1_epi8,
    };
    use std::ops::Range;

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
        needle: Meta,
        mut f: impl FnMut(usize) -> bool,
    ) -> Option<ProbeRunResult> {
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
                } else if group.0[index].empty() {
                    return Some(ProbeRunResult::FoundUnoccupied(
                        bucket_offset + index,
                        needle,
                    ));
                } else if f(index) {
                    return Some(ProbeRunResult::FoundPopulated(
                        bucket_offset + index,
                        needle,
                    ));
                }

                let step = next + 1;
                if step >= HASH_META_GROUP_SIZE as u32 {
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
    pub fn meta_get_group_find(
        haystack: &MetaGroup,
        max_index: usize,
        needle: crate::data_types::Meta,
        mut f: impl FnMut(usize) -> bool,
    ) -> bool {
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
                if step >= HASH_META_GROUP_SIZE as u32 {
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
        group
            .0
            .iter()
            .enumerate()
            .find(|x| x.1.empty())
            .map(|x| x.0)
    }

    /// Returns true and stops iteration if empty node found
    #[doc(hidden)]
    #[inline]
    /// Returns true if end of probe sequence was reached, or if closure returned true.
    pub fn meta_get_group_find(
        group: &MetaGroup,
        max_index: usize,
        needle: Meta,
        mut f: impl FnMut(usize) -> bool,
    ) -> bool {
        for (idx, meta) in group.0.iter().enumerate() {
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
        group: &MetaGroup,
        max_index: usize,
        needle: Meta,
        mut f: impl FnMut(usize) -> bool,
    ) -> Option<ProbeRunResult> {
        for (idx, meta) in group.0.iter().enumerate() {
            if idx >= max_index {
                return None;
            }
            if first_deleted.is_none() && meta.deleted() {
                *first_deleted = Some(group_offset + idx);
            } else if meta.empty() {
                return Some(ProbeRunResult::FoundUnoccupied(group_offset + idx, needle));
            } else if *meta == needle {
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
    use super::HASH_META_GROUP_SIZE;
    use crate::data_types::meta_finder::get_any_empty;
    use crate::data_types::{meta_finder, BucketNr, Meta, MetaGroup, ProbeRunResult};

    #[test]
    fn meta_alignment() {
        assert_eq!(HASH_META_GROUP_SIZE, align_of::<MetaGroup>());
    }
    #[test]
    fn meta_check_empty() {
        let haystack = MetaGroup([Meta(129u8); HASH_META_GROUP_SIZE]);
        assert!(get_any_empty(&haystack).is_none());
    }
    #[test]
    fn meta_check_empty2() {
        let mut haystack = MetaGroup([Meta(129u8); HASH_META_GROUP_SIZE]);
        haystack.0[13] = Meta::EMPTY;
        assert_eq!(get_any_empty(&haystack), Some(13));
    }
    #[test]
    fn meta_get_finds_medium() {
        let mut haystack = MetaGroup([Meta(129u8); HASH_META_GROUP_SIZE]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[13] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        meta_finder::meta_get_group_find(&haystack, HASH_META_GROUP_SIZE, needle, |pos| {
            found.push(pos);
            false
        });
        assert_eq!(found, vec![0, 13]);
    }
    #[test]
    fn meta_get_finds_small() {
        let mut haystack = MetaGroup([Meta(129u8); HASH_META_GROUP_SIZE]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[13] = Meta::new(142u8);
        haystack.0[14] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        meta_finder::meta_get_group_find(&haystack, 14, needle, |pos| {
            found.push(pos);
            false
        });
        assert_eq!(found, vec![0, 13]);
    }
    #[test]
    fn meta_insert_medium() {
        let mut haystack = MetaGroup([Meta(129u8); HASH_META_GROUP_SIZE]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[1] = Meta::DELETED;
        haystack.0[13] = Meta::new(142u8);
        haystack.0[31] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        let mut first_deleted = None;

        let result = meta_finder::meta_insert_group_find(
            BucketNr(32),
            &mut first_deleted,
            &haystack,
            31,
            needle,
            |pos| {
                found.push(pos);
                false
            },
        );
        assert_eq!(result, None);
        assert_eq!(first_deleted, Some(BucketNr(33)));
        assert_eq!(found, vec![0, 13]);
    }

    #[test]
    fn meta_insert2() {
        let mut haystack = MetaGroup([Meta(129u8); HASH_META_GROUP_SIZE]);
        haystack.0[2] = Meta::DELETED;
        haystack.0[13] = Meta::new(142u8);
        haystack.0[14] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        let mut first_deleted = None;

        let result = meta_finder::meta_insert_group_find(
            BucketNr(32),
            &mut first_deleted,
            &haystack,
            HASH_META_GROUP_SIZE,
            needle,
            |pos| {
                found.push(pos);
                true
            },
        );
        assert_eq!(
            result,
            Some(ProbeRunResult::FoundPopulated(BucketNr(13 + 32), needle))
        );
        assert_eq!(first_deleted, Some(BucketNr(34)));
        assert_eq!(found, vec![13]);
    }
    #[test]
    fn meta_insert3() {
        let mut haystack = MetaGroup([Meta(129u8); HASH_META_GROUP_SIZE]);
        haystack.0[2] = Meta::DELETED;
        haystack.0[13] = Meta::EMPTY;
        haystack.0[14] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        let mut first_deleted = None;

        let result = meta_finder::meta_insert_group_find(
            BucketNr(32),
            &mut first_deleted,
            &haystack,
            32,
            needle,
            |pos| {
                found.push(pos);
                true
            },
        );
        assert_eq!(
            result,
            Some(ProbeRunResult::FoundUnoccupied(BucketNr(13 + 32), needle))
        );
        assert_eq!(first_deleted, Some(BucketNr(34)));
        assert_eq!(found, vec![]);
    }

    #[test]
    fn meta_get_finds_empty() {
        let mut haystack = MetaGroup([Meta::new(129u8); HASH_META_GROUP_SIZE]);
        haystack.0[0] = Meta::new(142u8);
        haystack.0[17] = Meta::new(142u8);
        haystack.0[18] = Meta::EMPTY;
        haystack.0[19] = Meta::new(142u8);
        let needle = Meta::new(142u8);
        let mut found = Vec::new();
        meta_finder::meta_get_group_find(&haystack, 32, needle, |pos| {
            found.push(pos);
            false
        });
        assert_eq!(found, vec![0, 17]);
    }
}

impl Meta {
    const DELETED: Meta = Meta(1);
    const EMPTY: Meta = Meta(0);
    fn deleted(&self) -> bool {
        self.0 == 1
    }
    fn populated(&self) -> bool {
        self.0 & 128 != 0
    }
    fn empty(&self) -> bool {
        self.0 == 0
    }
    #[doc(hidden)]
    pub const fn new(x: u8) -> Meta {
        Meta(x)
    }
}

struct WithConcat<I, V>(Option<I>, Option<V>);
impl<I: Iterator> WithConcat<I, I::Item> {
    fn new(iter: I, val: I::Item) -> Self {
        WithConcat(Some(iter), Some(val))
    }
}
impl<I: Iterator> Iterator for WithConcat<I, I::Item> {
    type Item = I::Item;

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut left_len = self.0.as_ref().map(|x| x.size_hint().0).unwrap_or(0);
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
impl<I: Iterator> ExactSizeIterator for WithConcat<I, I::Item> {}

pub struct NoatunHashMapIterator<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject>
{
    hash_buckets: &'a [MaybeUninit<NoatunHashBucket<K, V>>],
    metas: &'a [MetaGroup],
    next_position: usize,
}
struct DatabaseHashOwningIterator<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject>
{
    hash_buckets: HashAccessContextMut<'a, K, V>,
    next_position: usize,
}
impl<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> Iterator
    for NoatunHashMapIterator<'a, K, V>
{
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
                let bucket = unsafe { self.hash_buckets[pos].assume_init_ref() };
                return Some((&bucket.key, &bucket.v));
            }
        }
    }
}

impl<K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> Iterator
    for DatabaseHashOwningIterator<'_, K, V>
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let pos = self.next_position;
            if pos >= self.hash_buckets.buckets.len() {
                return None;
            }
            self.next_position += 1;
            let meta = get_meta(self.hash_buckets.metas, BucketNr(pos));
            if meta.populated() {
                let bucket = unsafe { self.hash_buckets.buckets[pos].assume_init_mut() };
                let key_p = &mut bucket.key as *mut K;
                let v_p = &mut bucket.v as *mut V;
                return unsafe { Some((key_p.read(), v_p.read())) };
            }
        }
    }
}

struct HashAccessContext<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> {
    metas: &'a [MetaGroup],
    buckets: &'a [MaybeUninit<NoatunHashBucket<K, V>>],
}

impl<K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> Copy
    for HashAccessContext<'_, K, V>
{
}

impl<K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> Clone
    for HashAccessContext<'_, K, V>
{
    fn clone(&self) -> Self {
        *self
    }
}

struct HashAccessContextMut<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> {
    metas: &'a mut [MetaGroup],
    buckets: &'a mut [MaybeUninit<NoatunHashBucket<K, V>>],
}

fn get_meta_mut(metas: &mut [MetaGroup], bucket: BucketNr) -> &mut Meta {
    let group = bucket.0 / HASH_META_GROUP_SIZE;
    let subindex = bucket.0 % HASH_META_GROUP_SIZE;
    &mut metas[group].0[subindex]
}
fn get_meta(metas: &[MetaGroup], bucket: BucketNr) -> &Meta {
    let group = bucket.0 / HASH_META_GROUP_SIZE;
    let subindex = bucket.0 % HASH_META_GROUP_SIZE;
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
        before_empty: &'a mut Meta,
    },
}

/// Some(x) if the given bucket is part of a group with at least one empty slot.
/// x will be the first such empty slot.
fn get_meta_mut_and_emptyable(metas: &mut [MetaGroup], bucket: BucketNr) -> MetaMutAndEmpty<'_> {
    let group = bucket.0 / HASH_META_GROUP_SIZE;
    let subindex = bucket.0 % HASH_META_GROUP_SIZE;
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
                debug_assert_ne!(subindex, empty - 1);
                debug_assert!(subindex < empty - 1);

                group_obj.validate();
                let [meta, before_empty] =
                    group_obj.0.get_disjoint_mut([subindex, empty - 1]).unwrap();
                MetaMutAndEmpty::HasEmpty {
                    meta_bucket: bucket,
                    meta,
                    before_empty_bucket: BucketNr(HASH_META_GROUP_SIZE * group + empty - 1),
                    before_empty,
                }
            }
        }
        None => MetaMutAndEmpty::NoEmpty(&mut group_obj.0[subindex]),
    }
}

impl<'a, K: NoatunKey + PartialEq, V: FixedSizeObject> HashAccessContextMut<'a, K, V> {
    fn readonly(&'a self) -> HashAccessContext<'a, K, V> {
        HashAccessContext {
            metas: self.metas,
            buckets: self.buckets,
        }
    }
}

struct LengthGuard<'a, K: NoatunKey, V: FixedSizeObject> {
    new_length: usize,

    map: &'a mut NoatunHashMap<K, V>,
    //length_field: *mut usize,
}

impl<'a, K: NoatunKey, V: FixedSizeObject> LengthGuard<'a, K, V> {
    fn new(map: &'a mut NoatunHashMap<K, V>) -> LengthGuard<'a, K, V> {
        LengthGuard {
            new_length: map.length,
            map,
        }
    }
}

impl<K: NoatunKey, V: FixedSizeObject> Drop for LengthGuard<'_, K, V> {
    fn drop(&mut self) {
        NoatunContext.write_ptr(self.new_length, &mut self.map.length);
    }
}

/// Entry type for NoatunHashMap
pub enum NoatunHashMapEntry<'a, K, V>
where
    K: NoatunStorable + NoatunKey + PartialEq,
    V: FixedSizeObject,
    K::DetachedType: Sized,
{
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}
pub struct NoatunHashMapEntryInternal<'a, K, V>
where
    K: NoatunStorable + NoatunKey + PartialEq,
    V: FixedSizeObject,
    K::DetachedType: Sized,
{
    context: HashAccessContextMut<'a, K, V>,
    probe_result: (BucketNr, Meta),
    key: K::DetachedOwnedType,
    length: &'a mut usize,
}

pub struct VacantEntry<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject>
where
    K::DetachedType: Sized,
{
    context: HashAccessContextMut<'a, K, V>,
    probe_result: (BucketNr, Meta),
    key: K::DetachedOwnedType,
    length: &'a mut usize,
}
pub struct OccupiedEntry<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject>
where
    K::DetachedType: Sized,
{
    context: HashAccessContextMut<'a, K, V>,
    probe_result: (BucketNr, Meta),
    key: K::DetachedOwnedType,
    length: &'a mut usize,
}

impl<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> VacantEntry<'a, K, V>
where
    K::DetachedType: Sized,
{
    pub fn insert(self, v: &V::DetachedType) -> Pin<&'a mut V> {
        NoatunHashMap::insert_at_bucket(
            true,
            self.probe_result,
            self.key.borrow(),
            self.context,
            |new, val| {
                if new {
                    val.init_from_detached(v)
                }
            },
            self.length,
        )
    }
}

impl<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> OccupiedEntry<'a, K, V>
where
    K::DetachedType: Sized,
{
    pub fn get(&self) -> &V {
        let bucket_nr = self.probe_result.0;
        unsafe { &self.context.buckets[bucket_nr.0].assume_init_ref().v }
    }
    pub fn get_mut(&mut self) -> Pin<&mut V> {
        let bucket_nr = self.probe_result.0;
        unsafe { Pin::new_unchecked(&mut self.context.buckets[bucket_nr.0].assume_init_mut().v) }
    }
    pub fn insert(&mut self, v: &V::DetachedType) -> V::DetachedOwnedType {
        let val = self.get_mut();
        let ret = val.detach();
        val.init_from_detached(v);
        ret
    }
    pub fn remove(mut self) -> V::DetachedOwnedType {
        let bucket_nr = self.probe_result.0;
        let val = self.get();
        let ret = val.detach();

        trace!("removing HashMap key using entry");
        NoatunHashMap::remove_impl_by_bucket_nr(&mut self.context, bucket_nr, |_| {});
        let newlen = *self.length - 1;
        NoatunContext.write_internal(newlen, self.length);

        ret
    }
}
impl<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> NoatunHashMapEntry<'a, K, V>
where
    K::DetachedType: Sized,
{
    fn make_enum(
        probe_result: ProbeRunResult,
        context: HashAccessContextMut<'a, K, V>,
        key: K::DetachedOwnedType,
        length: &'a mut usize,
    ) -> NoatunHashMapEntry<'a, K, V> {
        match probe_result {
            ProbeRunResult::HashFull => {
                panic!("internal error, HashFull")
            }
            ProbeRunResult::FoundPopulated(_, _) => NoatunHashMapEntry::Occupied(OccupiedEntry {
                context,
                probe_result: probe_result.bucket_meta(),
                key,
                length,
            }),
            ProbeRunResult::FoundUnoccupied(_, _) => NoatunHashMapEntry::Vacant(VacantEntry {
                context,
                probe_result: probe_result.bucket_meta(),
                key,
                length,
            }),
        }
    }
    fn unify(self) -> NoatunHashMapEntryInternal<'a, K, V> {
        match self {
            NoatunHashMapEntry::Occupied(OccupiedEntry {
                context,
                probe_result,
                key,
                length,
            })
            | NoatunHashMapEntry::Vacant(VacantEntry {
                context,
                probe_result,
                key,
                length,
            }) => NoatunHashMapEntryInternal {
                context,
                probe_result,
                key,
                length,
            },
        }
    }
    fn or_default(self) -> Pin<&'a mut V> {
        let new = matches!(self, NoatunHashMapEntry::Vacant(_));
        let tself = self.unify();
        NoatunHashMap::insert_at_bucket(
            new,
            tself.probe_result,
            tself.key.borrow(),
            tself.context,
            |_new, _val| {
                // Leave at default
            },
            tself.length,
        )
    }
    /// In
    fn or_insert(self, v: &V::DetachedType) -> Pin<&'a mut V> {
        let new = matches!(self, NoatunHashMapEntry::Vacant(_));
        let tself = self.unify();
        NoatunHashMap::insert_at_bucket(
            new,
            tself.probe_result,
            tself.key.borrow(),
            tself.context,
            |new, val| {
                if new {
                    val.init_from_detached(v)
                }
            },
            tself.length,
        )
    }
    fn or_insert_with(self, v: impl FnOnce() -> V::DetachedOwnedType) -> Pin<&'a mut V> {
        let new = matches!(self, NoatunHashMapEntry::Vacant(_));
        let tself = self.unify();
        NoatunHashMap::insert_at_bucket(
            new,
            tself.probe_result,
            tself.key.borrow(),
            tself.context,
            |new, val| {
                if new {
                    val.init_from_detached(v().borrow())
                }
            },
            tself.length,
        )
    }
}

impl<K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> NoatunHashMap<K, V> {
    fn assert_not_apply(&self, method: &str, untracked_version_available: bool) {
        let context = unsafe { &*get_context_ptr() };
        if context.is_message_apply() {
            let extra = if untracked_version_available {
                format!(" To bypass this check, use NoatunHashMap::untracked_{method} instead.")
            } else {
                "".to_string()
            };
            panic!(
                "A call was made to NoatunHashMap::{method} from within Message::apply.
                 This is not allowed, since it would make the current Message causally dependent
                 upon all previous mutations to the map.{extra}"
            );
        }
    }

    /// Returns the number of elements in the map.
    ///
    /// This method panics if called from within [`Message::apply`]. But also see
    /// [`Self::untracked_len`].
    pub fn len(&self) -> usize {
        self.assert_not_apply("len", true);

        self.length
    }

    /// Note, this does not record a read dependency.
    ///
    /// Future pruning of unrelated messages may affect the result of this method.
    /// Only use the return value for logging/debugging, or uses where the numerical
    /// value will not be used as a decision factor in any logic.
    pub fn untracked_len(&self) -> usize {
        self.length
    }

    /// Returns true if there are no elements in the map.
    ///
    /// This method panics if called from within [`Message::apply`]. But also see
    /// [`Self::untracked_is_empty`].
    pub fn is_empty(&self) -> bool {
        self.assert_not_apply("is_empty", true);
        self.length == 0
    }
    /// Returns true if there are no elements in the map.
    ///
    /// Note, this does not record a read dependency.
    ///
    /// Future pruning of unrelated messages may affect the result of this method.
    /// Only use the return value for logging/debugging, or uses where the true
    /// value will not be used as a decision factor in any logic.
    pub fn untracked_is_empty(&self) -> bool {
        self.length == 0
    }

    /// Iterate over all the key/value-pairs of the map.
    ///
    /// Note, this does not record any read dependency.
    /// Generally, this is not a problem, since the code is probably reading from the
    /// actual iterated values, recording read dependencies on them.
    ///
    /// However, code could just count the number of elements by exhausting the iterator
    /// and counting the number of values. Doing this will not record any read dependency.
    ///
    /// It is recommended that applications do not count the number of iterated values, or
    /// if they do, that they do not use the numerical value for any decisions.
    pub fn iter(&self) -> NoatunHashMapIterator<'_, K, V> {
        let context = self.data_meta_len();
        NoatunHashMapIterator {
            hash_buckets: context.buckets,
            metas: context.metas,
            next_position: 0,
        }
    }

    /// Note, this takes ownership of keys+values, but doesn't update self.
    /// 'self' _must_ be overwritten subsequently using `Self::replace_internal`.
    unsafe fn unsafe_into_iter<'a>(&mut self) -> DatabaseHashOwningIterator<'a, K, V> {
        DatabaseHashOwningIterator {
            hash_buckets: self.data_meta_len_mut_unsafe(),
            next_position: 0,
        }
    }
    fn data_meta_len_mut(&mut self) -> HashAccessContextMut<'_, K, V> {
        self.data_meta_len_mut_unsafe()
    }
    fn data_meta_len_mut2<'a>(&'a mut self) -> (HashAccessContextMut<'a, K, V>, &'a mut usize) {
        (self.data_meta_len_mut_unsafe(), &mut self.length)
    }
    fn data_meta_len_mut_unsafe<'a>(&mut self) -> HashAccessContextMut<'a, K, V> {
        let dptr = NoatunContext.start_ptr_mut().wrapping_add(self.data);
        let align = align_of::<NoatunHashBucket<K, V>>().max(align_of::<MetaGroup>());
        let cap = self.capacity;
        if cap == 0 {
            return HashAccessContextMut {
                metas: &mut [],
                buckets: &mut [],
            };
        }
        let meta_group_count = cap.div_ceil(HASH_META_GROUP_SIZE);
        let aligned_meta_size = (size_of::<MetaGroup>() * meta_group_count).next_multiple_of(align);

        unsafe {
            let meta_groups = slice::from_raw_parts_mut(dptr as *mut MetaGroup, meta_group_count);
            let buckets = slice::from_raw_parts_mut(
                dptr.wrapping_add(aligned_meta_size) as *mut MaybeUninit<NoatunHashBucket<K, V>>,
                cap,
            );
            HashAccessContextMut {
                metas: meta_groups,
                buckets,
            }
        }
    }
    fn data_meta_len(&self) -> HashAccessContext<'_, K, V> {
        let dptr = NoatunContext.start_ptr().wrapping_add(self.data);
        let align = align_of::<NoatunHashBucket<K, V>>().max(align_of::<MetaGroup>());
        let cap = self.capacity;
        if cap == 0 {
            return HashAccessContext {
                metas: &[],
                buckets: &[],
            };
        }
        let meta_group_count = cap.div_ceil(HASH_META_GROUP_SIZE);
        let aligned_meta_size = (size_of::<MetaGroup>() * meta_group_count).next_multiple_of(align);

        unsafe {
            let meta_groups = slice::from_raw_parts(dptr as *const MetaGroup, meta_group_count);
            let buckets = slice::from_raw_parts(
                dptr.wrapping_add(aligned_meta_size) as *const MaybeUninit<NoatunHashBucket<K, V>>,
                cap,
            );
            HashAccessContext {
                metas: meta_groups,
                buckets,
            }
        }
    }

    /// Probe only useful for reading/updating existing bucket
    fn probe_read(
        database_context_data: HashAccessContext<K, V>,
        key: &K::DetachedType,
    ) -> Option<BucketNr> {
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

        let probe = BucketProbeSequence::new(group_nr, cap.div_ceil(HASH_META_GROUP_SIZE));

        run_get_probe_sequence(
            metas,
            cap,
            key_meta,
            |bucket_nr| {
                if <K as NoatunKey>::eq(
                    unsafe { buckets[bucket_nr.0].assume_init_ref().key.detach_key_ref() },
                    key,
                ) {
                    result = Some(bucket_nr);
                    true
                } else {
                    false
                }
            },
            probe,
        );

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

    /// Remove any items in the map for which the predicate returns true.
    ///
    /// This method does not record any read-dependency on the map. Applications are
    /// encouraged not to count the number of invocations of the predicate, or if they do,
    /// to not base any logic on the numeric value.
    pub fn retain(self: Pin<&mut Self>, mut predicate: impl FnMut(&K, Pin<&mut V>) -> bool) {
        let tself = unsafe { self.get_unchecked_mut() };
        let mut length_guard_and_map = LengthGuard::new(tself);
        let mut context = length_guard_and_map.map.data_meta_len_mut();

        let buckets_count = context.buckets.len();
        let mut i = 0;
        while i < buckets_count {
            let meta_group_index = i / HASH_META_GROUP_SIZE;
            let meta_group_offset = i % HASH_META_GROUP_SIZE;
            let meta = &mut context.metas[meta_group_index].0[meta_group_offset];
            if meta.populated() {
                let bucket = unsafe { context.buckets[i].assume_init_mut() };
                let val = unsafe { Pin::new_unchecked(&mut bucket.v) };
                if !predicate(&bucket.key, val) {
                    let bucket_nr = BucketNr(i);
                    length_guard_and_map.new_length -= 1;
                    trace!(key=?&bucket.key, "removing hashmap entry using 'retain'");
                    Self::remove_impl_by_bucket_nr(&mut context, bucket_nr, |_| {});
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        // Drop of length_guard will decrease length field value.
    }

    fn clear_impl(&mut self) {
        let context = self.data_meta_len_mut();
        let buckets_count = context.buckets.len();
        for i in 0..buckets_count {
            let meta_group_index = i / HASH_META_GROUP_SIZE;
            let meta_group_offset = i % HASH_META_GROUP_SIZE;
            let meta = &mut context.metas[meta_group_index].0[meta_group_offset];
            if meta.populated() {
                NoatunContext.write_internal(Meta::EMPTY, meta);
                let val =
                    unsafe { Pin::new_unchecked(&mut context.buckets[i].assume_init_mut().v) };
                val.destroy();
            } else if *meta != Meta::EMPTY {
                NoatunContext.write_internal(Meta::EMPTY, meta);
            }
        }
    }

    /// Clear all elements.
    ///
    /// This does not itself record any tombstones, but does record the current message
    /// as the "last clearer" of this map.
    ///
    /// If the map is repeatedly built up, then cleared, it's much more efficient to
    /// use this method than other methods to delete items from the map. The reason is
    /// that after 'clear', Noatun will be able to prune all previous messages, including
    /// those invoking 'clear'.
    pub fn clear(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };

        NoatunContext.update_registrar_ptr(addr_of_mut!(tself.clear_registrar), true);

        NoatunContext.write_internal(0, &mut tself.length);
        tself.clear_impl();
    }

    /// General purpose bucket probe
    fn probe(
        context: HashAccessContext<K, V>,
        key: impl Borrow<K::DetachedType>,
    ) -> ProbeRunResult {
        let HashAccessContext { metas, buckets } = context;
        let cap = buckets.len();
        if cap == 0 {
            return ProbeRunResult::HashFull;
        }
        let mut h = NoatunHasher::new();
        let key = key.borrow();
        K::hash(key, &mut h);

        let (meta_group_nr, key_meta) = MetaGroupNr::from_u64(h.finish(), cap);
        //let mut visited_buckets = 0;
        //let max_visited_buckets = self.capacity/2;
        //let mut first_deleted = None;
        let probe = BucketProbeSequence::new(meta_group_nr, cap.div_ceil(HASH_META_GROUP_SIZE));

        run_insert_probe_sequence(
            metas,
            cap,
            key_meta,
            |bucket_nr| unsafe {
                <K as NoatunKey>::eq(
                    buckets[bucket_nr.0].assume_init_ref().key.detach_key_ref(),
                    key,
                )
            },
            probe,
        )

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
        assert_eq!(HASH_META_GROUP_SIZE, 32); //Consider how any change to this affects the sizes to be chosen in this method
        static PRIMES: &[usize] = &[
            1,
            3,
            7,
            17,
            37,
            67,
            127,
            (1usize << 8) - 5,
            (1usize << 9) - 3,
            (1usize << 10) - 3,
            (1usize << 11) - 9,
            (1usize << 12) - 3,
            (1usize << 13) - 1,
            (1usize << 14) - 3,
            (1usize << 15) - 19,
            (1usize << 16) - 15,
            (1usize << 17) - 1,
            (1usize << 18) - 5,
            (1usize << 19) - 1,
            (1usize << 20) - 3,
            (1usize << 21) - 9,
            (1usize << 22) - 3,
            (1usize << 23) - 15,
            (1usize << 24) - 3,
            (1usize << 25) - 39,
            (1usize << 26) - 5,
            (1usize << 27) - 39,
            (1usize << 28) - 57,
            (1usize << 29) - 3,
            (1usize << 30) - 35,
            (1usize << 31) - 1,
            (1usize << 32) - 5,
            (1usize << 33) - 9,
            (1usize << 34) - 41,
            (1usize << 35) - 31,
            (1usize << 36) - 5,
            (1usize << 37) - 25,
            (1usize << 38) - 45,
            (1usize << 39) - 7,
            (1usize << 40) - 87,
            (1usize << 41) - 21,
            (1usize << 42) - 11,
            (1usize << 43) - 57,
            (1usize << 44) - 17,
            (1usize << 45) - 55,
            (1usize << 46) - 21,
            (1usize << 47) - 115,
            (1usize << 48) - 59,
            (1usize << 49) - 81,
            (1usize << 50) - 27,
            (1usize << 51) - 129,
            (1usize << 52) - 47,
            (1usize << 53) - 111,
            (1usize << 54) - 33,
            (1usize << 55) - 55,
            (1usize << 56) - 5,
            (1usize << 57) - 13,
            (1usize << 58) - 27,
            (1usize << 59) - 55,
            (1usize << 60) - 93,
            (1usize << 61) - 1,
            (1usize << 62) - 57,
            (1usize << 63) - 25,
        ];
        let group_count = capacity.div_ceil(HASH_META_GROUP_SIZE);
        let (Ok(x) | Err(x)) = PRIMES.binary_search(&group_count);
        HASH_META_GROUP_SIZE * PRIMES[x.min(PRIMES.len() - 1)]
    }

    /// For a mutable version of this, see [`get_mut_val`].
    #[inline]
    pub fn get(&self, key: &K::DetachedType) -> Option<&V> {
        let context = self.data_meta_len();
        let bucket = Self::probe_read(context, key)?;
        unsafe { Some(&context.buckets[bucket.0].assume_init_ref().v) }
    }

    /// Returns true if the given key is present in the map.
    pub fn contains_key(&self, key: &K::DetachedType) -> bool {
        let context = self.data_meta_len();
        Self::probe_read(context, key).is_some()
    }

    /// NOTE! This method is not available from within [`Message::apply`],
    /// and will panic if called from there.
    pub fn get_mut_val<'a>(
        self: Pin<&'a mut Self>,
        key: &K::DetachedType,
    ) -> Option<Pin<&'a mut V>> {
        let tself = unsafe { self.get_unchecked_mut() };
        let context = tself.data_meta_len_mut();
        let bucket = Self::probe_read(context.readonly(), key)?;
        unsafe {
            Some(Pin::new_unchecked(
                &mut context.buckets[bucket.0].assume_init_mut().v,
            ))
        }
    }

    /// Return the value for the given key.
    ///
    /// If the key is not present in the map, insert it with an all-zero value.
    pub fn get_insert<'a>(mut self: Pin<&'a mut Self>, key: &K::DetachedType) -> Pin<&'a mut V> {
        let context = self.data_meta_len();
        if let Some(bucket) = Self::probe_read(context, key) {
            trace!(bucket=?bucket, "Existing bucket found");
            unsafe {
                let context = self.get_unchecked_mut().data_meta_len_mut();
                return Pin::new_unchecked(&mut context.buckets[bucket.0].assume_init_mut().v);
            }
        }

        {
            let tself = unsafe { self.as_mut().get_unchecked_mut() };

            tself.insert_internal_impl(key, |_new, _target| {
                // Leave as zero
            });
        }

        self.get_mut_val(key).unwrap()
    }

    /// Return true if a value was removed
    pub fn remove(self: Pin<&mut Self>, key: &K::DetachedType) -> bool {
        unsafe { self.get_unchecked_mut().remove_impl(key, |_| {}) }
    }

    /// Return true if a value was removed
    pub(crate) fn remove_internal(&mut self, key: &K::DetachedType) -> bool {
        self.remove_impl(key, |_| {})
    }

    /// Remove and return the value for the given key.
    ///
    /// If the key is not present, None is returned.
    pub fn pop(&mut self, key: &K::DetachedType) -> Option<V::DetachedOwnedType> {
        let mut retval = None;
        self.remove_impl(key, |val| {
            retval = Some(val.detach());
        });
        retval
    }

    fn remove_impl(
        &mut self,
        key: &K::DetachedType,
        getval: impl FnOnce(&mut Pin<&mut V>),
    ) -> bool {
        let context = self.data_meta_len_mut();

        // We mark as tombstone even before actually checking if the entry exists.
        // We must do this, because the remove may make the message that originally
        // wrote the entry be pruned, meaning that on reprojection, we won't find anything,
        // and no tombstone would be created.
        NoatunContext.wrote_tombstone();
        let Some(bucket) = Self::probe_read(context.readonly(), key) else {
            return false;
        };
        let mut context = self.data_meta_len_mut();
        trace!("removing hashmap entry using remove");
        Self::remove_impl_by_bucket_nr(&mut context, bucket, getval);

        #[cfg(debug_assertions)]
        {
            for group in context.metas {
                group.validate();
            }
        }

        // Doing this last means the length would be off if we are aborted during
        // the remove (perhaps by some other thread terminating the process). However,
        // this should always be caught and should leave the database in a dirty state.
        let new_length = self.length - 1;
        NoatunContext.write_internal(new_length, &mut self.length);
        true
    }

    /// This does not update `self.length`
    fn remove_impl_by_bucket_nr(
        context: &mut HashAccessContextMut<K, V>,
        bucket_nr: BucketNr,
        getval: impl FnOnce(&mut Pin<&mut V>),
    ) {
        unsafe {
            let mut val = Pin::new_unchecked(&mut context.buckets[bucket_nr.0].assume_init_mut().v);
            getval(&mut val);
            val.as_mut().destroy();
            NoatunContext.zero_storable(val);
        };


        match get_meta_mut_and_emptyable(context.metas, bucket_nr) {
            MetaMutAndEmpty::NoEmpty(meta) => {
                NoatunContext.write_internal(Meta::DELETED, meta);
            }
            MetaMutAndEmpty::HasEmptyAfterMeta(meta) => {
                NoatunContext.write_internal(Meta::EMPTY, meta);
            }
            MetaMutAndEmpty::HasEmpty {
                meta_bucket,
                meta,
                before_empty_bucket,
                before_empty,
            } => {
                NoatunContext.copy(before_empty, meta);
                NoatunContext.write_internal(Meta::EMPTY, before_empty);

                let [meta_bucket_obj, before_empty_bucket_obj] = context
                    .buckets
                    .get_disjoint_mut([meta_bucket.0, before_empty_bucket.0])
                    .unwrap();

                unsafe {
                    NoatunContext.copy(
                        before_empty_bucket_obj.assume_init_ref(),
                        meta_bucket_obj.assume_init_mut(),
                    );
                }
            }
        }
    }

    /// Insert the given key value pair.
    ///
    /// If the key already contained a value, that value is returned.
    pub fn insert(
        self: Pin<&mut Self>,
        key: impl Borrow<K::DetachedType>,
        val: &V::DetachedType,
    ) -> Option<V::DetachedOwnedType>
    where
        V::DetachedOwnedType: Sized,
    {
        let mut ret: Option<V::DetachedOwnedType> = None;
        unsafe { self.get_unchecked_mut() }.insert_internal_impl(key, |new, target| {
            if !new {
                ret = Some(target.detach());
            }
            V::init_from_detached(target, val)
        });
        ret
    }

    /// Return true if a value with the given key already existed. In this case, it is
    /// overwritten. If no previous value existed, one is inserted.
    pub fn insert_fast(
        self: Pin<&mut Self>,
        key: impl Borrow<K::DetachedType>,
        val: &V::DetachedType,
    ) -> bool {
        let mut existed = false;
        unsafe { self.get_unchecked_mut() }.insert_internal_impl(key, |new, target| {
            if !new {
                existed = true;
            }
            V::init_from_detached(target, val)
        });
        existed
    }

    pub(crate) fn insert_internal(
        &mut self,
        key: impl Borrow<K::DetachedType>,
        val: &V::DetachedType,
    ) {
        self.insert_internal_impl(key, |_new, target| V::init_from_detached(target, val))
    }

    fn entry(&mut self, key: K::DetachedOwnedType) -> NoatunHashMapEntry<K, V>
    where
        K::DetachedType: Sized,
    {
        let self_cap = self.capacity;
        let context = self.data_meta_len();
        let mut probe_result = Self::probe(context, key.borrow());

        if !matches!(probe_result, ProbeRunResult::HashFull) {
            // Nominal, fast path.
            let (context, length) = self.data_meta_len_mut2();
            return NoatunHashMapEntry::make_enum(probe_result, context, key, length);
        }

        self.internal_change_capacity(self_cap + 15);

        let (context, length) = self.data_meta_len_mut2();
        probe_result = Self::probe(context.readonly(), key.borrow());
        assert!(!matches!(probe_result, ProbeRunResult::HashFull));
        NoatunHashMapEntry::make_enum(probe_result, context, key, length)
    }

    fn nothing(_k: &V) {}

    fn insert_at_bucket<'a>(
        new: bool,
        probe_result: (BucketNr, Meta),
        key: &K::DetachedType,
        context: HashAccessContextMut<'a, K, V>,
        val: impl FnOnce(bool, Pin<&mut V>),
        length: &mut usize,
    ) -> Pin<&'a mut V> {
        let (bucket, meta) = probe_result;
        let bucket_obj = unsafe { context.buckets[bucket.0].assume_init_mut() };

        let mut old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };

        val(new, old_v.as_mut());

        if new {
            let bucket_meta = get_meta_mut(context.metas, bucket);
            NoatunContext.write_internal(meta, bucket_meta);
            let old_k = unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
            old_k.init_from_detached(key);
            let new_length = *length + 1;
            NoatunContext.write_internal(new_length, length);
        }
        old_v
    }

    fn insert_internal_impl(
        &mut self,
        key: impl Borrow<K::DetachedType>,
        val: impl FnMut(bool /*new*/, Pin<&mut V>),
    ) {
        let key = key.borrow();
        let (context, length) = self.data_meta_len_mut2();
        let probe_result = Self::probe(context.readonly(), key);
        match probe_result {
            ProbeRunResult::HashFull => {
                self.internal_change_capacity(
                    self.capacity + 2
                );
                // Will not give infinite recursion, since 'new' has a capacity of at least 2 more
                self.insert_internal_impl(key, val);
            }
            ProbeRunResult::FoundUnoccupied(bucket, _meta)| //Optimization: We _could_ use the knowledge that this is unoccupied, to avoid the zero-check in write_pod
            ProbeRunResult::FoundPopulated(bucket, _meta) => {
                trace!(bucket=?bucket, "inserting hashmap element");

                Self::insert_at_bucket(
                    matches!(probe_result, ProbeRunResult::FoundUnoccupied(..)),
                    probe_result.bucket_meta(), key, context, val, length);
                /*
                let bucket_obj = unsafe { context.buckets[bucket.0].assume_init_mut() };

                let old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };


                val(old_v);

                if matches!(probe_result, ProbeRunResult::FoundUnoccupied(_, _)) {
                    let bucket_meta = get_meta_mut(context.metas, bucket);
                    NoatunContext.write_internal(meta, bucket_meta);
                    let old_k= unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
                    old_k.init_from_detached(key);
                    let new_length = self.length + 1;
                    NoatunContext.write_internal(new_length, &mut self.length);
                }
                */
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
            ProbeRunResult::FoundUnoccupied(bucket, meta)| //Optimization: We _could_ use the knowledge that this is unoccupied, to avoid the zero-check in write_pod
            ProbeRunResult::FoundPopulated(bucket, meta) => {
                let bucket_obj = unsafe { context.buckets[bucket.0].assume_init_mut()};
                let old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };
                NoatunContext.write(val, old_v);
                let bucket_meta = get_meta_mut(context.metas, bucket);
                NoatunContext.write_internal(meta, bucket_meta);
                if matches!(probe_result, ProbeRunResult::FoundUnoccupied(_, _)) {
                    let old_k= unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
                    NoatunContext.write(key, old_k);

                    let new_length = self.length + 1;
                    NoatunContext.write_internal(new_length, &mut self.length);
                }
            }
        }
    }

    /// WARNING! This returns a non-pinned DatabaseHash instance that is not in
    /// the noatun db. This is a fickle thing, most operations on it yield garbage.
    fn initialize_with_capacity(&mut self, capacity: usize) {
        let meta_size = capacity.div_ceil(HASH_META_GROUP_SIZE);

        let align = align_of::<NoatunHashBucket<K, V>>().max(align_of::<MetaGroup>());
        let aligned_meta_size = (HASH_META_GROUP_SIZE * meta_size).next_multiple_of(align);
        let bucket_data_size = capacity * size_of::<NoatunHashBucket<K, V>>();

        let data = NoatunContext.allocate_raw(aligned_meta_size + bucket_data_size, align);

        assert_eq!(data as usize % align, 0);

        let new = Self {
            length: 0,
            capacity,
            data: NoatunContext.index_of_ptr(data).0,
            clear_registrar: SequenceNr::INVALID,
            phantom_data: PhantomData,
        };
        NoatunContext.write(new, unsafe { Pin::new_unchecked(self) });
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
/// This is not an unsafe trait. However, incorrect implementations may potentially lead to
/// infinite loops, or incorrect hashmap operations.
pub trait NoatunKey: NoatunStorable + Sized + Debug {
    /// A 'detached' variant of Self.
    ///
    /// Detached types are meant to be ergonomic to work with, but may have representations
    /// that do not allow them to be stored in the mmap:ed db. For example, the detached
    /// type for `NoatunString` is simply `str`.
    ///
    /// For POD types without internal pointers, the detached version should typically be just
    /// `&'a Self`.
    type DetachedType: ?Sized;
    type DetachedOwnedType: Eq + Hash + Borrow<Self::DetachedType>;

    fn hash<H>(tself: &Self::DetachedType, state: &mut H)
    where
        H: Hasher;

    /// Return a reference to a detached key. This method should be fast, usually
    /// just returning a reference to something in memory.
    fn detach_key_ref(&self) -> &Self::DetachedType;

    fn detach_key(&self) -> Self::DetachedOwnedType;

    fn eq(a: &Self::DetachedType, b: &Self::DetachedType) -> bool;

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType);
}

impl NoatunKey for MessageId {
    type DetachedType = MessageId;
    type DetachedOwnedType = MessageId;

    fn hash<H>(tself: &Self::DetachedType, state: &mut H)
    where
        H: Hasher,
    {
        tself.hash(state)
    }

    fn detach_key_ref(&self) -> &Self::DetachedType {
        self
    }

    fn detach_key(&self) -> Self::DetachedOwnedType {
        *self
    }

    fn eq(a: &Self::DetachedType, b: &Self::DetachedType) -> bool {
        a == b
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        let tself = unsafe { self.get_unchecked_mut() };
        *tself = *detached;
    }
}

impl<K: NoatunKey + Hash + Eq, V: FixedSizeObject> Object for NoatunHashMap<K, V> {
    type Ptr = ThinPtr;
    type DetachedType = HashMap<K::DetachedOwnedType, V::DetachedOwnedType>;
    type DetachedOwnedType = HashMap<K::DetachedOwnedType, V::DetachedOwnedType>;

    fn detach(&self) -> Self::DetachedOwnedType {
        self.iter()
            .map(|(k, v)| (k.detach_key(), v.detach()))
            .collect()
    }

    fn destroy(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        NoatunContext.clear_registrar_ptr(addr_of_mut!(tself.clear_registrar), true);

        tself.clear_impl();
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        let tself = unsafe { self.get_unchecked_mut() };
        for (k, v) in detached {
            tself.insert_internal(k.borrow(), v.borrow());
        }
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate();
        ret.as_mut().init_from_detached(detached);
        ret
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        <Self as NoatunStorable>::hash_schema(hasher);
    }
}

#[cfg(test)]
mod tests {
    use super::{NoatunBox, NoatunHashMap, NoatunHashMapEntry, NoatunString};
    use crate::database::DatabaseSettings;
    use crate::tests::DummyTestMessage;

    use crate::tests::DummyTestMessageApply;
    use crate::{Database, NoatunCell, NoatunTime, Object};
    use datetime_literal::datetime;
    use std::hint::black_box;
    use std::pin::Pin;
    use std::time::Instant;

    impl DummyTestMessageApply for NoatunHashMap<u32, NoatunCell<u32>> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            let x = (time.0 % (1u64 << 32)) as u32;
            root.insert_internal(x, &x);
        }
    }
    impl DummyTestMessageApply for NoatunHashMap<NoatunString, NoatunCell<u32>> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            let x = (time.0 % (1u64 << 32)) as u32;
            root.insert_internal(x.to_string(), &x);
        }
    }
    impl DummyTestMessageApply for NoatunHashMap<NoatunString, NoatunString> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            let x = (time.0 % (1u64 << 32)) as u32;
            root.insert_internal(x.to_string(), &x.to_string());
        }
    }

    #[test]
    fn test_hashmap_miri_entry0() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    projection_time_limit: None,
                    ..DatabaseSettings::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            map.0.entry(42).or_insert(&420);

            let val = map.0.get(&42).unwrap();
            assert_eq!(val.get(), 420);

            let mut entry = map.0.entry(42);
            match &mut entry {
                NoatunHashMapEntry::Occupied(ref mut occ) => {
                    assert_eq!(occ.get().get(), 420);
                    assert_eq!(occ.get_mut().get(), 420);
                }
                NoatunHashMapEntry::Vacant(_) => {
                    unreachable!()
                }
            }
            entry.or_insert(&840);
            let val = map.0.get(&42).unwrap();
            assert_eq!(val.get(), 420);

            let entry = map.0.entry(30);
            let v = entry.or_default();
            assert_eq!(v.get(), 0);

            let val = map.0.get(&30).unwrap();
            assert_eq!(val.get(), 0);

            let val = match map.0.entry(50) {
                NoatunHashMapEntry::Occupied(_) => {
                    unreachable!()
                }
                NoatunHashMapEntry::Vacant(vac) => vac.insert(&500),
            };
            assert_eq!(val.get(), 500);

            let val = map.0.get(&50).unwrap();
            assert_eq!(val.get(), 500);
        })
        .unwrap();
    }

    #[test]
    fn test_hashmap_miri0() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    projection_time_limit: None,
                    ..DatabaseSettings::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            map.0.insert_internal(42, &42);

            let val = map.0.get(&42).unwrap();
            assert_eq!(val.get(), 42);

            let vals: Vec<u32> = map.0.iter().map(|(k, _)| *k).collect();
            assert_eq!(vals, [42]);
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_string_keys() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<NoatunString, NoatunCell<u32>>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|mut map| {
            map.0.insert_internal("hello", &42);
            assert_eq!(map.0.get("hello").unwrap().value, 42);
            assert_eq!(map.0.pop("hello"), Some(42));
            assert!(map.0.get("hello").is_none());
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_detach() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<NoatunString, NoatunCell<u32>>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|mut map| {
            map.0.insert_internal("hello", &42);

            let reg_map: Vec<_> = map.0.detach().into_iter().collect();
            assert_eq!(&[("hello".to_string(), 42)], &*reg_map);
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_attach() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<NoatunString, NoatunCell<u32>>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let mut map = map.inner_mut();
            map.insert_internal("hello", &42);

            let mut hm = std::collections::HashMap::new();
            hm.insert("world".to_string(), 43);
            map.as_mut().init_from_detached(&hm);

            let mut reg_map: Vec<_> = map.detach().into_iter().collect();
            reg_map.sort();
            assert_eq!(
                &[("hello".to_string(), 42), ("world".to_string(), 43)],
                &*reg_map
            );
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_detach2() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<NoatunString, NoatunString>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|mut map| {
            map.0.insert_internal("hello", "world");

            let reg_map: Vec<_> = map.0.detach().into_iter().collect();
            assert_eq!(&[("hello".to_string(), "world".to_string())], &*reg_map);
            map.as_mut().inner_pin().remove("hello");
            let reg_map: Vec<_> = map.0.detach().into_iter().collect();
            assert!(reg_map.is_empty());
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_delete() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            map.0.insert_internal(42, &42);

            let val = map.0.get(&42).unwrap();
            assert_eq!(val.get(), 42);

            let vals: Vec<u32> = map.0.iter().map(|(k, _)| *k).collect();
            assert_eq!(vals, [42]);

            assert!(!map.0.remove_internal(&41), "remove nonexisting key");

            assert!(map.0.remove_internal(&42));

            assert_eq!(map.0.iter().count(), 0);
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_delete_many0() {
        #[cfg(miri)]
        const N: u32 = 8;
        #[cfg(not(miri))]
        const N: u32 = 200;

        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                100000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);

            for i in 0..N {
                map.0.insert_internal(i, &i);
            }

            for i in 0..N {
                map.0.get(&i).expect("key exists before starting deletes");
            }

            for i in 0..N {
                assert!(map.0.remove_internal(&i));
                for j in i + 1..N {
                    map.0
                        .get(&j)
                        .expect(&format!("key {j} exists after delete of {i}"));
                }
            }
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_insert_many() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                200000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);
            for i in 0..300 {
                map.0.insert_internal(i, &i);
            }

            assert_eq!(map.0.len(), 300);
            assert_eq!(
                **unsafe { Pin::new_unchecked(&mut map.0) }.get_insert(&10),
                10
            );

            for i in 0..300 {
                let val = map.0.get(&i).unwrap();
                assert_eq!(val.get(), i);
            }
        })
        .unwrap();
    }

    #[test]
    fn test_hashmap_miri_retain() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                200000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);
            for i in 0..10 {
                map.0.insert_internal(i, &i);
            }
            assert_eq!(map.0.len(), 10);
            unsafe { Pin::new_unchecked(&mut map.0).retain(|k, _v| *k % 2 == 0) };
            assert_eq!(map.0.len(), 5);
            for (k, _v) in map.0.iter() {
                assert_eq!(*k % 2, 0, "the odd elements have ben removed by `retain`");
            }
        })
        .unwrap();
    }

    #[test]
    fn test_hashmap_miri_clear() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                200000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);
            for i in 0..10 {
                map.0.insert_internal(i, &i);
            }
            assert_eq!(map.0.len(), 10);
            unsafe { Pin::new_unchecked(&mut map.0).clear() };
            assert_eq!(map.0.len(), 0);
            assert!(map.0.iter().next().is_none());
        })
        .unwrap();
    }

    #[test]
    fn test_hashmap_lookup_speed_noatun() {
        let mut db: Database<DummyTestMessage<NoatunHashMap<u32, NoatunCell<u32>>>> =
            Database::create_in_memory(
                5000000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let map = unsafe { map.get_unchecked_mut() };
            assert_eq!(map.0.len(), 0);
            for i in 0..10000 {
                map.0.insert_internal(i, &i);
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
            map.insert(i, i);
        }
        let bef = Instant::now();
        for i in 0..10000 {
            let val = map.get(black_box(&i)).unwrap();
            black_box(val);
        }
        println!("std hashmap: 10000 lookups in {:?}", bef.elapsed());
    }

    #[test]
    fn test_noatun_boxed_hashmap_miri() {
        impl DummyTestMessageApply for NoatunBox<NoatunHashMap<u32, NoatunCell<u32>>> {
            fn test_message_apply(_time: NoatunTime, _root: Pin<&mut Self>) {}
        }

        let mut db: Database<DummyTestMessage<NoatunBox<NoatunHashMap<u32, NoatunCell<u32>>>>> =
            Database::create_in_memory(
                10000,
                DatabaseSettings {
                    mock_time: Some(datetime!(2021-01-01 Z).into()),
                    projection_time_limit: None,
                    ..DatabaseSettings::default()
                },
            )
            .unwrap();
        let mut db = db.begin_session_mut().unwrap();
        db.with_root_mut(|map| {
            let mut noatun_box = map.inner_mut();

            let mut hm = std::collections::HashMap::new();
            hm.insert(1, 1);

            noatun_box.as_mut().init_from_detached(&hm);

            let items: Vec<_> = noatun_box.iter().map(|x| (*x.0, x.1.get())).collect();
            assert_eq!(items, [(1, 1)]);
        })
        .unwrap();
    }
}
