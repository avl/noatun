use crate::data_types::NoatunKey;
use crate::sequence_nr::{SequenceNr, Tracker};
use crate::{NoatunContext, NoatunStorable, Object, SchemaHasher, ThinPtr};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::slice;

/// A NoatunString can be thought of as a noatun equivalent of [`std::string::String`].
/// 
/// However, under the hood, NoatunString does not allow mutation of existing data. Any
/// modification requires a complete reallocation.
/// 
#[derive(Copy, Clone)]
#[repr(C)]
pub struct NoatunString {
    start: ThinPtr,
    length: usize,
    tracker: Tracker,
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
    type ExternalType = str;
    type ExternalOwnedType = String;

    fn export(&self) -> Self::ExternalOwnedType {
        self.get().to_string()
    }

    fn destroy(self: Pin<&mut Self>) {
        let tself = unsafe { self.get_unchecked_mut() };
        unsafe {
            NoatunContext.clear_tracker_ptr(&mut tself.tracker, false);
        }
    }

    fn init_from(self: Pin<&mut Self>, external: &Self::ExternalType) {
        self.assign(external);
    }

    unsafe fn allocate_from<'a>(external: &Self::ExternalType) -> Pin<&'a mut Self> {
        let mut temp: Pin<&mut Self> = NoatunContext.allocate();
        temp.as_mut().assign(external);
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
        NoatunContext.observe_registrar(self.tracker);

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
        unsafe { NoatunContext.update_tracker_ptr(addr_of_mut!(tself.tracker), false) };
    }
}
impl NoatunKey for NoatunString {
    type ExternalType = str;
    type ExternalOwnedType = String;

    fn hash<H>(tself: &Self::ExternalType, state: &mut H)
    where
        H: Hasher,
    {
        state.write_usize(tself.len());
        state.write(tself.as_bytes());
    }

    fn export_key(&self) -> String {
        (*self).to_string()
    }
    fn export_key_ref(&self) -> &Self::ExternalType {
        self
    }

    fn eq(a: &Self::ExternalType, b: &Self::ExternalType) -> bool {
        *a == *b
    }

    fn init_from<'a>(self: Pin<&mut Self>, external: &Self::ExternalType) {
        self.assign(external);
    }

    fn destroy(&mut self) {
        unsafe {
            NoatunContext.clear_tracker_ptr(&mut self.tracker, false);
        }
    }
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct OpaqueNoatunString {
    start: ThinPtr,
    length: usize,
}

unsafe impl NoatunStorable for OpaqueNoatunString {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::OpaqueNoatunString/1")
    }
}

impl Debug for OpaqueNoatunString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl Hash for OpaqueNoatunString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: &str = self.as_ref();
        // We can safely defer to the std hash method here, since
        // `Hash` is only used for per-process hashes. Long-lived hashes
        // use NoatunHash trait instead.
        s.hash(state);
    }
}

impl Eq for OpaqueNoatunString {}
impl PartialEq for OpaqueNoatunString {
    fn eq(&self, other: &Self) -> bool {
        let s: &str = self;
        let o: &str = other;
        s.eq(o)
    }
}

impl Object for OpaqueNoatunString {
    type Ptr = ThinPtr;
    type ExternalType = str;
    type ExternalOwnedType = String;

    fn export(&self) -> Self::ExternalOwnedType {
        self.get().to_string()
    }

    fn destroy(self: Pin<&mut Self>) {}

    fn init_from(self: Pin<&mut Self>, external: &Self::ExternalType) {
        self.assign(external);
    }

    unsafe fn allocate_from<'a>(external: &Self::ExternalType) -> Pin<&'a mut Self> {
        let mut temp: Pin<&mut Self> = NoatunContext.allocate();
        temp.as_mut().assign(external);
        temp
    }

    fn hash_object_schema(hasher: &mut SchemaHasher) {
        <Self as NoatunStorable>::hash_schema(hasher);
    }
}

impl Deref for OpaqueNoatunString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl OpaqueNoatunString {
    pub fn get(&self) -> &str {
        if self.length == 0 {
            return "";
        }
        let start_ptr = NoatunContext.start_ptr().wrapping_add(self.start.0);
        unsafe {
            let bytes = slice::from_raw_parts(start_ptr, self.length);
            std::str::from_utf8_unchecked(bytes)
        }
    }
    /// Replace the string with the given value.
    ///
    /// This is not a tracked write, since OpaqueNoatunString is always opaque/untracked.
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
    }
}
impl NoatunKey for OpaqueNoatunString {
    type ExternalType = str;
    type ExternalOwnedType = String;

    fn hash<H>(tself: &Self::ExternalType, state: &mut H)
    where
        H: Hasher,
    {
        state.write_usize(tself.len());
        state.write(tself.as_bytes());
    }

    fn export_key(&self) -> String {
        (*self).to_string()
    }
    fn export_key_ref(&self) -> &Self::ExternalType {
        self
    }

    fn eq(a: &Self::ExternalType, b: &Self::ExternalType) -> bool {
        *a == *b
    }

    fn init_from<'a>(self: Pin<&mut Self>, external: &Self::ExternalType) {
        self.assign(external);
    }

    fn destroy(&mut self) {}
}
