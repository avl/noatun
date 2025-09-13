//! Datatypes for use in the noatun materialized view.
//! Normal rust types cannot be used in a noatun view, since they do not have a guaranteed
//! stable memory layout. Noatun persists materialized views to facilitate quick startup
//! after reboot. This means that every type used in a noatun materialized view
//! must have a stable memory layout. This is something not guaranteed by most standard
//! library types.
//!
//! # Foundational types:
//!
//!  * [`NoatunCell`] and [`OpaqueNoatunCell`] - used for storing primitives in noatun (like u32,
//!    i64, etc).
//!  * [`NoatunString`] and [`OpaqueNoatunString`] - used to store strings.
//!  * [`NoatunHashMap`] - Recommended collection type.
//!
//! # Other types:
//!
//!  * [`NoatunVec`] and [`OpaqueNoatunVec`] - similar to stdlib's Vec. Note, avoid
//!    [`NoatunVec`] when possible, since every push causes a read-dependency on every
//!    prior mutator, since the index of a pushed event depends on all prior mutations.
//!  * [`NoatunOption`] - similar to [`std::option::Option`]
//!  * [`NoatunBox`] - similar to [`std::boxed::Box`]
//!
use crate::{
    get_context_mut_ptr, get_context_ptr, DatabaseContextData, NoatunContext, NoatunStorable,
    Object, Pointer, SchemaHasher, ThinPtr,
};

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;

pub use noatun_cell::{NoatunCell, NoatunCellArrayExt, OpaqueNoatunCell};
pub use noatun_hash_map::meta_finder::get_any_empty;
pub use noatun_hash_map::{
    Meta, MetaGroup, MetaGroupNr, NoatunHashMap, NoatunHashMapEntry, NoatunHashMapIterator,
    NoatunKey,
};
pub use noatun_option::NoatunOption;
pub use noatun_string::{NoatunString, OpaqueNoatunString};
pub use noatun_vec::{NoatunVec, NoatunVecIterator, NoatunVecIteratorMut, OpaqueNoatunVec};
pub(crate) use noatun_vec::{NoatunVecRaw, RawDatabaseVec};

mod noatun_cell;
mod noatun_hash_impls;
mod noatun_hash_map;
mod noatun_option;
mod noatun_string;
mod noatun_vec;

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

// Safety: NoatunUntrackedCell contains only NoatunStorable fields
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
    type NativeType = T;
    type NativeOwnedType = T;

    fn export(&self) -> Self::NativeOwnedType {
        self.0
    }

    fn destroy(self: Pin<&mut Self>) {}

    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType) {
        // Safety: We don't move out of value
        unsafe {
            let value = self.get_unchecked_mut();
            *value = NoatunUntrackedCell(*external);
        }
    }

    unsafe fn allocate_from<'a>(_external: &Self::NativeType) -> Pin<&'a mut Self> {
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
        // Safety: The context ptr is valid
        unsafe { &*context_ptr }
    }

    fn get_context_mut(&mut self) -> &mut DatabaseContextData {
        let context_ptr = get_context_mut_ptr();
        // Safety: The context is valid long enough
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

/// A boxed noatun value.
///
/// This can be useful since a box can contain an unsized type.
///
/// Boxes can be used to store unsized types in collections, for example.
#[repr(transparent)]
pub struct NoatunBox<T: Object + ?Sized> {
    object_index: T::Ptr,
    phantom: PhantomData<T>,
}

// Safety: NoatunBox contains only NoatunStorable fields
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
    type NativeType = T::NativeType;
    type NativeOwnedType = T::NativeOwnedType;

    fn export(&self) -> Self::NativeOwnedType {
        self.get_inner().export()
    }

    fn destroy(self: Pin<&mut Self>) {
        Self::get_inner_mut(self).destroy();
    }

    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType) {
        // Safety: The returned lifetime is tied to `self`
        unsafe {
            let target = T::allocate_from(external);
            let new_index = NoatunContext.index_of(&*target);
            NoatunContext.write(
                new_index,
                Pin::new_unchecked(&mut self.get_unchecked_mut().object_index),
            );
        }
    }

    unsafe fn allocate_from<'a>(external: &Self::NativeType) -> Pin<&'a mut Self> {
        let mut pod: Pin<&mut Self> = NoatunContext.allocate_obj();
        pod.as_mut().init_from(external);
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
        // Safety: object_index is valid
        unsafe { self.object_index.access::<T>() }
    }
}

impl<T: Object + ?Sized> NoatunBox<T> {
    /// Synonym of `deref`. You usually don't have to call this.
    pub fn get_inner(&self) -> &T {
        if self.object_index.is_null() {
            panic!("get() called on an uninitialized (null) NoatunBox.");
        }
        // Safety: object_index is valid
        unsafe { self.object_index.access::<T>() }
    }

    /// This could have been a `DerefMut` impl, but unfortunately that doesn't work
    /// with a Pin self
    pub fn get_inner_mut(self: Pin<&mut Self>) -> Pin<&mut T> {
        // Safety: We don't move out of tself
        let tself = unsafe { self.get_unchecked_mut() };
        if tself.object_index.is_null() {
            panic!("get_mut() called on an uninitialized (null) NoatunBox.");
        }
        // Safety: object_index is valid
        unsafe { tself.object_index.access_mut::<T>() }
    }

    /// Create a new box instance pointing at the given address
    pub fn new(value: T::Ptr) -> Self {
        Self {
            object_index: value,
            phantom: Default::default(),
        }
    }

    /// Assign the given 'value' to self
    pub fn assign(self: Pin<&mut Self>, value: &T::NativeType) {
        // Safety: We don't move out of tself
        let tself = unsafe { self.get_unchecked_mut() };
        // Safety: We don't keep the reference, lifetime of root object doesn't end
        let target = unsafe { T::allocate_from(value) };
        let index = NoatunContext.index_of(&*target);
        NoatunContext.write_internal(index, &mut tself.object_index);
    }

    /// Allocate a new instance of Self
    ///
    /// # Safety
    ///
    /// The returned reference is allocated in the Noatun database.
    /// It must not be retained past the lifetime of the currently active [`crate::Database`]
    /// object. Before the reference is returned to safe code, it must be tied to the
    /// lifetime of the materialized view main root object.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn allocate<'a>(value: T) -> Pin<&'a mut Self>
    where
        T: Object<Ptr = ThinPtr>,
        T: NoatunStorable,
    {
        let mut this = NoatunContext.allocate::<NoatunBox<T>>();
        let mut target = NoatunContext.allocate::<T>();
        // Safety: We don't move out of the ref
        unsafe {
            *target.as_mut().get_unchecked_mut() = value;
        }
        // Safety: We don't move out of the ref
        unsafe {
            this.as_mut().get_unchecked_mut().object_index = NoatunContext.index_of(&*target);
        }
        this
    }
}

#[allow(non_local_definitions)]
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
            let mut map = unsafe { map.map_unchecked_mut(|x| &mut x.0) };
            assert_eq!(map.len(), 0);

            map.as_mut().untracked_entry(42).or_insert(&420);

            let val = map.get(&42).unwrap();
            assert_eq!(val.get(), 420);

            let mut entry = map.as_mut().untracked_entry(42);
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
            let val = map.get(&42).unwrap();
            assert_eq!(val.get(), 420);

            let entry = map.as_mut().untracked_entry(30);
            let v = entry.or_default();
            assert_eq!(v.get(), 0);

            let val = map.get(&30).unwrap();
            assert_eq!(val.get(), 0);

            let val = match map.as_mut().untracked_entry(50) {
                NoatunHashMapEntry::Occupied(_) => {
                    unreachable!()
                }
                NoatunHashMapEntry::Vacant(vac) => vac.insert(&500),
            };
            assert_eq!(val.get(), 500);

            let val = map.get(&50).unwrap();
            assert_eq!(val.get(), 500);
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_entry1() {
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
            let mut map = unsafe { map.map_unchecked_mut(|x| &mut x.0) };
            assert_eq!(map.len(), 0);

            map.insert_internal(42, &42);
            match map.as_mut().untracked_entry(42) {
                NoatunHashMapEntry::Occupied(mut occ) => {
                    assert_eq!(**occ.get(), 42);
                    assert_eq!(**occ.get_mut(), 42);
                    occ.insert(&43);
                }
                NoatunHashMapEntry::Vacant(_) => {}
            }
            let val = map.get(&42).unwrap();
            assert_eq!(val.get(), 43);
            match map.as_mut().untracked_entry(42) {
                NoatunHashMapEntry::Occupied(occ) => {
                    occ.remove();
                }
                NoatunHashMapEntry::Vacant(_) => {}
            }
            assert!(map.is_empty());
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
            let mut map = unsafe { map.map_unchecked_mut(|x| &mut x.0) };
            assert_eq!(map.len(), 0);

            map.insert_internal(42, &42);

            let val = map.get(&42).unwrap();
            assert_eq!(val.get(), 42);

            let vals: Vec<u32> = map.iter().map(|(k, _)| *k).collect();
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
        db.with_root_mut(|map| {
            let mut map = map.inner_mut();
            map.insert_internal("hello", &42);
            assert_eq!(**map.get("hello").unwrap(), 42);
            assert_eq!(map.as_mut().untracked_pop("hello"), Some(42));
            assert!(map.get("hello").is_none());
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_export() {
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

            let reg_map: Vec<_> = map.0.export().into_iter().collect();
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

            let mut hm = indexmap::IndexMap::new();
            hm.insert("world".to_string(), 43);
            map.as_mut().init_from(&hm);

            let mut reg_map: Vec<_> = map.export().into_iter().collect();
            reg_map.sort();
            assert_eq!(
                &[("hello".to_string(), 42), ("world".to_string(), 43)],
                &*reg_map
            );
        })
        .unwrap();
    }
    #[test]
    fn test_hashmap_miri_export2() {
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

            let reg_map: Vec<_> = map.0.export().into_iter().collect();
            assert_eq!(&[("hello".to_string(), "world".to_string())], &*reg_map);
            map.as_mut().inner_pin().remove("hello");
            let reg_map: Vec<_> = map.0.export().into_iter().collect();
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

            let mut hm = indexmap::IndexMap::new();
            hm.insert(1, 1);

            noatun_box.as_mut().init_from(&hm);

            let items: Vec<_> = noatun_box.iter().map(|x| (*x.0, x.1.get())).collect();
            assert_eq!(items, [(1, 1)]);
        })
        .unwrap();
    }
}
