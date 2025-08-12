use crate::{
    get_context_mut_ptr, get_context_ptr, DatabaseContextData,
    NoatunContext, NoatunStorable, Object, Pointer, SchemaHasher, ThinPtr,
};

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref};
use std::pin::Pin;

pub use noatun_string::NoatunString;
pub use noatun_option::NoatunOption;
pub use noatun_cell::{NoatunCell, OpaqueNoatunCell, NoatunCellArrayExt};
pub use noatun_vec::{NoatunVec, OpaqueNoatunVec, NoatunVecIterator, NoatunVecIteratorMut};
pub(crate) use noatun_vec::{RawDatabaseVec, NoatunVecRaw};
pub use noatun_hash_map::{NoatunHashMap, NoatunHashMapEntry, NoatunHashMapIterator, NoatunKey};



mod noatun_hash_impls;
mod noatun_string;
mod noatun_option;
mod noatun_cell;
mod noatun_vec;
mod noatun_hash_map;



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
        Self::get_inner_mut(self).destroy();
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
            let mut map = unsafe { map.map_unchecked_mut(|x|&mut x.0) };
            assert_eq!(map.len(), 0);

            map.as_mut().entry(42).or_insert(&420);

            let val = map.get(&42).unwrap();
            assert_eq!(val.get(), 420);

            let mut entry = map.as_mut().entry(42);
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

            let entry = map.as_mut().entry(30);
            let v = entry.or_default();
            assert_eq!(v.get(), 0);

            let val = map.get(&30).unwrap();
            assert_eq!(val.get(), 0);

            let val = match map.as_mut().entry(50) {
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
            let mut map = unsafe { map.map_unchecked_mut(|x|&mut x.0) };
            assert_eq!(map.len(), 0);

            map.insert_internal(42, &42);
            match map.as_mut().entry(42) {
                NoatunHashMapEntry::Occupied(mut occ) => {
                    assert_eq!(**occ.get(), 42);
                    assert_eq!(**occ.get_mut(), 42);
                    occ.insert(&43);
                }
                NoatunHashMapEntry::Vacant(_) => {}
            }
            let val = map.get(&42).unwrap();
            assert_eq!(val.get(), 43);
            match map.as_mut().entry(42) {
                NoatunHashMapEntry::Occupied(mut occ) => {
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
            let mut map = unsafe { map.map_unchecked_mut(|x|&mut x.0) };
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
        db.with_root_mut(|mut map| {
            let mut map = map.inner_mut();
            map.insert_internal("hello", &42);
            assert_eq!(**map.get("hello").unwrap(), 42);
            assert_eq!(map.as_mut().pop("hello"), Some(42));
            assert!(map.get("hello").is_none());
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
