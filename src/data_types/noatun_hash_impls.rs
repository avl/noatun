use crate::data_types::NoatunKey;
use std::hash::Hasher;
use std::pin::Pin;

macro_rules! noatun_hash_primitive {
    ($t: ident, $tm: ident) => {
        impl NoatunKey for $t {
            type DetachedType = $t;
            type DetachedOwnedType = $t;
            fn hash<H>(tself: &Self::DetachedType, state: &mut H)
            where
                H: Hasher,
            {
                state.$tm(*tself);
            }
            fn destroy(&mut self) {}
            fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
                let tself = unsafe { self.get_unchecked_mut() };
                *tself = *detached;
            }
            fn detach_key(&self) -> Self::DetachedType {
                *self
            }
            fn detach_key_ref(&self) -> &Self::DetachedType {
                self
            }
            fn eq(a: &Self::DetachedType, b: &Self::DetachedType) -> bool {
                a == b
            }
        }
    };
}

noatun_hash_primitive!(usize, write_usize);
noatun_hash_primitive!(isize, write_isize);
noatun_hash_primitive!(u8, write_u8);
noatun_hash_primitive!(u16, write_u16);
noatun_hash_primitive!(u32, write_u32);
noatun_hash_primitive!(u64, write_u64);
noatun_hash_primitive!(u128, write_u128);
noatun_hash_primitive!(i8, write_i8);
noatun_hash_primitive!(i16, write_i16);
noatun_hash_primitive!(i32, write_i32);
noatun_hash_primitive!(i64, write_i64);
noatun_hash_primitive!(i128, write_i128);
