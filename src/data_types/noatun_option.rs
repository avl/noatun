use savefile_derive::Savefile;
use crate::{NoatunStorable, SchemaHasher};

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
