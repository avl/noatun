use crate::{NoatunPod, NoatunStorable, SchemaHasher};


/// Noatun equivalent to [`Option`].
///
/// Note that this is not a [`crate::Object`]. To use an Option, it must
/// be placed in a [`crate::data_types::NoatunCell`] or similar.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct NoatunOption<T: NoatunPod> {
    value: T,
    present: u8,
}

unsafe impl<T: NoatunPod> NoatunStorable for NoatunOption<T> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunOption/1");
        T::hash_schema(hasher);
    }
}

impl<T: NoatunPod> NoatunOption<T> {
    /// Convert the given NoatunOption to an std Option
    pub fn into_option(self) -> Option<T> {
        self.into()
    }
    /// Returns true if the option has a value
    pub fn is_some(&self) -> bool {
        self.present == 1
    }
    /// Returns true if the option has no value
    pub fn is_none(&self) -> bool {
        self.present == 0
    }
}

impl<T: NoatunPod> From<Option<T>> for NoatunOption<T> {
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
impl<T: NoatunPod> From<NoatunOption<T>> for Option<T> {
    fn from(value: NoatunOption<T>) -> Self {
        if value.present == 0 {
            None
        } else {
            Some(value.value)
        }
    }
}
