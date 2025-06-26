use crate::NoatunStorable;
use std::fmt::{Debug, Display, Formatter};


/// Sequence number of a message.
/// 
/// Each message applied to the database is given a sequence number.
/// The numbers are monotonically increasing, without gaps.
#[derive(Copy, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(C)]
// 0 is an invalid sequence number, used to represent 'not a number'
pub struct SequenceNr(u32);

unsafe impl NoatunStorable for SequenceNr {}

impl Display for SequenceNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "#INVALID")
        } else {
            write!(f, "#{}", self.0 - 1)
        }
    }
}
impl Debug for SequenceNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "#INVALID")
        } else {
            write!(f, "#{}", self.0 - 1)
        }
    }
}

impl SequenceNr {
    pub const INVALID: SequenceNr = SequenceNr(0);
    pub fn is_invalid(self) -> bool {
        self.0 == 0
    }
    pub fn is_valid(self) -> bool {
        self.0 != 0
    }
    pub fn successor(self) -> SequenceNr {
        SequenceNr(self.0 + 1)
    }
    pub fn from_index(index: usize) -> SequenceNr {
        if index >= (u32::MAX - 1) as usize {
            panic!("More than 2^32 elements created. Not supported by noatun");
        }
        SequenceNr(index as u32 + 1)
    }
    pub fn index(self) -> usize {
        if self.0 == 0 {
            panic!("0 SequenceNr does not have an index")
        }
        self.0 as usize - 1
    }
    pub fn try_index(self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        Some(self.0 as usize - 1)
    }
}
