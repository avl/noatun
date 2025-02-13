use crate::{MessageId, NoatunTime};
use anyhow::{anyhow, bail, Result};
use bytemuck::{Pod, Zeroable};
use savefile_derive::Savefile;
use std::fmt::{Debug, Formatter};

pub(crate) struct CutOffConfig {
    /// The approximate time in history at which all nodes must have been in sync.
    /// I.e, all nodes are expected to eventually sync up. I.e, all nodes are expected
    /// to have all messages created prior to (now - interval_ms).next_multiple_of(grace_period_ms)
    pub(crate) age: CutOffDuration,
    pub(crate) stride: CutOffDuration,
}

impl Default for CutOffConfig {
    fn default() -> Self {
        Self {
            age: CutOffDuration(1440),
            stride: CutOffDuration(60),
        }
    }
}
impl CutOffConfig {
    pub fn new(age: CutOffDuration) -> Result<Self> {
        if age.0 < 4 {
            bail!("CutOffConfig::new called with an invalid value '{:?}'. Minimum cutoff time is 4 minutes.", age);
        }
        let stride = CutOffDuration((age.0 / 10).max(1));
        //println!("Stride: {:?}", stride);
        Ok(Self { age, stride })
    }
    pub fn nominal_cutoff(&self, time_now: CutOffTime) -> CutOffTime {
        CutOffHashPos::nominal_cutoff(time_now, self)
    }
}

#[derive(Savefile, Clone, Copy, Pod, Zeroable, PartialEq, Eq, Default)]
#[repr(C)]
pub struct CutoffHash {
    values: [u64; 2],
}

impl Debug for CutoffHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let data: &[u32] = bytemuck::cast_slice(&self.values);

        write!(
            f,
            "{:x}:{:x}-{:x}-{:x}-{:x}",
            data[0],
            data[0] & 0xffff,
            (data[1] & 0xffff0000) >> 16,
            data[2],
            data[3]
        )
    }
}

impl CutoffHash {
    pub(crate) const ZERO: CutoffHash = CutoffHash { values: [0, 0] };
    /*pub(crate) fn from_all(msg: &[MessageId]) -> CutoffHash {
        let mut temp = CutoffHash::default();
        for m in msg {
            temp.xor_with_msg(*m);
        }
        temp
    }*/
    pub fn from_msg(msg: MessageId) -> CutoffHash {
        bytemuck::cast(msg)
    }
    fn xor_with(&mut self, other: CutoffHash) {
        self.values[0] ^= other.values[0];
        self.values[1] ^= other.values[1];
    }
    pub(crate) fn xor_with_msg(&mut self, other: MessageId) {
        self.xor_with(CutoffHash::from_msg(other));
    }
}

pub struct CutOffInterval(u32);

impl CutOffInterval {}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CutOffDuration(u32);

impl Debug for CutOffDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} minutes", self.0)
    }
}

impl CutOffDuration {
    pub fn from_minutes(minutes: u32) -> Self {
        Self(minutes)
    }
    pub fn from_hours(hours: u32) -> Result<Self> {
        Ok(Self(
            hours
                .checked_mul(60)
                .ok_or_else(|| anyhow!("hours value out of range"))?,
        ))
    }
    pub fn from_days(days: u32) -> Result<Self> {
        let minutes = days
            .checked_mul(24 * 60)
            .ok_or_else(|| anyhow!("days value out of range"))?;
        Ok(Self(minutes))
    }
}

#[derive(Clone, Copy, Pod, Zeroable, Savefile, PartialEq, Eq, PartialOrd, Ord, Default)]
#[repr(C)]
pub struct CutOffTime(u32 /*minutes since unix epoch*/);

impl Debug for CutOffTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CutOffTime({:?})", self.to_noatun_time())
    }
}

impl From<CutOffTime> for NoatunTime {
    fn from(value: CutOffTime) -> Self {
        NoatunTime((value.0 as u64) * (60 * 1000))
    }
}

impl CutOffTime {
    pub fn saturating_sub(self, other: CutOffDuration) -> CutOffTime {
        CutOffTime(self.0.saturating_sub(other.0))
    }
    pub fn from_noatun_time(noatun_time: NoatunTime) -> CutOffTime {
        // NoatunTime time has a range of millions of years into the future.
        // CutOffTime only has a range of "just" thousands of years.
        // Let's not make from_noatun_time fallible just because of this
        match (noatun_time.0 / (60 * 1000)).try_into() {
            Ok(minutes) => CutOffTime(minutes),
            Err(_) => CutOffTime(u32::MAX),
        }
    }
    pub fn to_noatun_time(self) -> NoatunTime {
        NoatunTime((self.0 as u64) * (60 * 1000))
    }
    pub fn truncate_from(noatun_time: NoatunTime) -> Result<CutOffTime> {
        Ok(CutOffTime((noatun_time.0 / (1000 * 60)).try_into()?))
    }
}

#[derive(Clone, Copy, Debug, Pod, Zeroable, Savefile, PartialEq, Eq)]
#[repr(C)]
pub struct CutOffHashPos {
    pub(crate) hash: CutoffHash,
    /// Era
    pub(crate) before_time: CutOffTime,
    padding: u32,
}

#[derive(Clone, Copy, Debug, Pod, Zeroable, Savefile)]
#[repr(C)]
pub struct CutOffState {
    /// The prior, the current, and the upcoming,
    stamps: CutOffHashPos,
}

pub enum Acceptability {
    /// The hashes are identical. This is the nominal case
    Nominal,
    /// The hashes are definitely incompatible
    Unacceptable,
    /// A peer clock appears to be more than one 'stride' off. I.e, if the
    /// cutoff stride is 60 minutes, and we get this error, a peer has a clock that is more
    /// than 60 minutes off.
    /// This condition means our node and the peer are not on nearby eras, and consistency
    /// cannot be determined. It is possible the nodes are actually in sync, we just can't know.
    UnacceptablePeerClockDrift,
    /// The peer hash is from a later era. We need to advance to that era, to determine
    /// if the hashes are compatible or not.
    Undecided(CutOffTime /*peer era*/),
}

impl CutOffHashPos {
    pub(crate) fn is_acceptable_cutoff_hash(
        &self,
        peer_hash: CutOffHashPos,
        config: &CutOffConfig,
    ) -> Acceptability {
        if *self == peer_hash {
            return Acceptability::Nominal;
        }
        if self.before_time < peer_hash.before_time {
            if self.before_time < peer_hash.before_time.saturating_sub(config.stride) {
                return Acceptability::UnacceptablePeerClockDrift;
            }

            return Acceptability::Undecided(peer_hash.before_time);
        }
        Acceptability::Unacceptable
    }
    pub fn nominal_hash(&self) -> CutoffHash {
        self.hash
    }
    /// Now rounded to the nearest multiple of stride
    fn nominal_cutoff(now: CutOffTime, config: &CutOffConfig) -> CutOffTime {
        CutOffTime(
            now.0
                .saturating_sub(config.age.0 + config.stride.0)
                .next_multiple_of(config.stride.0),
        )
    }

    pub fn report_add(&mut self, message_id: MessageId) {
        self.apply(message_id, "add");
    }
    pub fn report_delete(&mut self, message_id: MessageId) {
        self.apply(message_id, "delete");
    }

    /// Add and delete are logically identical ops (because xor)
    pub(crate) fn apply(&mut self, message_id: MessageId, _op: &str) {
        let t = message_id.timestamp();

        if t < self.before_time.to_noatun_time() {
            //let prev = self.hash;
            self.hash.xor_with_msg(message_id);
            //println!("Xoring out message {:?} ({}), giving hash: {:?} (prev: {:?})", message_id, op, self.hash, prev);
        } else {
            //println!("Op at {:?} was not before cutoff-period {:?} ({})", t, self.before_time, op);
        }
        //println!(" == {} {:?} Resulting hash: {:?}", op, message_id, self);
    }
}
