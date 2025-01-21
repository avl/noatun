use crate::MessageId;
use crate::message_store::IndexEntry;
use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Utc};
use savefile_derive::Savefile;

pub(crate) struct CutOffConfig {
    /// The approximate time in history at which all nodes must have been in sync.
    /// I.e, all nodes are expected to eventually sync up. I.e, all nodes are expected
    /// to have all messages created prior to (now - interval_ms).next_multiple_of(grace_period_ms)
    age: u64,
    stride: u64,
}

impl Default for CutOffConfig {
    fn default() -> Self {
        Self {
            age: 86_400_000,
            stride: 3_600_000,
        }
    }
}
impl CutOffConfig {
    pub fn nominal_cutoff(&self, time_now: DateTime<Utc>) -> u64 {
        CutOffState::nominal_now(time_now.timestamp_millis() as u64, self) //TODO: Try_into
    }
}

#[derive(Savefile, Clone, Copy, Debug, Pod, Zeroable, PartialEq, Eq, Default)]
#[repr(C)]
pub struct CutoffHash {
    values: [u64; 2],
}

impl CutoffHash {
    pub(crate) fn from_all(msg: &[MessageId]) -> CutoffHash {
        let mut temp = CutoffHash::default();
        for m in msg {
            temp.xor_with_msg(*m);
        }
        temp
    }
    fn from(msg: MessageId) -> CutoffHash {
        bytemuck::cast(msg)
    }
    fn xor_with(&mut self, other: CutoffHash) {
        self.values[0] ^= other.values[0];
        self.values[1] ^= other.values[1];
    }
    fn xor_with_msg(&mut self, other: MessageId) {
        self.xor_with(CutoffHash::from(other));
    }
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
struct CutOffHashPos {
    hash: CutoffHash,
    before_time: u64,
}

impl CutOffHashPos {
    fn adjust_forward_to(&mut self, time: u64, messages: &[IndexEntry]) {
        assert!(self.before_time <= time);
        if self.before_time == time {
            return; //Nothing to do
        }
        let prior = MessageId::from_parts_raw(self.before_time, [0u8; 10]).unwrap();
        let mut cur_index = match messages.binary_search_by_key(&prior, |x| x.message) {
            Ok(hit) => hit,
            Err(insloc) => insloc,
        };
        while cur_index < messages.len() {
            let cur = &messages[cur_index];
            if cur.message.timestamp() >= time {
                // Done
                return;
            }
            self.hash.xor_with_msg(cur.message);
            cur_index += 1;
        }
        self.before_time = time;
    }
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub(crate) struct CutOffState {
    /// The prior, the current, and the upcoming,
    stamps: [CutOffHashPos; 3],
}

impl CutOffState {
    pub fn is_acceptable_cutoff_hash(&self, hash: CutoffHash) -> bool {
        self.stamps.iter().any(|x| x.hash == hash)
    }
    pub fn nominal_hash(&self) -> CutoffHash {
        self.stamps[1].hash
    }
    /// Now rounded to the nearest multiple of stride
    fn nominal_now(now: u64, config: &CutOffConfig) -> u64 {
        now.saturating_sub(config.stride / 2)
            .next_multiple_of(config.stride)
    }
    pub fn advance_time(&mut self, now: u64, config: &CutOffConfig, messages: &[IndexEntry]) {
        let nominal_now = Self::nominal_now(now, config);
        let prior = nominal_now.saturating_sub(config.stride);
        let next = nominal_now.saturating_add(config.stride);

        self.stamps[0].adjust_forward_to(prior, messages);
        self.stamps[1].adjust_forward_to(nominal_now, messages);
        self.stamps[2].adjust_forward_to(next, messages);
    }

    pub fn report_add(&mut self, message_id: MessageId) {
        self.apply(message_id);
    }
    pub fn report_delete(&mut self, message_id: MessageId) {
        self.apply(message_id);
    }

    /// Add and delete are logically identical ops (because xor)
    fn apply(&mut self, message_id: MessageId) {
        let t = message_id.timestamp();
        for pos in &mut self.stamps {
            if t < pos.before_time {
                pos.hash.xor_with_msg(message_id);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::{CutOffHashPos, CutoffHash};
    use crate::MessageId;
    use crate::message_store::IndexEntry;

    #[test]
    fn test_advance_pos() {
        let mut pos = CutOffHashPos {
            hash: CutoffHash::from(MessageId::new_debug(0)),
            before_time: 100,
        };

        pos.adjust_forward_to(201, &[IndexEntry {
            message: MessageId::from_parts_raw(200, [0u8; 10]).unwrap(),
            file_offset: crate::message_store::FileOffset::deleted(),
            file_total_size: 0,
        }]);

        assert_eq!(
            pos.hash,
            CutoffHash::from(MessageId::from_parts_raw(200, [0u8; 10]).unwrap())
        );
    }
}
