use crate::sequence_nr::{SequenceNr, Tracker};
use crate::xxh3_vendored::NoatunHasher;
use crate::{
    get_context_ptr, FixedSizeObject, MessageId, NoatunContext, NoatunStorable, Object,
    SchemaHasher, ThinPtr,
};
use indexmap::IndexMap;
pub use meta_finder::get_any_empty;
use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Add;
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::slice;
use tracing::trace;

const HASH_META_GROUP_SIZE: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
struct NoatunHashBucket<K, V> {
    hash: u32,
    key: K,
    v: V,
}

// Safety: NoatunHashBucket<K,V> contains only NoatunStorable fields
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
/// This is a non-opaque type that supports reading during materialization. It contains
/// a single tracker of "clear" type. This tracker records only the latest message to
/// clear the hashmap. This in turn means that clearing a hashmap does not mark a message
/// as a tombstone, but does make the message own the "clear"-tracker. The advantage compared
/// to a tombstone is that subsequent calls to "clear" will take ownership, allowing the
/// previous owner to be pruned.
///
/// # Note regarding [`NoatunHashMap::len`].
/// The len method is not available during materialized view-building. The reason is that
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
/// T=2: [`NoatunHashMap::untracked_contains_key`] is called with parameter 'A' and the presence of 'A'  is stored
/// into field X.
///
/// T=3: Key 'A' is removed from hashmap
///
/// In this case, at T=2 Noatun will record the dependency by the second message on the first
/// message. The first message will not be pruned when 'A' is removed from the hashmap. Not until
/// the field X is overwritten (without dependency on the previous value) will the first message
/// be pruned.
#[repr(C)]
pub struct NoatunHashMap<K: NoatunStorable, V: FixedSizeObject> {
    length: usize,
    capacity: usize,
    data: usize,

    // By having a tracker for any "clear" calls, we can avoid
    // accumulating multiple "tombstone" messages when clearing hashmaps.
    // The downside is that each hashmap retains a clearing message
    // even _after_ the cutoff (since it remains in the db).
    // Observes any "clear" calls. This allows us to not register
    // such actions as 'tombstones'.
    clear_tracker: Tracker,

    phantom_data: PhantomData<(K, V)>,
}

// Safety: NoatunHashMap<K,V> contains only NoatunStorable fields
unsafe impl<K: NoatunStorable, V: FixedSizeObject> NoatunStorable for NoatunHashMap<K, V> {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::NoatunHashMap/1(");
        K::hash_schema(hasher);
        V::hash_schema(hasher);
        hasher.write_str(")");
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

impl Add<usize> for BucketNr {
    type Output = BucketNr;

    fn add(self, rhs: usize) -> Self::Output {
        Self(self.0 + rhs)
    }
}

#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct BucketProbeSequence {
    cur_group: usize,
    group_capacity: usize,
    group_step: usize,
}

impl BucketProbeSequence {
    #[doc(hidden)]
    pub fn new(start_group: MetaGroupNr, total_group_count: usize) -> BucketProbeSequence {
        BucketProbeSequence {
            cur_group: start_group.0,
            group_capacity: total_group_count,
            group_step: 1,
        }
    }
    pub(crate) fn probe_next(&mut self) -> Option<MetaGroupNr> {
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

// Safety: Meta contains only NoatunStorable fields
unsafe impl NoatunStorable for Meta {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::data_types::Meta/1");
    }
}

#[repr(align(32))]
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct MetaGroup(pub [Meta; HASH_META_GROUP_SIZE]);

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

/// Result of running a hashmap probing sequence
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ProbeRunResult {
    /// The hashmap is full. No empty bucket can thus be found.
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
    use super::{BucketNr, Meta, MetaGroup, ProbeRunResult};

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

/// Iterator for [`NoatunHashMap`]
pub struct NoatunHashMapIterator<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject>
{
    hash_buckets: &'a [MaybeUninit<NoatunHashBucket<K, V>>],
    metas: &'a [MetaGroup],
    next_position: usize,
}

/// Iterator for [`NoatunHashMap`] that produces owned values
///
/// Not exposed externally
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
                // Safety: If meta is populated, the bucket must have been initialized
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
                // Safety: If meta is populated, the bucket must have been initialized
                let bucket = unsafe { self.hash_buckets.buckets[pos].assume_init_mut() };
                let key_p = &mut bucket.key as *mut K;
                let v_p = &mut bucket.v as *mut V;
                // Safety: bucket is a valid object, so the two pointers must also be valid.
                // The key and value are NoatunStorable, which guarantees no Drop impl
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
///
/// WARNING!
/// Matching on this enum is dangerous, since the [`NoatunHashMap::untracked_entry`] does
/// not establish a read dependency.
pub enum NoatunHashMapEntry<'a, K, V>
where
    K: NoatunStorable + NoatunKey + PartialEq,
    V: FixedSizeObject,
    K::NativeOwnedType: Sized,
{
    /// The key has a value
    ///
    /// WARNING!
    /// Matching on this enum is dangerous, since the [`NoatunHashMap::untracked_entry`] does
    /// not establish a read dependency.
    Occupied(OccupiedEntry<'a, K, V>),
    /// The key is not associated with a value, but a value can be inserted
    ///
    /// WARNING!
    /// Matching on this enum is dangerous, since the [`NoatunHashMap::untracked_entry`] does
    /// not establish a read dependency.
    Vacant(VacantEntry<'a, K, V>),
}

struct NoatunHashMapEntryInternal<'a, K, V>
where
    K: NoatunStorable + NoatunKey + PartialEq,
    V: FixedSizeObject,
    K::NativeOwnedType: Sized,
{
    context: HashAccessContextMut<'a, K, V>,
    probe_result: (BucketNr, Meta),
    key: K::NativeOwnedType,
    length: &'a mut usize,
}

/// Entry representing the absence of a value for a particular key
pub struct VacantEntry<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject>
where
    K::NativeOwnedType: Sized,
{
    context: HashAccessContextMut<'a, K, V>,
    probe_result: (BucketNr, Meta),
    key: K::NativeOwnedType,
    length: &'a mut usize,
}
/// Entry representing the value for a particular key
pub struct OccupiedEntry<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject>
where
    K::NativeOwnedType: Sized,
{
    context: HashAccessContextMut<'a, K, V>,
    probe_result: (BucketNr, Meta),
    key: K::NativeOwnedType,
    length: &'a mut usize,
}

impl<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> VacantEntry<'a, K, V>
where
    K::NativeType: Sized,
{
    /// Insert the given value in this entry.
    ///
    /// This does not itself update any tracker or cause any read dependency.
    /// However, the value must contain a tracker, and ownership of this will be gained.
    pub fn insert(self, v: &V::NativeType) -> Pin<&'a mut V> {
        NoatunHashMap::insert_at_bucket(
            true,
            self.probe_result,
            self.key.borrow(),
            self.context,
            |new, val| {
                if new {
                    val.init_from(v)
                }
            },
            self.length,
        )
    }
}

impl<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> OccupiedEntry<'a, K, V>
where
    K::NativeOwnedType: Sized,
{
    /// Get a shared reference to the value of this entry
    ///
    /// This itself causes no read dependency, but the operation is useless unless
    /// the returned value is actually read, which should cause a read dependency.
    pub fn get(&self) -> &V {
        let bucket_nr = self.probe_result.0;
        // Safety: The entry is occupied, so it must have been initialized
        unsafe { &self.context.buckets[bucket_nr.0].assume_init_ref().v }
    }
    /// Get a mutable reference to the value of this entry
    ///
    /// This does not itself cause a write to occur.
    ///
    /// This itself causes no read dependency, but the operation is useless unless
    /// the returned value is actually read, which should cause a read dependency.
    pub fn get_mut(&mut self) -> Pin<&mut V> {
        let bucket_nr = self.probe_result.0;
        // Safety: The entry is occupied, so it must have been initialized
        unsafe { Pin::new_unchecked(&mut self.context.buckets[bucket_nr.0].assume_init_mut().v) }
    }

    /// Insert a new value for the entry.
    ///
    /// This does not itself update any tracker or cause any read dependency.
    /// However, the value must contain a tracker, and ownership of this will be gained.
    pub fn insert(&mut self, v: &V::NativeType) -> V::NativeOwnedType {
        let val = self.get_mut();
        let ret = val.export();
        val.init_from(v);
        ret
    }

    /// Remove the current entry from the map.
    ///
    /// This does not cause a read dependency.
    ///
    /// This will destroy the value, which will release any trackers contained therein.
    pub fn remove(mut self) -> V::NativeOwnedType {
        let bucket_nr = self.probe_result.0;
        let val = self.get();
        let ret = val.export();

        trace!("removing HashMap key using entry");
        NoatunHashMap::remove_impl_by_bucket_nr(&mut self.context, bucket_nr, |_| {});
        let newlen = *self.length - 1;
        NoatunContext.write_internal(newlen, self.length);

        ret
    }
}
impl<'a, K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> NoatunHashMapEntry<'a, K, V>
where
    K::NativeOwnedType: Sized,
{
    fn make_enum(
        probe_result: ProbeRunResult,
        context: HashAccessContextMut<'a, K, V>,
        key: K::NativeOwnedType,
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

    /// Return the value of the entry, or if it is vacant, insert a new value
    /// and return it. Inserted values are initialized with an all-zero bit pattern.
    pub fn or_default(self) -> Pin<&'a mut V> {
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
    /// Insert a new value for the key, and return a reference to the inserted value.
    ///
    /// This does not cause any read dependencies.
    pub fn or_insert(self, v: &V::NativeType) -> Pin<&'a mut V> {
        let new = matches!(self, NoatunHashMapEntry::Vacant(_));
        let tself = self.unify();
        NoatunHashMap::insert_at_bucket(
            new,
            tself.probe_result,
            tself.key.borrow(),
            tself.context,
            |new, val| {
                if new {
                    val.init_from(v)
                }
            },
            tself.length,
        )
    }

    /// Insert a new value for the key, and return a reference to the inserted value.
    ///
    /// The value to insert is provided by closure 'v'.
    ///
    /// This does not cause any read dependencies.
    pub fn or_insert_with(self, v: impl FnOnce() -> V::NativeOwnedType) -> Pin<&'a mut V> {
        let new = matches!(self, NoatunHashMapEntry::Vacant(_));
        let tself = self.unify();
        NoatunHashMap::insert_at_bucket(
            new,
            tself.probe_result,
            tself.key.borrow(),
            tself.context,
            |new, val| {
                if new {
                    val.init_from(v().borrow())
                }
            },
            tself.length,
        )
    }
}

impl<K: NoatunStorable + NoatunKey + PartialEq, V: FixedSizeObject> NoatunHashMap<K, V> {
    fn assert_not_apply(&self, method: &str, untracked_version_available: bool) {
        // Safety: get_context_ptr returns valid pointers
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
    /// This method panics if called from within [`crate::Message::apply`]. But also see
    /// [`Self::untracked_len`].
    pub fn len(&self) -> usize {
        self.assert_not_apply("len", true);

        self.length
    }

    /// Return the number of elements in the hashmap
    ///
    /// Future pruning of unrelated messages may affect the result of this method.
    /// Only use the return value for logging/debugging, or uses where the numerical
    /// value will not be used as a decision factor in any logic.
    ///
    /// This does not record a read dependency.
    pub fn untracked_len(&self) -> usize {
        self.length
    }

    /// Returns true if there are no elements in the map.
    ///
    /// This method panics if called from within [`crate::Message::apply`]. But also see
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
    /// It is strongly recommended that applications do not count the number of iterated values, or
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
    ///
    /// # Safety
    /// Must only iterate mutably once at a time
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

        // Safety: dptr is valid
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

        // Safety: dptr is valid
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
        key: &K::NativeType,
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
                    // Safety: probe sequence only visits populated buckets
                    unsafe { buckets[bucket_nr.0].assume_init_ref().key.export_key_ref() },
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
    }

    /// Remove any items in the map for which the predicate returns true.
    ///
    /// This method does not record any read-dependency on the map. Applications are
    /// encouraged not to count the number of invocations of the predicate, or if they do,
    /// to not base any logic on the numeric value.
    pub fn retain(self: Pin<&mut Self>, mut predicate: impl FnMut(&K, Pin<&mut V>) -> bool) {
        // Safety: We don't move out of the ref
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
                // Safety: Populated buckets are always initialized
                let bucket = unsafe { context.buckets[i].assume_init_mut() };
                // Safety: We don't move out of the ref
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
                // Safety: Populated buckets are always initialized
                let kv = unsafe { context.buckets[i].assume_init_mut() };
                // Safety: We don't move out of the ref
                let val = unsafe { Pin::new_unchecked(&mut kv.v) };
                val.destroy();
                kv.key.destroy();
                NoatunContext.zero_internal(kv);
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
    /// that after 'clear', Noatun will be able to prune all previous messages that
    /// owned any tracker that used to be in the map, including those messages invoking 'clear'.
    pub fn clear(self: Pin<&mut Self>) {
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };

        // Safety: `tself.clear_tracker` is a valid object
        unsafe { NoatunContext.update_tracker_ptr(addr_of_mut!(tself.clear_tracker), true) };

        NoatunContext.write_internal(0, &mut tself.length);
        tself.clear_impl();
    }

    /// General purpose bucket probe
    fn probe(context: HashAccessContext<K, V>, key: impl Borrow<K::NativeType>) -> ProbeRunResult {
        let HashAccessContext { metas, buckets } = context;
        let cap = buckets.len();
        if cap == 0 {
            return ProbeRunResult::HashFull;
        }
        let mut h = NoatunHasher::new();
        let key = key.borrow();
        K::hash(key, &mut h);

        let (meta_group_nr, key_meta) = MetaGroupNr::from_u64(h.finish(), cap);

        let probe = BucketProbeSequence::new(meta_group_nr, cap.div_ceil(HASH_META_GROUP_SIZE));

        run_insert_probe_sequence(
            metas,
            cap,
            key_meta,
            |bucket_nr|
                // Safety: The probe sequence only visits populated buckets, which are always
                // initialized
                unsafe {
                <K as NoatunKey>::eq(
                    buckets[bucket_nr.0].assume_init_ref().key.export_key_ref(),
                    key,
                )
            },
            probe,
        )
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

    /// Return the value associated with key 'key', if present.
    ///
    /// This does not itself record a read dependency, though reading
    /// from the returned value presumably will.
    ///
    /// For a mutable version of this, see [`Self::get_mut_val`].
    #[inline]
    pub fn get(&self, key: &K::NativeType) -> Option<&V> {
        let context = self.data_meta_len();
        let bucket = Self::probe_read(context, key)?;
        // Safety: When probe_read returns Some, that bucket contains the searched key,
        // and is thus initialized.
        unsafe { Some(&context.buckets[bucket.0].assume_init_ref().v) }
    }

    /// Returns true if the given key is present in the map.
    ///
    /// WARNING!
    /// Note, this does _not_ record a read dependency. Calling this method
    /// from within [`crate::Message::apply`] is not recommended. If a call is made,
    /// no decision affecting the materialized state may be made based on the
    /// value.
    pub fn untracked_contains_key(&self, key: &K::NativeType) -> bool {
        let context = self.data_meta_len();
        Self::probe_read(context, key).is_some()
    }

    /// Get a mutable reference to the value for `key`.
    ///
    /// Note, this does not record a read dependency, but reading from the
    /// returned value will.
    ///
    /// WARNING!
    /// Applications should make sure to either read from any returned value, or else
    /// base no decisions on the return value.
    pub fn get_mut_val<'a>(self: Pin<&'a mut Self>, key: &K::NativeType) -> Option<Pin<&'a mut V>> {
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        let context = tself.data_meta_len_mut();
        let bucket = Self::probe_read(context.readonly(), key)?;
        // Safety: probe_read found the sought key, it must be initialized.
        unsafe {
            Some(Pin::new_unchecked(
                &mut context.buckets[bucket.0].assume_init_mut().v,
            ))
        }
    }

    /// Return the value for the given key.
    ///
    /// If the key is not present in the map, insert it with an all-zero value.
    ///
    /// This does not itself create a read dependency, but reading from the contained
    /// value will.
    pub fn get_insert<'a>(mut self: Pin<&'a mut Self>, key: &K::NativeType) -> Pin<&'a mut V> {
        let context = self.data_meta_len();
        if let Some(bucket) = Self::probe_read(context, key) {
            trace!(bucket=?bucket, "Existing bucket found");
            // Safety: probe_read found the sought key, it must be initialized.
            unsafe {
                let context = self.get_unchecked_mut().data_meta_len_mut();
                return Pin::new_unchecked(&mut context.buckets[bucket.0].assume_init_mut().v);
            }
        }

        {
            // Safety: We don't move out of the ref
            let tself = unsafe { self.as_mut().get_unchecked_mut() };

            tself.insert_internal_impl(key, |_new, _target| {
                // Leave as zero
            });
        }

        self.get_mut_val(key).unwrap()
    }

    /// Remove the given key from the map.
    ///
    /// Return true if a value was removed.
    ///
    /// The current message will be marked as a tombstone (it will not be pruned until after
    /// the cutoff period has elapsed).
    ///
    /// This methods does not cause a read dependency.
    ///
    /// WARNING! No decision must be based on the return value, since this method
    /// does not cause a read dependency.
    pub fn remove_untracked(self: Pin<&mut Self>, key: &K::NativeType) -> bool {
        // Safety: We don't move out of the ref
        unsafe { self.get_unchecked_mut().remove_impl(key, |_| {}) }
    }

    /// Remove the given key from the map.
    ///
    /// The current message will be marked as a tombstone (it will not be pruned until after
    /// the cutoff period has elapsed).
    ///
    /// This does not cause a read dependency.
    pub fn remove(self: Pin<&mut Self>, key: &K::NativeType) {
        // Safety: We don't move out of the ref
        unsafe {
            self.get_unchecked_mut().remove_impl(key, |_| {});
        }
    }

    /// Return true if a value was removed
    #[cfg(test)]
    pub(crate) fn remove_internal(&mut self, key: &K::NativeType) -> bool {
        self.remove_impl(key, |_| {})
    }

    /// Remove and return the value for the given key.
    ///
    /// If the key is not present, None is returned.
    ///
    /// WARNING!
    /// This does not cause a read dependency. No decision should be made based on whether
    /// the key was present or not. If the key was present, it's acceptable to read the removed
    /// value and base decisions on it.
    pub fn untracked_pop(self: Pin<&mut Self>, key: &K::NativeType) -> Option<V::NativeOwnedType> {
        let mut retval = None;
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        tself.remove_impl(key, |val| {
            retval = Some(val.export());
        });
        retval
    }

    fn remove_impl(&mut self, key: &K::NativeType, getval: impl FnOnce(&mut Pin<&mut V>)) -> bool {
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
        // Safety: This is only called for populated buckets
        unsafe {
            let kv = context.buckets[bucket_nr.0].assume_init_mut();
            let mut val = Pin::new_unchecked(&mut kv.v);
            getval(&mut val);
            val.destroy();
            kv.key.destroy();
        };

        match get_meta_mut_and_emptyable(context.metas, bucket_nr) {
            MetaMutAndEmpty::NoEmpty(meta) => {
                // Safety: Non-empty bucket is initialized
                unsafe {
                    let kv = context.buckets[bucket_nr.0].assume_init_mut();
                    NoatunContext.zero_internal(kv);
                }
                NoatunContext.write_internal(Meta::DELETED, meta);
            }
            MetaMutAndEmpty::HasEmptyAfterMeta(meta) => {
                // Safety: Meta is populated and bucket is thus initialized
                unsafe {
                    let kv = context.buckets[bucket_nr.0].assume_init_mut();
                    NoatunContext.zero_internal(kv);
                }
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

                // Safety: before_empty_bucket is not itself empty
                unsafe {
                    NoatunContext.copy(
                        before_empty_bucket_obj.assume_init_ref(),
                        meta_bucket_obj.assume_init_mut(),
                    );
                    NoatunContext.zero_internal(before_empty_bucket_obj.assume_init_mut());
                }
            }
        }
    }

    /// Insert the given key value pair.
    ///
    /// If the key already contained a value, that value is returned.
    ///
    /// This does not cause a read dependency.
    pub fn insert_untracked(
        self: Pin<&mut Self>,
        key: impl Borrow<K::NativeType>,
        val: &V::NativeType,
    ) -> Option<V::NativeOwnedType>
    where
        V::NativeOwnedType: Sized,
    {
        let mut ret: Option<V::NativeOwnedType> = None;
        // Safety: We don't move out of the ref
        unsafe { self.get_unchecked_mut() }.insert_internal_impl(key, |new, target| {
            if !new {
                ret = Some(target.export());
            }
            V::init_from(target, val)
        });
        ret
    }

    /// Insert the given key value pair.
    ///
    /// This does not cause a read dependency.
    pub fn insert(self: Pin<&mut Self>, key: impl Borrow<K::NativeType>, val: &V::NativeType) {
        self.insert_untracked(key, val);
    }

    /// Return true if a value with the given key already existed. In this case, it is
    /// overwritten. If no previous value existed, one is inserted.
    ///
    /// This does not cause a read dependency.
    ///
    /// WARNING!
    /// Since this does not cause a read dependency, not decision must be made based
    /// on the return value.
    pub fn untracked_insert_fast(
        self: Pin<&mut Self>,
        key: impl Borrow<K::NativeType>,
        val: &V::NativeType,
    ) -> bool {
        let mut existed = false;
        // Safety: We don't move out of the ref
        unsafe { self.get_unchecked_mut() }.insert_internal_impl(key, |new, target| {
            if !new {
                existed = true;
            }
            V::init_from(target, val)
        });
        existed
    }

    pub(crate) fn insert_internal(&mut self, key: impl Borrow<K::NativeType>, val: &V::NativeType) {
        self.insert_internal_impl(key, |_new, target| V::init_from(target, val))
    }

    /// Lookup key 'key' in the hashmap, and return an 'Entry' that represents that key's
    /// position in the hashmap.
    ///
    /// Methods on the returned type allow inserting or removing from said entry.
    ///
    /// This does not cause a read dependency.
    ///
    /// WARNING!
    /// Since this does not cause a read dependency, applications should base no decisions
    /// on the variant of the returned enum value.
    pub fn untracked_entry(
        self: Pin<&mut Self>,
        key: K::NativeOwnedType,
    ) -> NoatunHashMapEntry<'_, K, V>
    where
        K::NativeOwnedType: Sized,
    {
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        let self_cap = tself.capacity;
        let context = tself.data_meta_len();
        let mut probe_result = Self::probe(context, key.borrow());

        if !matches!(probe_result, ProbeRunResult::HashFull) {
            // Nominal, fast path.
            let (context, length) = tself.data_meta_len_mut2();
            return NoatunHashMapEntry::make_enum(probe_result, context, key, length);
        }

        tself.internal_change_capacity(self_cap + 15);

        let (context, length) = tself.data_meta_len_mut2();
        probe_result = Self::probe(context.readonly(), key.borrow());
        assert!(!matches!(probe_result, ProbeRunResult::HashFull));
        NoatunHashMapEntry::make_enum(probe_result, context, key, length)
    }

    fn insert_at_bucket<'a>(
        new: bool,
        probe_result: (BucketNr, Meta),
        key: &K::NativeType,
        context: HashAccessContextMut<'a, K, V>,
        val: impl FnOnce(bool, Pin<&mut V>),
        length: &mut usize,
    ) -> Pin<&'a mut V> {
        let (bucket, meta) = probe_result;
        // Safety: Probe found a bucket, it must thus be initialized
        let bucket_obj = unsafe { context.buckets[bucket.0].assume_init_mut() };

        // Safety: We don't move buckets
        // Se may copy them, but that's okay, it ends the lifetime.
        let mut old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };

        val(new, old_v.as_mut());

        if new {
            let bucket_meta = get_meta_mut(context.metas, bucket);
            NoatunContext.write_internal(meta, bucket_meta);
            // Safety: We don't move buckets
            // Se may copy them, but that's okay, it ends the lifetime.
            let old_k = unsafe { Pin::new_unchecked(&mut bucket_obj.key) };
            old_k.init_from(key);
            let new_length = *length + 1;
            NoatunContext.write_internal(new_length, length);
        }
        old_v
    }

    fn insert_internal_impl(
        &mut self,
        key: impl Borrow<K::NativeType>,
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
            }
        }
    }

    fn insert_impl(&mut self, key: K, val: V) {
        let context = self.data_meta_len_mut();
        let probe_result = Self::probe(context.readonly(), key.export_key_ref());
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
                // Safety: Bucket was populated, or empty. Empty buckets are still technically
                // initialized by mmap impl.
                let bucket_obj = unsafe { context.buckets[bucket.0].assume_init_mut()};
                // Safety: We don't move buckets
                let old_v = unsafe { Pin::new_unchecked(&mut bucket_obj.v) };
                NoatunContext.write(val, old_v);
                let bucket_meta = get_meta_mut(context.metas, bucket);
                NoatunContext.write_internal(meta, bucket_meta);
                if matches!(probe_result, ProbeRunResult::FoundUnoccupied(_, _)) {
                    // Safety: We don't move buckets
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
            clear_tracker: Tracker {
                owner: SequenceNr::INVALID,
            },
            phantom_data: PhantomData,
        };
        // Safety: We don't move buckets
        NoatunContext.write(new, unsafe { Pin::new_unchecked(self) });
    }

    // This does not write the handle itself into the noatun database!!
    fn internal_change_capacity(&mut self, new_min_capacity: usize) {
        // Safety: We have a mutable ref to `self`, so uniqueness is guaranteed
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
///    the same values (i.e, even if the underlying hasher hasn't changed, the implementation
///    of the `hash`-method may).
///
/// For these reasons, noatun requires users to implement `NoatunHash` for all types used as hash
/// keys. If you know that the underlying `Hash` implementation is actually stable, you can of
/// course just forward to such an impl.
///
/// This is not an unsafe trait. However, incorrect implementations may potentially lead to
/// infinite loops, or incorrect hashmap operations.
pub trait NoatunKey: NoatunStorable + Sized + Debug {
    /// A 'native' variant of Self.
    ///
    /// Native types are meant to be ergonomic to work with, but may have representations
    /// that do not allow them to be stored in the mmap:ed db. For example, the external
    /// type for `NoatunString` is simply `str`.
    ///
    /// For POD types without internal pointers, the external version should typically be just
    /// `&'a Self`.
    type NativeType: ?Sized;
    /// Owned version of [`Self::NativeType`].
    ///
    /// If `NativeType` is [`str`], this should be [`String`].
    type NativeOwnedType: Eq + Hash + Borrow<Self::NativeType>;

    /// Hash the key using the provided hasher 'hasher'.
    ///
    /// This method must be compatible with the [`Self::eq`] method.
    ///
    /// If the implementation of this or [`Self::eq`] changes, this must be
    /// reflected by a change in the [`Self::hash_key_schema`] method.
    fn hash<H>(tself: &Self::NativeType, hasher: &mut H)
    where
        H: Hasher;

    /// Clear out any registrars
    fn destroy(&mut self);

    /// Return a reference to a external key. This method should be fast, ideally
    /// just returning a reference to something in memory.
    fn export_key_ref(&self) -> &Self::NativeType;

    /// Export key, giving an owned instance.
    fn export_key(&self) -> Self::NativeOwnedType;

    /// Determine if 'a' and 'b' have identical key values.
    /// Typically, this method can just return `a == b`.
    ///
    /// However, this method is kept separate to ensure that equality is implemented
    /// consistently with respect to hashing.
    ///
    /// NOTE! This method must be compatible with the hashing done by [`Self::hash`].
    /// Specifically, the hash must not be computed on any field that is not part of
    /// the comparison.
    ///
    /// If the implementation of this or [`Self::eq`] changes, this must be
    /// reflected by a change in the [`Self::hash_key_schema`] method.
    fn eq(a: &Self::NativeType, b: &Self::NativeType) -> bool;

    /// Initialize a key value from the given 'external' type.
    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType);

    /// Hash the schema (and comparison/hashing semantics) of the current key
    /// and write to 'hasher'.
    ///
    /// This is used by Noatun to automatically determine at startup if an existing
    /// materialized view can be reused or not. If the hash of the complete schema
    /// is not identical, the whole materialized view is rebuilt from scratch.
    fn hash_key_schema(hasher: &mut SchemaHasher);
}

impl NoatunKey for MessageId {
    type NativeType = MessageId;
    type NativeOwnedType = MessageId;

    fn hash<H>(tself: &Self::NativeType, state: &mut H)
    where
        H: Hasher,
    {
        // Safety: MessageId is known to be 16 bytes
        let data: [u64; 2] = unsafe { std::mem::transmute(tself.data) };
        state.write_u64(data[0] ^ data[1])
    }

    fn destroy(&mut self) {}

    fn export_key_ref(&self) -> &Self::NativeType {
        self
    }

    fn export_key(&self) -> Self::NativeOwnedType {
        *self
    }

    fn eq(a: &Self::NativeType, b: &Self::NativeType) -> bool {
        a == b
    }

    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType) {
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        *tself = *external;
    }

    fn hash_key_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::MessageId/1");
    }
}

impl<K: NoatunKey + Hash + Eq, V: FixedSizeObject> Object for NoatunHashMap<K, V> {
    type Ptr = ThinPtr;
    type NativeType = IndexMap<K::NativeOwnedType, V::NativeOwnedType>;
    type NativeOwnedType = IndexMap<K::NativeOwnedType, V::NativeOwnedType>;

    fn export(&self) -> Self::NativeOwnedType {
        self.iter()
            .map(|(k, v)| (k.export_key(), v.export()))
            .collect()
    }

    fn destroy(self: Pin<&mut Self>) {
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        // Safety: `tself.clear_tracker` is a valid object
        unsafe { NoatunContext.clear_tracker_ptr(addr_of_mut!(tself.clear_tracker), true) };

        tself.clear_impl();
    }

    fn init_from(self: Pin<&mut Self>, external: &Self::NativeType) {
        // Safety: We don't move out of the ref
        let tself = unsafe { self.get_unchecked_mut() };
        for (k, v) in external {
            tself.insert_internal(k.borrow(), v.borrow());
        }
    }

    unsafe fn allocate_from<'a>(external: &Self::NativeType) -> Pin<&'a mut Self> {
        let mut ret: Pin<&mut Self> = NoatunContext.allocate();
        ret.as_mut().init_from(external);
        ret
    }
    fn hash_object_schema(hasher: &mut SchemaHasher) {
        Self::hash_schema(hasher);
    }
}

#[cfg(test)]
mod meta_tests {
    use super::meta_finder::get_any_empty;
    use super::HASH_META_GROUP_SIZE;
    use super::{meta_finder, BucketNr, Meta, MetaGroup, ProbeRunResult};

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
