//! The distributor contains all logic needed to distribute events between noatun
//! instances. It does not contain any actual IO code. Instead, something like
//! `crate::communication::DatabaseCommunication` can be used to communicate over UDP or similar.
use crate::colors::*;
use crate::cutoff::{Acceptability, CutOffHashPos};
use crate::database::{DatabaseSession, DatabaseSessionMut};
use crate::mini_pather::MiniPather;
use crate::noatun_instant::Instant;
use crate::{Database, Message, MessageExt, MessageFrame, MessageHeader, MessageId};
use anyhow::Result;
use arcshift::ArcShift;
use arrayvec::ArrayString;
use indexmap::map::Entry;
use indexmap::{IndexMap, IndexSet};
use metrics::{describe_gauge, gauge, Gauge, Unit};
use rand::random;
use savefile_derive::Savefile;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::io::Cursor;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub(crate) struct MessageSourceInfo {
    /// The node we first heard this message from. The firstactual transmitter of the complete
    /// message, if any such available, otherwise the first node that mentioned the message
    pub(crate) original_source: EphemeralNodeId,
    /// True if we have observed the actual message, not just a messageid mentioned in
    /// ReportHeads or similar
    pub(crate) transmitter_seen: bool,
    /// True if `origina_source` is the only source from which we have heard this particular
    /// message
    pub(crate) other_transmitters: bool,
}

#[derive(Default, Debug)]
pub(crate) struct RecentMessages {
    pub(crate) recent_messages: IndexMap<MessageId, MessageSourceInfo>,
}
impl RecentMessages {
    /// actual_transmission is to be set to true if message was transmitted in full, not just
    /// mentioned. This is used to help us understand if a node has a message because of a
    /// retransmission. I.e, if actual_transmission=false, this event itself can't be such a
    /// retransmission.
    fn record_message_source(
        &mut self,
        message: MessageId,
        source: EphemeralNodeId,
        actual_transmission: bool,
    ) {
        if self.recent_messages.len() > 10000 {
            self.recent_messages.drain(0..2500);
        }
        match self.recent_messages.entry(message) {
            Entry::Vacant(e) => {
                e.insert(MessageSourceInfo {
                    original_source: source,
                    transmitter_seen: actual_transmission,
                    other_transmitters: false,
                });
            }
            Entry::Occupied(mut e) => {
                let item = e.get_mut();
                if actual_transmission && !item.transmitter_seen {
                    item.transmitter_seen = true;
                    item.original_source = source;
                } else if actual_transmission && source != item.original_source {
                    item.other_transmitters = true;
                }
            }
        }
    }
}

// TODO(future): Consider the responsibilities of 'communciation.rs' and 'distributor.rs'
// And the latter should perhaps be split into submodules.

#[derive(Debug, Default)]
enum UpToSpeedStatus {
    /// We don't know how up-to-date we are with respect to the node
    #[default]
    Uninitialized,
    /// The node has some update heads that we lack.
    ///
    /// The tuple member gives the message ids of the missing messages.
    HeadsNeeded(Vec<MessageId>),
    /// We are up-to-date with respect to this node.
    ///
    /// That is, we know everything it knows.
    UpToSpeed,
}

impl UpToSpeedStatus {
    pub(crate) fn is_up_to_speed(&self) -> bool {
        match self {
            UpToSpeedStatus::Uninitialized | UpToSpeedStatus::HeadsNeeded(_) => false,
            UpToSpeedStatus::UpToSpeed => true,
        }
    }
}

/// Information about closest peers
#[derive(Debug)]
pub struct PeerInfo {
    /// How up-to-date we are with respect to this neighbor
    up_to_speed: UpToSpeedStatus,

    /// The last time an UpdateHeads message was received from this peer
    pub(crate) last_seen: Instant,
}
impl PeerInfo {
    /// Return an uninitialized PeerInfo
    pub fn new(now: Instant) -> PeerInfo {
        PeerInfo {
            up_to_speed: Default::default(),
            last_seen: now,
        }
    }
}

impl Neighborhood {
    /// GC the neighborhood state if timer elapsed
    pub fn gc_if_necessary(
        &mut self,
        our_node_id: EphemeralNodeId,
        periodic_message: Duration,
        now: Instant,
    ) {
        {
            let mut pather = self.fast_pather.write().unwrap();
            if pather.my_id() != our_node_id.raw_u16() {
                info!("identical ephemeral node_id detected, changing our id");
                *pather = MiniPather::new(our_node_id.raw_u16());
                self.peers.clear();
                self.metric_neighbor_count.set(0.0);
                drop(pather);
                return;
            }
        }

        if now.saturating_duration_since(self.last_gc) > periodic_message {
            self.last_gc = now;

            self.inhibited_request_upstream
                .retain(|_k, v| now.saturating_duration_since(v.0) < 32 * periodic_message);

            let mut removed = vec![];
            self.peers.retain(|k, v| {
                let retained = now.saturating_duration_since(v.last_seen) < 3 * periodic_message;
                if !retained {
                    removed.push(*k);
                }
                retained
            });
            self.metric_neighbor_count.set(self.peers.len() as f64);

            let mut pather = self.fast_pather.write().unwrap();
            for item in removed {
                pather.remove_neighbor(item.raw_u16());
            }
        }
    }

    /// Insert the given peer into the neighborhood
    pub fn get_insert_peer(&mut self, peer_id: EphemeralNodeId, now: Instant) -> &mut PeerInfo {
        let mut len = self.peers.len();
        let t = self.peers.entry(peer_id).or_insert_with(|| {
            len += 1;
            PeerInfo::new(now)
        });
        self.metric_neighbor_count.set(len as f64);
        t.last_seen = now;
        t
    }

    pub(crate) fn get_peer(&self, peer_id: EphemeralNodeId) -> Option<&PeerInfo> {
        self.peers.get(&peer_id)
    }
    /// Get the list of neighbors
    pub fn get_neighbors(&self) -> Vec<EphemeralNodeId> {
        let mut t: Vec<_> = self.peers.keys().copied().collect();
        t.sort_unstable();
        t
    }
}

/// Data structure to detect duplicate requests.
///
/// If multiple parts of the algorithm decide to do the same request,
/// we wish to merge these and not emit two identical requests on the net.
#[derive(Debug)]
pub struct DuplicationChecker<T> {
    memory: IndexMap<T, Instant>,
    gc_counter: usize,
    interval: Duration,
}
impl<T: Eq + Hash> DuplicationChecker<T> {
    /// Create a new DuplicationChecker instance, with the
    /// given interval.
    ///
    /// Messages are kept in the checker for approximately the given interval.
    pub fn new(interval: Duration) -> DuplicationChecker<T> {
        DuplicationChecker {
            memory: Default::default(),
            gc_counter: 0,
            interval,
        }
    }
    /// Returns true if duplicate.
    ///
    /// The item is added to the checker, allowing future checks for the same
    /// item to return true.
    pub fn is_duplicate(&mut self, id: T, now: Instant) -> bool {
        self.gc_counter += 1;
        if self.gc_counter > 10000 {
            self.gc_counter = 0;
            self.memory
                .retain(|_k, v| now.saturating_duration_since(*v) <= 2 * self.interval);
        }
        match self.memory.entry(id) {
            Entry::Occupied(mut e) => {
                let prev_elapsed = now.saturating_duration_since(*e.get());
                if prev_elapsed > self.interval {
                    *e.get_mut() = now;
                    false
                } else {
                    true
                }
            }
            Entry::Vacant(e) => {
                e.insert(now);
                false
            }
        }
    }
}

// TODO(future): Maybe nodes that notice that they have more neighbors than basically anybody,
// should try to change node-id to a small number, so they can be natural, efficient relays for everybody.

/// Information about the current node's neighborhood
#[derive(Debug)]
pub struct Neighborhood {
    /// The set of neighbors
    pub peers: IndexMap<EphemeralNodeId, PeerInfo>,
    /// Neighbor-tracker with logic to figure out when retransmits need to be sent
    pub fast_pather: Arc<RwLock<MiniPather>>,

    last_gc: Instant,
    /// The source we've observed for recent messages.
    pub(crate) recent_messages_source: RecentMessages,

    /// Messages that we were totally going to request from upstream, but didn't
    /// because we figured a peer would do it. request_inhibited_based_on_node_numbers has
    /// a counter that is increased when adding stuff here, and decreased whenever we
    /// receive a message in here without having requested it.
    pub(crate) inhibited_request_upstream: IndexMap<MessageId, (Instant, EphemeralNodeId)>,

    /// The number of other peers detected in the network
    metric_neighbor_count: Gauge,
}

impl Neighborhood {
    /// Return a new neighborhood struct with the given pather
    pub fn new(now: Instant, pather: Arc<RwLock<MiniPather>>) -> Neighborhood {
        let neighbor_count = gauge!("neighbor_count");
        describe_gauge!(
            "neighbor_count",
            Unit::Count,
            "Number of distinct neighbors detected"
        );

        Self {
            peers: Default::default(),
            fast_pather: pather,
            last_gc: now,
            recent_messages_source: Default::default(),
            inhibited_request_upstream: Default::default(),
            metric_neighbor_count: neighbor_count,
        }
    }

    /// request_from = the origin of the request we're about to respond to
    /// self_node = ourselves
    pub(crate) fn is_request_upstream_inhibited(
        &mut self,
        request_from: EphemeralNodeId,
        message_ids: &[MessageId],
        now: Instant,
        periodic_interval: Duration,
    ) -> bool {
        let mut fast_pather_guard = self.fast_pather.write().unwrap();
        let fast_pather = &mut *fast_pather_guard;

        if let Some(ordinal) = fast_pather.should_i_ask_for_retransmission(request_from.raw_u16()) {
            let mut inhibit = true;
            for msg in message_ids {
                match self.inhibited_request_upstream.entry(*msg) {
                    Entry::Occupied(o) => {
                        let periods = ((now.saturating_duration_since(o.get().0).as_millis()
                            as u64)
                            / (periodic_interval.as_millis().max(1) as u64))
                            as usize;
                        if periods > ordinal {
                            inhibit = false;
                            o.swap_remove();
                        }
                    }
                    Entry::Vacant(o) => {
                        if ordinal != 0 {
                            o.insert((now, request_from));
                        } else {
                            inhibit = false;
                        }
                    }
                }
            }
            inhibit
        } else {
            // They can't hear us.
            true
        }
    }

    /// Record that the given message has been received over the network
    pub fn record_message(&mut self, message: &DistributorMessage) {
        match message {
            DistributorMessage::ReportHeads { heads, source, .. } => {
                for head in heads {
                    self.recent_messages_source
                        .record_message_source(head.msg, *source, false);
                }
            }
            DistributorMessage::SyncAllQuery(_) => {}
            DistributorMessage::SyncAllRequest(_) => {}
            DistributorMessage::SyncAllAck(_) => {}
            DistributorMessage::RequestUpstream {
                source: _,
                query: _,
                ..
            } => {}
            DistributorMessage::UpstreamResponse {
                source, messages, ..
            } => {
                for msg in messages {
                    self.recent_messages_source
                        .record_message_source(msg.id, *source, false);
                }
            }
            DistributorMessage::SendMessageAndAllDescendants { .. } => {}
            DistributorMessage::Message {
                source,
                message: msg,
                ..
            } => {
                self.inhibited_request_upstream
                    .swap_remove(&msg.message_id());

                self.recent_messages_source
                    .record_message_source(msg.message_id(), *source, true);
            }
        }
    }
}

//TODO(future): Consider merging this with NeighborHood?
/// Output buffer with state
#[derive(Debug)]
pub struct QueryableOutbuffer {
    outbuf: VecDeque<DistributorMessage>,

    request_upstream_message_inhibit: DuplicationChecker<MessageId>,
    periodic_message_interval: Duration,
    recently_sent_upstream_responses_for: DuplicationChecker<MessageId>,

    recently_sent_message_ids: DuplicationChecker<MessageId>,
    gc_timer: Instant,
    parentless_messages: IndexMap</*missing parent*/ MessageId, Vec<AccumulatedMessage>>,
}

impl QueryableOutbuffer {
    pub(crate) fn gc_if_necessary(&mut self, now: Instant) {
        if now.saturating_duration_since(self.gc_timer) > 2 * self.periodic_message_interval {
            self.gc_timer = now;
            let mut ok = 0;
            let mut size_so_far = 0;
            for (_msg_id, msg_objs) in self.parentless_messages.iter().rev() {
                let mut bad = false;
                for msg_obj in msg_objs {
                    size_so_far += msg_obj.ram_size_estimate();
                    if size_so_far < 50_000_000 {
                    } else {
                        bad = true;
                        break;
                    }
                }
                if !bad {
                    ok += 1;
                }
            }
            if ok != self.parentless_messages.len() {
                debug_assert!(ok < self.parentless_messages.len());
                self.parentless_messages.drain(0..ok);
            }
        }
    }

    /// Return true if no message are queued for sending
    pub fn is_empty(&self) -> bool {
        self.outbuf.is_empty()
    }
    /// Pop the first message from the output queue, if any
    pub fn pop_front(&mut self) -> Option<DistributorMessage> {
        let msg = self.outbuf.pop_front();
        msg
    }
    /// Add a message to the send queue
    pub fn push_back(&mut self, msg: DistributorMessage) {
        self.outbuf.push_back(msg);
    }

    /// Add multiple messages to the send queue
    pub fn extend<I: IntoIterator<Item = DistributorMessage>>(&mut self, items: I) {
        self.outbuf.extend(items);
    }
}

impl QueryableOutbuffer {
    fn new(periodic_message_interval: Duration, now: Instant) -> Self {
        Self {
            outbuf: Default::default(),
            request_upstream_message_inhibit: DuplicationChecker::new(
                2 * periodic_message_interval,
            ),
            periodic_message_interval,
            recently_sent_upstream_responses_for: DuplicationChecker::new(
                2 * periodic_message_interval,
            ),
            recently_sent_message_ids: DuplicationChecker::new(2 * periodic_message_interval),
            gc_timer: now,
            parentless_messages: Default::default(),
        }
    }

    fn upstream_response_blocked(&mut self, id: MessageId, now: Instant) -> bool {
        self.recently_sent_upstream_responses_for
            .is_duplicate(id, now)
    }

    fn request_upstream(
        &mut self,
        messages_to_request: &[MessageId],
        self_node: EphemeralNodeId,
        request_from: EphemeralNodeId,
        neighbors: &mut Neighborhood,
        now: Instant,
        uninhibitable: bool,
    ) {
        if !uninhibitable
            && neighbors.is_request_upstream_inhibited(
                request_from,
                messages_to_request,
                now,
                self.periodic_message_interval,
            )
        {
            return;
        }

        let query: Vec<_> = messages_to_request
            .iter()
            .filter_map(|msg| {
                let is_dup = self
                    .request_upstream_message_inhibit
                    .is_duplicate(*msg, now);
                if is_dup {
                    None
                } else {
                    Some((*msg, 4))
                }
            })
            .collect();

        if !query.is_empty() {
            self.outbuf.push_back(DistributorMessage::RequestUpstream {
                query,
                source: self_node,
                destination: request_from,
            });
        }
    }
}

// Principle
// The node that is 'most ahead' (highest MessageId) has responsibility.
// If knows all the heads of other node, just sends perfect updates.
// Otherwise:
// Must request messages until it has complete picture

/// A serialized [`MessageFrame`].
#[derive(Debug, Savefile, Clone)]
pub struct SerializedMessage {
    id: MessageId,
    parents: Vec<MessageId>,
    data: Vec<u8>,
}

impl SerializedMessage {
    /// Create a serialized message from a header and payload
    pub fn from_header_and_body<M: Message>(
        header: MessageHeader,
        payload: M,
    ) -> Result<SerializedMessage> {
        Self::new(MessageFrame { header, payload })
    }

    /// Retain parents for which the closure returns true.
    ///
    /// ALl other parents are removed
    pub fn retain_parents(&mut self, mut predicate: impl FnMut(MessageId) -> bool) {
        self.parents.retain(move |id| predicate(*id));
    }
    /// Return the message id for this message
    pub fn message_id(&self) -> MessageId {
        self.id
    }
    /// Convert to [`MessageFrame<M>`]
    pub fn to_message<M: Message>(self) -> Result<MessageFrame<M>> {
        let reader = Cursor::new(&self.data);
        Ok(MessageFrame {
            header: MessageHeader {
                id: self.id,
                parents: self.parents,
            },
            payload: M::deserialize(&self.data[reader.position() as usize..])?,
        })
    }
    /// Like [`Self::to_message`], but does not take ownership
    pub fn to_message_from_ref<M: Message>(&self) -> Result<MessageFrame<M>> {
        let reader = Cursor::new(&self.data);
        Ok(MessageFrame {
            header: MessageHeader {
                id: self.id,
                parents: self.parents.clone(),
            },
            payload: M::deserialize(&self.data[reader.position() as usize..])?,
        })
    }
    /// Create a new instance based on the given message frame
    pub fn new<M: Message>(m: MessageFrame<M>) -> Result<SerializedMessage> {
        let mut data = vec![];
        m.payload.serialize(&mut data)?;
        Ok(SerializedMessage {
            id: m.header.id,
            parents: m.header.parents.clone(),
            data,
        })
    }
}

/// A message, a set of parents and a query count
#[derive(Debug, Savefile, Clone)]
pub struct MessageSubGraphNode {
    pub(crate) id: MessageId,
    parents: Vec<MessageId>,
    query_count: usize,
}

/// A set of parents, and a query count
pub struct MessageSubGraphNodeValue {
    parents: Vec<MessageId>,
    query_count: usize,
}

/// An address
///
/// Truncated to at most 40 bytes
#[derive(Savefile, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Address(ArrayString<40>);

impl PartialEq<str> for Address {
    fn eq(&self, other: &str) -> bool {
        &self.0 == other
    }
}
impl PartialEq<Address> for str {
    fn eq(&self, other: &Address) -> bool {
        self == &other.0
    }
}

impl Address {
    /// The maximum address length fully supported by noatun, in bytes
    pub const MAX_LENGTH: usize = 40;
    /// Convert from a value that implements Display, to Self
    pub fn from(value: impl Display) -> Self {
        use std::fmt::Write;
        let mut x = ArrayString::<40>::new();
        #[cfg(debug_assertions)]
        {
            let mut s = String::new();
            write!(&mut s, "{value}").unwrap();
            assert!(
                s.len() <= 40,
                "address {} (len={}) was too long",
                s,
                s.len()
            );
        }
        write!(&mut x, "{value}").unwrap();
        Address(x)
    }
}

impl Debug for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An ephemeral node id.
///
/// Each node has an ephemeral node id, at any time.
///
/// This id is not necessarily globally unique. However, when nodes detect a neighbor
/// with the same id, the id is re-randomized.
///
/// This is used by the distributor, but not by the database itself.
///
/// Under the hood, this value is a 16-bit integer, which gives enough entropy
/// for approximately a few tens of neighbors. However, we don't require that no
/// collisions occur - collisions mean we change the id dynamically. So we can
/// probably allow 1000 neighbors, which is enough for what Noatun tries to do a
/// and be.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Savefile, PartialOrd, Ord)]
pub struct EphemeralNodeId(u16);

impl Display for EphemeralNodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", colored_int(self.0.into()))
    }
}
impl Debug for EphemeralNodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", colored_int(self.0.into()))
    }
}

#[cfg(test)]
pub(crate) static NON_RANDOM_EPHEMERAL_NODE_ID_COUNTER: std::sync::atomic::AtomicU16 =
    std::sync::atomic::AtomicU16::new(0);

impl EphemeralNodeId {
    /// Return the inner 16 bit value
    pub fn raw_u16(self) -> u16 {
        self.0
    }
    /// Create an instance wrapping the given 16 bit value
    pub fn new(value: u16) -> Self {
        EphemeralNodeId(value)
    }
    /// Create a random EphemeralNodeId
    pub fn random() -> EphemeralNodeId {
        #[cfg(test)]
        {
            if crate::FOR_TEST_NON_RANDOM_ID {
                let id = NON_RANDOM_EPHEMERAL_NODE_ID_COUNTER
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return EphemeralNodeId(id);
            }
        }
        EphemeralNodeId(random())
    }
}

//TODO(future): Add a quick-join mechanism. A newly started node can force all nodes to send an
//extra ReportHeads, after which the newly joined nodes sends one of its own. Thus we
//get rid of the 1-periodic msg delay of waiting for ReportHeads-messages

/// An updatehead, and the node we believe wrote this update head (if known)
#[derive(Debug, Savefile, Clone)]
pub struct Head {
    /// The message id
    pub msg: MessageId,
    /// The origin of said message, if known
    pub origin: Option<EphemeralNodeId>,
}

/// A message sent by the distributor
///
/// This describes every message of the distributor protocol
#[derive(Debug, Savefile, Clone)]
pub enum DistributorMessage {
    /// Report all update heads for the sender
    ReportHeads {
        /// The sender of the message
        source: EphemeralNodeId,
        /// The cutoff hash for the sender
        cutoff: CutOffHashPos,
        /// The update heads of the sender
        heads: Vec<Head>,
        /// Neighbors known by sender
        ///
        neighbors: Vec<EphemeralNodeId>,
    },
    /// A query to tell if the listed messages are known.
    ///
    /// If they are not, they should be requested by SyncAllRequest.
    /// I.e, if the node receiving a SyncAllRequest doesn't have any of the messages
    /// listed, they should be requested using SyncAllRequest.
    SyncAllQuery(Vec<MessageId>),
    /// The given messages should be sent.
    SyncAllRequest(Vec<MessageId>),
    /// Sent only when doing a full sync
    SyncAllAck(Vec<MessageId>),
    /// Report a cut in the source node message graph
    RequestUpstream {
        /// Sender of this distributor message
        source: EphemeralNodeId,
        /// the usize is How many levels to ascend from message
        query: Vec<(MessageId, usize)>,
        /// Destination of request
        destination: EphemeralNodeId,
    },
    /// Response to a RequestUpstream, giving information about the message graph
    UpstreamResponse {
        /// Sender of this distributor message
        source: EphemeralNodeId,
        /// Destination of response
        dest: EphemeralNodeId,
        /// Messages
        messages: Vec<MessageSubGraphNode>,
    },
    /// Command the recipient to send all descendants of the given messages
    SendMessageAndAllDescendants {
        /// Sender of this distributor message
        source: EphemeralNodeId,
        /// Node that is asked to send all descendants
        destination: EphemeralNodeId,
        /// Messages whose descendants should be sent
        message_id: Vec<MessageId>,
    },
    /// Actual messages
    Message {
        /// Sender of this distributor message
        source: EphemeralNodeId,
        /// Message
        message: SerializedMessage,
        /// True if we demand recipient to ack this message
        demand_ack: bool,

        /// Original sender of this message, as far as we know.
        /// If this message was created on the node that sent it, this means it cannot be squelched.
        origin: EphemeralNodeId,
        /// True if this message was transmitted because it was explicitly requested
        explicit_retransmit: bool,
    },
}

impl DistributorMessage {
    /// Source of this message
    pub fn source(&self) -> Option<EphemeralNodeId> {
        match self {
            DistributorMessage::ReportHeads { source, .. } => Some(*source),
            DistributorMessage::SyncAllQuery(_) => None,
            DistributorMessage::SyncAllRequest(_) => None,
            DistributorMessage::SyncAllAck(_) => None,
            DistributorMessage::RequestUpstream { source, .. } => Some(*source),
            DistributorMessage::UpstreamResponse { source, .. } => Some(*source),
            DistributorMessage::SendMessageAndAllDescendants { source, .. } => Some(*source),
            DistributorMessage::Message { source, .. } => Some(*source),
        }
    }
    /// Debug representation of this message
    pub fn debug_format<M: Message>(&self) -> Result<String> {
        Ok(match self {
            DistributorMessage::ReportHeads {
                cutoff,
                heads,
                source,
                neighbors,
            } => {
                format!(
                    "{}: cutoff: {cutoff}, heads: {:?}, src: {:?}, neigh: {:?}",
                    lightgray("ReportHeads"),
                    heads,
                    source,
                    neighbors
                )
            }
            DistributorMessage::SyncAllQuery(messages) => {
                format!("{}: Messages: {:?}", lightgreen("SyncAllQuery"), messages)
            }
            DistributorMessage::SyncAllRequest(messages) => {
                format!("{}: Messages: {:?}", orange("SyncAllRequest"), messages)
            }
            DistributorMessage::SyncAllAck(messages) => {
                format!("{}: Messages: {:?}", lightbluegreen("SyncAllAck"), messages)
            }
            DistributorMessage::RequestUpstream {
                source,
                query,
                destination,
            } => {
                format!(
                    "{}: Query: {:?}, src: {:?}, dst: {:?}",
                    red("RequestUpstream"),
                    query,
                    source,
                    destination
                )
            }
            DistributorMessage::UpstreamResponse {
                source,
                dest,
                messages,
            } => {
                format!(
                    "{}: Messages: {:?}, src: {:?}, dst: {:?}",
                    lightbrown("UpstreamResponse"),
                    messages,
                    source,
                    dest
                )
            }
            DistributorMessage::SendMessageAndAllDescendants {
                source,
                message_id,
                destination,
            } => {
                format!(
                    "{}: messages: {:?}, src: {:?}, dst: {:?}",
                    pink("SendMessageAndAllDescendants"),
                    message_id,
                    source,
                    destination
                )
            }
            DistributorMessage::Message {
                source,
                message,
                demand_ack,
                origin,
                explicit_retransmit,
            } => {
                let msg: MessageFrame<M> = message.to_message_from_ref()?;
                format!(
                    "{}: id = {}, parents = {:?}, need_ack = {}, msg = {:?}, src = {:?}, origin = {:?}, retransmit = {}",
                    turqouise("Message"),
                    msg.header.id,
                    msg.header.parents,
                    demand_ack,
                    msg.payload,
                    source,
                    origin,
                    explicit_retransmit
                )
            }
        })
    }
}

#[derive(Debug)]
enum SyncAllState {
    NotActive,
    Starting,
    /// Query messages starting at 'MessageId', inclusive
    BeginQuery(MessageId),
    /// The current query concerns messages a..=b (i.e, note, inclusive of 'b')
    QueryActive(
        /*a*/ MessageId,
        /*b*/ MessageId,
        IndexSet<MessageId>,
    ),
}

impl SyncAllState {
    pub fn idle(&self) -> bool {
        match self {
            SyncAllState::NotActive => true,
            SyncAllState::Starting
            | SyncAllState::BeginQuery(_)
            | SyncAllState::QueryActive(_, _, _) => false,
        }
    }
}

/// Status of distributor, including information about peers
#[derive(Debug)]
pub struct DistributorStatus {
    nominal: bool,
    most_recent_clockdrift: HashMap<EphemeralNodeId, Instant>,
    most_recent_unsynced: HashMap<EphemeralNodeId, Instant>,
    have_heard_peer: bool,
    last_gc: Instant,
}

impl DistributorStatus {
    fn gc_if_necessary(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_gc).as_millis() > 60000 {
            self.last_gc = now;
            self.most_recent_unsynced
                .retain(|_k, v| now.saturating_duration_since(*v).as_millis() < 60000);
            self.most_recent_clockdrift
                .retain(|_k, v| now.saturating_duration_since(*v).as_millis() < 60000);
        }
    }
}

/// The distributor ensures the messages of a Database are synchronized
/// to other peers in the network.
#[derive(Debug)]
pub struct Distributor {
    #[doc(hidden)]
    pub ephemeral_node_id: ArcShift<EphemeralNodeId>,
    // A sync-all request is in progress.
    // It sends all Messages in MessageId-order (which guarantees that all
    // parents will be sent before any children.
    sync_all_inprogress: SyncAllState,

    distributor_state: DistributorStatus,

    periodic_message_interval: Duration,
    // TODO(future): Clean this up. The repsonsibilities of `QueryableOutbuffer` vs `Neighborhood`
    // are very unclear
    // TODO(future): This should not be public.
    #[doc(hidden)]
    pub outbuf: QueryableOutbuffer,
    #[doc(hidden)]
    pub neighborhood: Neighborhood,
    auto_resync: bool,
}

/// Synchronization status
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Status {
    /// Synchronization has been achieved
    Nominal,
    /// Synchronization could not be achieved, because clocks are too far off
    BadClocksDetected,
    /// Nodes are not synchronized
    OutOfSync,
    /// There are no peers to synchronize with
    NoPeers,
    /// Synchronization is happening
    Synchronizing,
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Nominal => {
                write!(f, "Nominal")
            }
            Status::BadClocksDetected => {
                write!(f, "BadClocksDetected")
            }
            Status::OutOfSync => {
                write!(f, "OutOfSync")
            }
            Status::NoPeers => {
                write!(f, "NoPeers")
            }
            Status::Synchronizing => {
                write!(f, "Synchronizing")
            }
        }
    }
}

/// Truncate the given name to a limited length 'Address'.
pub fn truncate_to_address(name: &str) -> Address {
    if name.len() <= Address::MAX_LENGTH {
        return Address::from(name);
    }
    for i in (1..=Address::MAX_LENGTH).rev() {
        if name.is_char_boundary(i) {
            return Address::from(name.split_at(i).0);
        }
    }
    Address::from("?")
}

#[derive(Debug)]
pub(crate) struct AccumulatedMessage {
    msg: SerializedMessage,
    /// The sender of this message. Might not be original origin, since
    /// message relaying occurs
    source: EphemeralNodeId,
    /// Equal to source for messages created _at_ source.
    origin: EphemeralNodeId,
    need_ack: bool,
    /// Is explicit re-transmission (i.e, not an automatic forwarding or organic reception)
    explicit_retransmit: bool,
}

impl AccumulatedMessage {
    fn ram_size_estimate(&self) -> usize {
        self.msg.data.len() + std::mem::size_of::<Self>()
    }
}

impl Distributor {
    /// The interval between periodic messages
    pub fn periodic_message_interval(&self) -> Duration {
        self.periodic_message_interval
    }
    const BATCH_SIZE: usize = 20;

    /// Create a new distributor
    pub fn new(
        periodic_message_interval: Duration,
        mut initial_node_id: ArcShift<EphemeralNodeId>,
        now: Instant,
        mini_pather: Option<Arc<RwLock<MiniPather>>>,
        auto_resync: bool,
    ) -> Distributor {
        let node = initial_node_id.get().raw_u16();
        Self {
            sync_all_inprogress: SyncAllState::NotActive,
            distributor_state: DistributorStatus {
                nominal: false,
                most_recent_clockdrift: Default::default(),
                most_recent_unsynced: Default::default(),
                have_heard_peer: false,
                last_gc: now,
            },
            periodic_message_interval,
            neighborhood: Neighborhood::new(
                now,
                mini_pather.unwrap_or(Arc::new(RwLock::new(MiniPather::new(node)))),
            ),
            ephemeral_node_id: initial_node_id,
            outbuf: QueryableOutbuffer::new(periodic_message_interval, now),
            auto_resync,
        }
    }

    /// The current ephemeral node id for this distributor
    pub fn node_id(&mut self) -> EphemeralNodeId {
        *self.ephemeral_node_id.get()
    }

    /// Returns the current status of the system with respect to clock drift.
    ///
    /// If clock drift has been observed with the last 60 seconds, it will be reported
    /// by this method.
    pub fn get_status(&self, now: Instant) -> Status {
        for drift in self.distributor_state.most_recent_clockdrift.values() {
            if now.saturating_duration_since(*drift).as_millis() < 60000 {
                return Status::BadClocksDetected;
            }
        }

        for unsync in self.distributor_state.most_recent_unsynced.values() {
            let unsync_t = now.saturating_duration_since(*unsync).as_millis();
            if unsync_t < 60000 {
                return Status::OutOfSync;
            }
        }
        if !self.distributor_state.have_heard_peer {
            return Status::NoPeers;
        }
        if self.distributor_state.nominal {
            return Status::Nominal;
        }
        Status::Synchronizing
    }

    /// Call this to retrieve a message that should be sent periodically
    pub fn get_periodic_message<MSG: Message + 'static>(
        &mut self,
        database: &DatabaseSession<MSG>,
        now: Instant,
    ) -> Result<Vec<DistributorMessage>> {
        self.neighborhood.gc_if_necessary(
            *self.ephemeral_node_id.get(),
            self.periodic_message_interval,
            now,
        );

        self.outbuf.gc_if_necessary(now);

        self.distributor_state.gc_if_necessary(now);

        let mut heads = database
            .get_update_heads()
            .iter()
            .map(|msg_id| {
                let origin = self
                    .neighborhood
                    .recent_messages_source
                    .recent_messages
                    .get(msg_id)
                    .map(|x| x.original_source);
                Head {
                    msg: *msg_id,
                    origin,
                }
            })
            .collect::<Vec<_>>();
        heads.sort_by_key(|head| head.msg);
        let mut temp = vec![DistributorMessage::ReportHeads {
            source: *self.ephemeral_node_id.get(),
            cutoff: database.current_cutoff_state()?,
            heads,
            neighbors: self.neighborhood.get_neighbors(),
        }];
        let sync_from = match &self.sync_all_inprogress {
            SyncAllState::NotActive => None,
            SyncAllState::Starting => Some(MessageId::ZERO),
            SyncAllState::BeginQuery(start) => Some(*start),
            SyncAllState::QueryActive(from, _to, _request_identity) => Some(*from),
        };
        if let Some(sync_from) = sync_from {
            let cur_batch = database.get_messages_at_or_after(sync_from, Self::BATCH_SIZE)?;
            info!(
                "All messages after {:?} turned out to be {:?}",
                sync_from, cur_batch
            );
            if cur_batch.is_empty() {
                self.sync_all_inprogress = SyncAllState::NotActive;
            } else {
                self.sync_all_inprogress = SyncAllState::QueryActive(
                    *cur_batch.first().unwrap(),
                    *cur_batch.last().unwrap(),
                    cur_batch.iter().copied().collect(),
                );
                temp.push(DistributorMessage::SyncAllQuery(cur_batch));
            }
        }

        Ok(temp)
    }

    // Legacy, for some old tests
    #[cfg(test)]
    pub(crate) fn receive_message2<MSG: Message>(
        &mut self,
        database: &mut Database<MSG>,
        input: impl Iterator<Item = DistributorMessage>,
        now: Instant,
    ) -> Result<Vec<DistributorMessage>> {
        self.receive_message(
            database,
            input.map(|x| (None, x.source().unwrap_or(EphemeralNodeId(0)), x)),
            now,
        )?;
        let ret = self.outbuf.outbuf.drain(..).collect();
        self.outbuf = QueryableOutbuffer::new(self.periodic_message_interval, now);
        Ok(ret)
    }

    /// Call this when a node id collision has been detected.
    ///
    /// This can often be detected quicker by lower layers, this is available
    /// as a public method. A new unique node id will be generated.
    pub fn report_node_id_collision(&mut self) {
        self.ephemeral_node_id.update(EphemeralNodeId::random());
    }

    /// Note, loopback messages should be detected by caller
    /// This method will interpret incoming node-ids identical to our own as a node id
    /// collision, not as a message loopback
    pub fn receive_message<MSG: Message>(
        &mut self,
        database: &mut Database<MSG>,
        input: impl Iterator<Item = (Option<Address>, EphemeralNodeId, DistributorMessage)>,
        now: Instant,
    ) -> Result<()> {
        let mut database = database.begin_session_mut()?;
        database.maybe_advance_cutoff()?;
        let mut accumulated_heads: IndexMap<
            MessageId,
            Vec<(
                /*source:*/ EphemeralNodeId,
                /*origin:*/ Option<EphemeralNodeId>,
            )>,
        > = IndexMap::new();
        let mut accumulated_request_upstream = IndexMap::new();
        let mut accumulated_upstream_responses: IndexMap<
            MessageId,
            /*parents*/
            (
                EphemeralNodeId, /*src*/
                EphemeralNodeId, /*dst*/
                MessageSubGraphNodeValue,
            ),
        > = IndexMap::new();
        let mut accumulated_send_msg_and_descendants = IndexMap::new();
        let mut accumulated_serialized = vec![];
        let mut accumulated_sync_all_queries = IndexSet::new();
        let mut accumulated_sync_all_requests = IndexSet::new();

        let our_node_id = *self.ephemeral_node_id.get();
        let mut collision = false;
        let mut check_node_id_collision = |observed: EphemeralNodeId| {
            if observed == our_node_id {
                collision = true;
            }
        };

        for (_src_addr, src, item) in input {
            self.neighborhood.record_message(&item);

            self.distributor_state.have_heard_peer = true;
            match item {
                DistributorMessage::SyncAllQuery(query) => {
                    accumulated_sync_all_queries.extend(query);
                }
                DistributorMessage::SyncAllRequest(requests) => {
                    accumulated_sync_all_requests.extend(requests);
                }

                DistributorMessage::ReportHeads {
                    source,
                    cutoff: cutoff_hash,
                    heads,
                    neighbors,
                } => {
                    check_node_id_collision(source);
                    for pass in 0..2 {
                        match database.is_acceptable_cutoff_hash(cutoff_hash)? {
                            Acceptability::Previous | Acceptability::Nominal => {
                                // If the neighbor has no neighbors of its own, it's just starting up.
                                // Let's wait a bit before acting on its messages

                                self.neighborhood
                                    .fast_pather
                                    .write()
                                    .unwrap()
                                    .report_neighbors(
                                        source.raw_u16(),
                                        neighbors.iter().map(|x| x.raw_u16()),
                                    );

                                let peer = self.neighborhood.get_insert_peer(source, now);

                                match &mut peer.up_to_speed {
                                    UpToSpeedStatus::Uninitialized => {
                                        peer.up_to_speed = UpToSpeedStatus::HeadsNeeded(
                                            heads.iter().map(|x| x.msg).collect(),
                                        );
                                    }
                                    UpToSpeedStatus::HeadsNeeded(needed) => {
                                        needed.retain(|x| {
                                            !database.contains_message(*x).unwrap_or(false)
                                        });
                                        if needed.is_empty() {
                                            peer.up_to_speed = UpToSpeedStatus::UpToSpeed;
                                        }
                                    }
                                    UpToSpeedStatus::UpToSpeed => {}
                                }

                                if self.sync_all_inprogress.idle() && neighbors.is_empty() == false
                                {
                                    for head in heads {
                                        let sources =
                                            accumulated_heads.entry(head.msg).or_default();
                                        if !sources.contains(&(source, head.origin)) {
                                            sources.push((source, head.origin));
                                        }
                                    }
                                }

                                debug!("Acceptability: Nominal");

                                self.distributor_state.most_recent_unsynced.remove(&src);
                                self.distributor_state.most_recent_clockdrift.remove(&src);

                                self.neighborhood
                                    .fast_pather
                                    .write()
                                    .unwrap()
                                    .report_own_neighbors(
                                        self.neighborhood.peers.keys().map(|x| x.raw_u16()),
                                    );

                                debug_assert!(neighbors.is_sorted());
                                break;
                            }
                            Acceptability::Unacceptable => {
                                debug!("Acceptability: Unacceptable");
                                {
                                    self.distributor_state
                                        .most_recent_unsynced
                                        .insert(src, Instant::now());
                                }

                                if self.sync_all_inprogress.idle() && self.auto_resync {
                                    self.sync_all_inprogress = SyncAllState::Starting;
                                }
                                break;
                            }
                            Acceptability::Undecided(advance) => {
                                debug!("Acceptability: Undecided");
                                // We know this won't advance too far, because is_acceptable_cutoff_hash
                                // never advances far
                                database.advance_cutoff(advance)?;
                                if pass == 1 {
                                    // If we get here, in pass 1, it means we apparently didn't
                                    // advance to the correct place in the first pass. This is
                                    // not expected.
                                    error!(
                                        "unexpected case, cutoff hash considered undecided twice"
                                    )
                                }
                            }
                            Acceptability::UnacceptablePeerClockDrift => {
                                /*if self.sync_all_inprogress.idle() {
                                    accumulated_heads.extend(heads);
                                }*/
                                info!("Acceptability: Clockdrift");
                                self.distributor_state
                                    .most_recent_clockdrift
                                    .insert(src, Instant::now());
                                break;
                            }
                        }
                    }
                }
                DistributorMessage::RequestUpstream {
                    source,
                    query,
                    destination,
                } => {
                    check_node_id_collision(source);

                    for (msg, count) in query {
                        // TODO(future): Consider if this is fast enough to do unbatched here? (and is batching really faster?)
                        if !database.contains_message(msg)? {
                            continue;
                        }
                        trace!("Considering {:?} query: {:?}", source, msg);
                        if destination == *self.ephemeral_node_id.get() {
                            let accum_count = accumulated_request_upstream
                                .entry(msg)
                                .or_insert((0usize, source));
                            accum_count.0 = (accum_count.0).max(count);
                            accum_count.1 = (accum_count.1).min(source);
                        }
                    }
                }
                DistributorMessage::UpstreamResponse {
                    source,
                    dest,
                    messages,
                } => {
                    check_node_id_collision(source);

                    if dest == *self.ephemeral_node_id.get() {
                        for msg in messages {
                            let val = MessageSubGraphNodeValue {
                                parents: msg.parents,
                                query_count: msg.query_count,
                            };
                            match accumulated_upstream_responses.entry(msg.id) {
                                Entry::Occupied(mut e) => {
                                    if e.get_mut().0 < source {
                                        e.insert((source, dest, val));
                                    }
                                }
                                Entry::Vacant(e) => {
                                    e.insert((source, dest, val));
                                }
                            }
                        }
                    }
                }
                DistributorMessage::SendMessageAndAllDescendants {
                    source,
                    message_id,
                    destination,
                } => {
                    check_node_id_collision(source);

                    if destination == *self.ephemeral_node_id.get() {
                        for msg in message_id {
                            match accumulated_send_msg_and_descendants.entry(msg) {
                                Entry::Occupied(mut e) => {
                                    if *e.get() > source {
                                        e.insert(source);
                                    }
                                }
                                Entry::Vacant(e) => {
                                    e.insert(source);
                                }
                            }
                        }
                    }
                }
                DistributorMessage::Message {
                    source,
                    message: mut msg,
                    demand_ack: need_ack,
                    origin,
                    explicit_retransmit,
                } => {
                    check_node_id_collision(source);

                    database.remove_cutoff_parents(&mut msg.parents);
                    accumulated_serialized.push(AccumulatedMessage {
                        msg,
                        need_ack,
                        source,
                        origin,
                        explicit_retransmit,
                    });
                }
                DistributorMessage::SyncAllAck(acked) => match &mut self.sync_all_inprogress {
                    SyncAllState::QueryActive(from, to, items) => {
                        debug!("Processing active query: {:?}..{:?}", from, to);
                        for ack in &acked {
                            items.swap_remove(ack);
                        }
                        debug!("Active items: {:?}, after acks: {:?}", items, acked);
                        if items.is_empty() {
                            info!("Advance state of active query to {:?}", *to);
                            self.sync_all_inprogress = SyncAllState::BeginQuery(to.successor());
                        }
                    }
                    SyncAllState::NotActive => {}
                    SyncAllState::Starting => {}
                    SyncAllState::BeginQuery(_) => {}
                },
            }
        }

        self.process_reported_heads(&mut database, accumulated_heads, now)?;

        accumulated_request_upstream.sort_keys();

        self.process_request_upstream(&mut database, accumulated_request_upstream, now)?;
        self.process_upstream_response(&mut database, accumulated_upstream_responses, now)?;
        self.process_send_message_all_descendants(
            &mut database,
            accumulated_send_msg_and_descendants,
            now,
        )?;

        for i in 0..1000 {
            if i == 1000 - 1 {
                error!("Unexpected iteration count {}", i);
            }
            // Temp is messages that previously couldn't be imported because they
            // were missing their parents. One or more parents have now become known.
            let temp = self.process_received_messages(
                &mut database,
                std::mem::take(&mut accumulated_serialized),
                now,
            )?;
            if temp.is_empty() {
                break;
            }
            accumulated_serialized = temp;
        }
        self.process_sync_all_queries(&mut database, accumulated_sync_all_queries)?;
        self.process_sync_all_requests(&mut database, accumulated_sync_all_requests, now)?;

        if collision {
            self.report_node_id_collision();
        }

        database.commit()?;
        Ok(())
    }

    fn process_sync_all_queries<MSG: Message>(
        &mut self,
        database: &mut DatabaseSessionMut<MSG>,
        accumulated_sync_all_queries: IndexSet<MessageId>,
    ) -> Result<()> {
        let mut request = vec![];
        let mut acks = vec![];
        for query in accumulated_sync_all_queries {
            if !database.contains_message(query)? {
                request.push(query);
            } else {
                acks.push(query);
            }
        }
        if !request.is_empty() {
            self.outbuf
                .push_back(DistributorMessage::SyncAllRequest(request));
        }
        if !acks.is_empty() {
            self.outbuf.push_back(DistributorMessage::SyncAllAck(acks));
        }
        Ok(())
    }

    fn process_sync_all_requests<MSG: Message>(
        &mut self,
        database: &mut DatabaseSessionMut<MSG>,
        accumulated_sync_all_requests: IndexSet<MessageId>,
        now: Instant,
    ) -> Result<()> {
        for request in accumulated_sync_all_requests {
            match database.load_message(request) {
                Ok(msg) => {
                    if self
                        .outbuf
                        .recently_sent_message_ids
                        .is_duplicate(msg.id(), now)
                        == false
                    {
                        self.outbuf.push_back(DistributorMessage::Message {
                            source: *self.ephemeral_node_id.get(),
                            origin: self
                                .neighborhood
                                .recent_messages_source
                                .recent_messages
                                .get(&msg.id())
                                .map(|x| x.original_source)
                                .unwrap_or(*self.ephemeral_node_id.get()),
                            message: SerializedMessage::new(msg)?,
                            demand_ack: true,
                            explicit_retransmit: true,
                        });
                    }
                }
                Err(err) => {
                    warn!(
                        "Received request for message {:?}, that we couldn't load: {:?}",
                        request, err
                    );
                }
            }
        }
        Ok(())
    }

    fn process_reported_heads<MSG: Message>(
        &mut self,
        database: &mut DatabaseSessionMut<MSG>,
        accumulated_heads: IndexMap<
            MessageId,
            Vec<(
                /*source:*/ EphemeralNodeId,
                /*origin:*/ Option<EphemeralNodeId>,
            )>,
        >,
        now: Instant,
    ) -> Result<()> {
        self.distributor_state.nominal = true;

        let mut messages_to_request_from_source = IndexMap::<_, Vec<_>>::new();
        for (message, srcs) in accumulated_heads {
            assert!(!srcs.is_empty());
            if database.contains_message(message)? {
                continue;
            }

            let (min_src, _origin) = if srcs.len() == 1 {
                srcs[0]
            } else {
                srcs.iter()
                    .min_by_key(|(src, _)| {
                        let score = self
                            .neighborhood
                            .get_peer(*src)
                            .map(|x| {
                                if x.up_to_speed.is_up_to_speed() {
                                    0u8
                                } else {
                                    1
                                }
                            })
                            .unwrap_or(2);
                        (score, src)
                    })
                    .copied()
                    .unwrap()
            };

            messages_to_request_from_source
                .entry(min_src)
                .or_default()
                .push(message);
        }
        for (src, msgs) in messages_to_request_from_source.into_iter() {
            self.outbuf.request_upstream(
                &msgs,
                *self.ephemeral_node_id.get(),
                src,
                &mut self.neighborhood,
                now,
                false,
            );
            self.distributor_state.nominal = false;
        }
        Ok(())
    }
    fn process_request_upstream<MSG: Message>(
        &mut self,
        database: &mut DatabaseSessionMut<MSG>,
        accumulated_heads: IndexMap<MessageId, (usize, /*src:*/ EphemeralNodeId)>,
        now: Instant,
    ) -> Result<()> {
        let mut by_src: IndexMap<EphemeralNodeId, Vec<(MessageId, usize)>> = IndexMap::new();

        for (msg, (count, src)) in accumulated_heads {
            by_src.entry(src).or_default().push((msg, count));
        }
        for (src, heads) in by_src {
            let messages: Vec<MessageSubGraphNode> = database
                .get_upstream_of(heads.into_iter())?
                .filter(|x| self.outbuf.upstream_response_blocked(x.0.id, now) == false)
                .map(|(msg, query_count)| MessageSubGraphNode {
                    id: msg.id,
                    parents: msg.parents,
                    query_count,
                })
                .collect();

            if !messages.is_empty() {
                self.outbuf.push_back(DistributorMessage::UpstreamResponse {
                    messages,
                    dest: src,
                    source: *self.ephemeral_node_id.get(),
                });
            }
        }

        Ok(())
    }
    fn process_upstream_response<MSG: Message>(
        &mut self,
        database: &mut DatabaseSessionMut<MSG>,
        upstream_response: IndexMap<
            MessageId,
            /*parents*/
            (
                EphemeralNodeId, /*src*/
                EphemeralNodeId, /*dst*/
                MessageSubGraphNodeValue,
            ),
        >,
        now: Instant,
    ) -> Result<()> {
        let mut unknowns: IndexMap<EphemeralNodeId, Vec<(MessageId, usize)>> = IndexMap::new();
        let mut send_cmds = IndexMap::new();
        for (msg_id, (msg_source, msg_dest, msg_value)) in upstream_response.iter() {
            if database.contains_message(*msg_id)? {
                continue; //We already have this one
            }
            if *msg_dest != *self.ephemeral_node_id.get() {
                continue;
            }

            let mut err = Ok(());

            let missing_parents = msg_value
                .parents
                .iter()
                .copied()
                .filter(|x| {
                    let have_parent = database
                        .contains_message(*x)
                        .map_err(|e| {
                            error!("Error: {e:?}");
                            err = Err(e);
                        })
                        .is_ok_and(|x| x);
                    !have_parent
                })
                .collect::<Vec<_>>();
            debug_assert!(err.is_ok());
            err?;

            if missing_parents.is_empty() {
                // We have all the parents, a perfect msg to request!
                info!(
                    "Requesting msg.id={:?}, because we have all its parents: {:?}",
                    msg_id, msg_value.parents
                );

                send_cmds.insert(*msg_id, msg_source);
                continue;
            }

            unknowns.entry(*msg_source).or_default().extend(
                missing_parents
                    .iter()
                    .map(|x| (*x, (2 * msg_value.query_count).min(256))),
            );
        }
        if send_cmds.is_empty() == false {
            let mut msg_by_source = IndexMap::<_, Vec<_>>::new();
            for (k, v) in send_cmds {
                msg_by_source.entry(*v).or_default().push(k);
            }
            for (src, messages) in msg_by_source {
                self.outbuf
                    .push_back(DistributorMessage::SendMessageAndAllDescendants {
                        message_id: messages,
                        source: *self.ephemeral_node_id.get(),
                        destination: src,
                    });
            }
        }
        if unknowns.is_empty() == false {
            for (src_node, unknowns) in unknowns {
                let unknowns = unknowns.into_iter().map(|x| x.0).collect::<Vec<_>>();
                self.outbuf.request_upstream(
                    &unknowns,
                    *self.ephemeral_node_id.get(),
                    src_node,
                    &mut self.neighborhood,
                    now,
                    true,
                );
            }
        }

        Ok(())
    }
    fn process_send_message_all_descendants<MSG: Message>(
        &mut self,
        database: &mut DatabaseSessionMut<MSG>,
        mut message_list: IndexMap<MessageId, EphemeralNodeId>,
        now: Instant,
    ) -> Result<()> {
        let mut message_list: VecDeque<_> = message_list.drain(..).collect();
        while let Some((msg, src)) = message_list.pop_front() {
            let msg = match database.load_message(msg) {
                Ok(msg) => msg,
                Err(err) => {
                    tracing::warn!("could not find requested message {:?}: {:?}", msg, err);
                    continue;
                }
            };
            let msg_id = msg.id();

            if self
                .outbuf
                .recently_sent_message_ids
                .is_duplicate(msg.id(), now)
                == false
            {
                self.outbuf.push_back(DistributorMessage::Message {
                    origin: self
                        .neighborhood
                        .recent_messages_source
                        .recent_messages
                        .get(&msg_id)
                        .map(|x| x.original_source)
                        .unwrap_or(*self.ephemeral_node_id.get()),
                    message: SerializedMessage::new(msg)?,
                    source: *self.ephemeral_node_id.get(),
                    demand_ack: false,
                    explicit_retransmit: true,
                });
            }

            let children = database.get_message_children(msg_id)?;
            message_list.extend(children.iter().map(|x| (*x, src)));
            #[cfg(debug_assertions)]
            {
                let mut children = children;
                let mut actual_children = vec![];
                for child_msg in database.get_all_messages()? {
                    if child_msg.header.parents.contains(&msg_id) {
                        actual_children.push(child_msg.id());
                    }
                }
                actual_children.sort();
                children.sort();
                assert_eq!(actual_children, children);
            }
        }

        Ok(())
    }

    /// Returns list of messages whose parents are now known
    fn process_received_messages<MSG: Message>(
        &mut self,
        database: &mut DatabaseSessionMut<MSG>,
        mut message_list: Vec<AccumulatedMessage>,
        _now: Instant,
    ) -> Result<Vec<AccumulatedMessage>> {
        let mut released_list = Vec::new();

        message_list.sort_by_key(|x| x.msg.id);

        let mut chosen_messages = IndexMap::new();
        'msg_iter: for AccumulatedMessage {
            msg,
            source,
            origin,
            need_ack,
            explicit_retransmit,
        } in message_list.into_iter()
        {
            for parent in msg.parents.iter() {
                if database.contains_message(*parent)? == false
                    && !chosen_messages.contains_key(parent)
                // message_list is sorted by id (i.e, also by time), so parent should be found here
                {
                    warn!(
                        "Could not apply message {:?} because parent {:?} is not known",
                        msg.id, parent
                    );

                    let parent = *parent;

                    let accum = AccumulatedMessage {
                        msg,
                        source,
                        origin,
                        need_ack,
                        explicit_retransmit,
                    };
                    match self.outbuf.parentless_messages.entry(parent) {
                        Entry::Occupied(mut e) => {
                            if !e.get().iter().any(|x| x.msg.id == accum.msg.id) {
                                e.get_mut().push(accum);
                            }
                        }
                        Entry::Vacant(e) => {
                            e.insert(vec![accum]);
                        }
                    }
                    continue 'msg_iter;
                }
            }

            let already_present = database.contains_message(msg.id)?;
            if !already_present {
                if self
                    .neighborhood
                    .fast_pather
                    .write()
                    .unwrap()
                    .should_i_forward(origin.raw_u16(), source.raw_u16())
                {
                    self.outbuf.push_back(DistributorMessage::Message {
                        source: *self.ephemeral_node_id.get(),
                        message: msg.clone(),
                        demand_ack: false,
                        origin,
                        explicit_retransmit,
                    });
                }
            }

            if let Some(released) = self.outbuf.parentless_messages.swap_remove(&msg.id) {
                released_list.extend(released);
            }

            chosen_messages.insert(msg.id, (msg, need_ack));
        }

        let mut to_ack = vec![];
        let messages: Vec<_> = chosen_messages
            .into_values()
            .map(|(x, need_ack)| (x.to_message(), need_ack))
            .filter_map(|(x, need_ack)| match x {
                Ok(x) => {
                    if need_ack {
                        to_ack.push(x.header.id);
                    }
                    Some(x)
                }
                Err(x) => {
                    error!("Message could not be deserialized: {:?}", x);
                    None
                }
            })
            .inspect(|x| {
                debug!("Append received message: {:?}", x);
            })
            .collect();

        database.append_many(messages.iter(), false, false)?;
        if !to_ack.is_empty() {
            self.outbuf
                .push_back(DistributorMessage::SyncAllAck(to_ack));
        }
        Ok(released_list)
    }
}

#[cfg(test)]
mod tests {
    use super::truncate_to_address;
    #[test]
    fn do_test_truncate() {
        assert_eq!(
            truncate_to_address("012345678901234567890123456789012345678﷽")
                .0
                .as_str(),
            "012345678901234567890123456789012345678"
        );
        assert_eq!(
            truncate_to_address("0123456789012345678901234567890123456789A")
                .0
                .as_str(),
            "0123456789012345678901234567890123456789"
        );
        assert_eq!(truncate_to_address("abcd").0.as_str(), "abcd");
        assert_eq!(truncate_to_address("0123456789").0.as_str(), "0123456789");
        assert_eq!(
            truncate_to_address("01234567890123456789012345678901234567◌")
                .0
                .as_str(),
            "01234567890123456789012345678901234567"
        );
    }
}
