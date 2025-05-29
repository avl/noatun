use crate::colors::*;
#[cfg(feature = "tokio")]
use crate::communication::{Neighborhood, QueryableOutbuffer};
use crate::cutoff::{Acceptability, CutOffHashPos};
use crate::database::{DatabaseSession, DatabaseSessionMut};
use crate::{test_elapsed, Application, Database, Message, MessageFrame, MessageHeader, MessageId, NoatunTime};
use anyhow::Result;
use arrayvec::ArrayString;
use indexmap::{IndexMap, IndexSet};
use savefile_derive::Savefile;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::time::Duration;
use arcshift::ArcShift;
use indexmap::map::Entry;
use rand::{thread_rng, Rng};
use tracing::{debug, error, info, trace, warn};

pub const MAX_RECENT_SENT_MEMORY: usize = 100;

// Principle
// The node that is 'most ahead' (highest MessageId) has responsibility.
// If knows all the heads of other node, just sends perfect updates.
// Otherwise:
// Must request messages until it has complete picture

#[derive(Debug, Savefile, Clone)]
pub struct SerializedMessage {
    id: MessageId,
    parents: Vec<MessageId>,
    data: Vec<u8>,
}
impl SerializedMessage {
    pub fn message_id(&self) -> MessageId {
        self.id
    }
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

#[derive(Debug, Savefile, Clone)]
pub struct MessageSubGraphNode {
    pub(crate) id: MessageId,
    parents: Vec<MessageId>,
    query_count: usize,
}

pub struct MessageSubGraphNodeValue {
    parents: Vec<MessageId>,
    query_count: usize,
}

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
    pub const MAX_LENGTH: usize = 40;
    pub fn from(value: impl Display) -> Self {
        use std::fmt::Write;
        let mut x = ArrayString::<40>::new();
        #[cfg(debug_assertions)]
        {
            let mut s = String::new();
            write!(&mut s, "{value}").unwrap();
            assert!(s.len() <= 40, "address {} (len={}) was too long", s, s.len());
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

#[derive(Clone,Copy,PartialEq,Eq,Hash, Savefile, PartialOrd, Ord)]
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


static NON_RANDOM_EPHEMERAL_NODE_ID_COUNTER: std::sync::atomic::AtomicU16 =
    std::sync::atomic::AtomicU16::new(0);

impl EphemeralNodeId {
    pub fn raw_u16(self) -> u16 {
        self.0
    }
    pub fn new(value: u16) -> Self {
        EphemeralNodeId(value)
    }
    pub fn random() -> EphemeralNodeId {
        #[cfg(test)] {
            if crate::FOR_TEST_NON_RANDOM_ID {
                let id = NON_RANDOM_EPHEMERAL_NODE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return EphemeralNodeId(id);
            }
        }
        EphemeralNodeId(thread_rng().gen())
    }
}

//TODO: Add a quick-join mechanism. A newly started node can force all nodes to send an
//extra ReportHeads, after which the newly joined nodes sends one of its own. Thus we
//get rid of the 1-periodic msg delay of waiting for ReportHeads-messages

#[derive(Debug, Savefile, Clone, Copy)]
#[repr(u8)]
pub enum ForwardingChange {
    Add,
    Remove
}


#[derive(Debug, Savefile, Clone)]
pub enum DistributorMessage {
    /// Report all update heads for the sender
    ReportHeads {
        source: EphemeralNodeId,
        cutoff: CutOffHashPos,
        heads: Vec<MessageId>,
        neighbors: Vec<EphemeralNodeId>,
    },
    /// A query to tell if the listed messages are known.
    /// If they are, they should be requested by SyncAllRequest.
    SyncAllQuery(Vec<MessageId>),
    /// The given messages should be sent.
    SyncAllRequest(Vec<MessageId>),
    /// Sent only when doing a full sync
    SyncAllAck(Vec<MessageId>),
    /// Report a cut in the source node message graph
    RequestUpstream {
        source: EphemeralNodeId,
        /// the usize is How many levels to ascend from message
        query: Vec<(MessageId, usize)>,
        destination: EphemeralNodeId,
    },
    /// Response to a RequestUpstream, giving information about the message graph
    UpstreamResponse {
        source: EphemeralNodeId,
        dest: EphemeralNodeId,
        messages: Vec<MessageSubGraphNode>,
    },
    /// Command the recipient to send all descendants of the given messages
    SendMessageAndAllDescendants {
        source: EphemeralNodeId,
        destination: EphemeralNodeId,
        message_id: Vec<MessageId>,
    },
    /// Actual messages
    Message {
        source: EphemeralNodeId,
        message: SerializedMessage,
        demand_ack: bool,
        /// This message was created on the node that sent it.
        /// This means it cannot be squelched.
        origin: EphemeralNodeId,
    },
    /// Inform 'transmitter' that messages from 'origin' should not be
    /// transmitted when 'source' asks for them (because 'source' knows
    /// that some other node is responding to this.
    Forwarding {
        /// The node requesting the forwarding
        source: EphemeralNodeId,
        /// The origin from which this forwarding request concerns itself with
        origin: EphemeralNodeId,
        /// The node that is to create the forwading path,
        transmitter: EphemeralNodeId,
        /// True if the forward should be removed
        action: ForwardingChange,
    }
}

impl DistributorMessage {
    pub(crate) fn message_id(&self) -> Option<MessageId> {
        match self {
            DistributorMessage::Message{message: msg, ..} => Some(msg.id),
            _ => None,
        }
    }
    pub fn debug_format<M: Message>(&self) -> Result<String> {
        Ok(match self {
            DistributorMessage::ReportHeads{cutoff, heads, source, neighbors} => {
                format!(
                    "{}: cutoff: {cutoff}, heads: {:?}, src: {:?}, neigh: {:?}",
                    lightgray("ReportHeads"),
                    heads,
                    source,
                    neighbors
                )
            }
            DistributorMessage::Forwarding { source, origin, transmitter, action } => {
                format!(
                    "{}: source: {source} transmitter: {transmitter}, origin: {origin}, action: {action:?}",
                    brightred("Squelch"),
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
            DistributorMessage::RequestUpstream { source, query, destination } => {
                format!("{}: Query: {:?}, src: {:?}, dst: {:?}", red("RequestUpstream"), query, source, destination)
            }
            DistributorMessage::UpstreamResponse { source,dest, messages } => {
                format!(
                    "{}: Messages: {:?}, src: {:?}, dst: {:?}",
                    lightbrown("UpstreamResponse"),
                    messages,
                    source,
                    dest
                )
            }
            DistributorMessage::SendMessageAndAllDescendants { source, message_id, destination } => {
                format!(
                    "{}: messages: {:?}, src: {:?}, dst: {:?}",
                    pink("SendMessageAndAllDescendants"),
                    message_id,
                    source,
                    destination
                )
            }
            DistributorMessage::Message {
                source, message, demand_ack, origin
            } => {
                let msg: MessageFrame<M> = message.to_message_from_ref()?;
                format!(
                    "{}: id = {}, parents = {:?}, need_ack = {}, msg = {:?}, src = {:?}, origin = {:?}",
                    turqouise("Message"),
                    msg.header.id,
                    msg.header.parents,
                    demand_ack,
                    msg.payload,
                    source,
                    origin
                )
            }
        })
    }
}

struct MergedDistributorMessages {
    report_heads: IndexSet<MessageId>,
    requests: IndexMap<MessageId, /*count*/ usize>,
    responses: IndexSet<MessageSubGraphNode>,
    send_msg_and_descendants: IndexSet<MessageId>,
    actual_messages: Vec<SerializedMessage>,
}

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

#[derive(Debug, Default)]
pub struct DistributorStatus {
    nominal: bool,
    most_recent_clockdrift: HashMap<Address, NoatunTime>,
    most_recent_unsynced: HashMap<Address, NoatunTime>,
    have_heard_peer: bool,
}

pub struct Distributor {
    ephemeral_node_id: ArcShift<EphemeralNodeId>,
    // A sync-all request is in progress.
    // It sends all Messages in MessageId-order (which guarantees that all
    // parents will be sent before any children.
    sync_all_inprogress: SyncAllState,

    distributor_state: DistributorStatus,
    periodic_message_interval: Duration,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Status {
    Nominal,
    BadClocksDetected,
    OutOfSync,
    NoPeers,
    Synchronizing,
}

pub fn truncate_to_arraystring(name: &str) -> Address {
    if name.len() <= Address::MAX_LENGTH {
        println!("Name len: {}", name.len());
        return Address::from(name);
    }
    println!("Length: {}", name.len());
    for i in (1..=Address::MAX_LENGTH).rev() {
        println!("IS char boundary at {}: {}", i, name.is_char_boundary(i));
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
    /// Equal to source for messages created _at_ source
    origin: EphemeralNodeId,
    need_ack: bool,
}

enum SquelchAction {
    StartForwarding {
        forwarder: EphemeralNodeId,
        origin: EphemeralNodeId,
    },
    CancelForwardering {
        forwarder: EphemeralNodeId,
        origin: EphemeralNodeId,
    },
}
impl SquelchAction {
    fn origin(&self) -> EphemeralNodeId {
        match self {
            SquelchAction::StartForwarding { origin, .. } => *origin,
            SquelchAction::CancelForwardering { origin, .. } => *origin
        }
    }
}

impl Distributor {
    const BATCH_SIZE: usize = 20;
    pub(crate) fn new(report_head_interval: Duration, initial_node_id: ArcShift<EphemeralNodeId>) -> Distributor {
        Self {
            sync_all_inprogress: SyncAllState::NotActive,
            distributor_state: DistributorStatus::default(),
            periodic_message_interval: report_head_interval,
            ephemeral_node_id: initial_node_id
        }
    }

    pub fn node_id(&mut self) -> EphemeralNodeId {
        *self.ephemeral_node_id.get()
    }


    /// Returns the current status of the system with respect to clock drift.
    ///
    /// If clock drift has been observed with the last 60 seconds, it will be reported
    /// by this method.
    pub(crate) fn get_status(&self, now: NoatunTime) -> Status {
        for drift in self.distributor_state.most_recent_clockdrift.values() {
            if drift.elapsed_ms_since(now) < 60000 {
                return Status::BadClocksDetected;
            }
        }
        for unsync in self.distributor_state.most_recent_unsynced.values() {
            let unsync_t = unsync.elapsed_ms_since(now);
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
    pub(crate) fn get_periodic_message<APP: Application>(
        &mut self,
        database: &DatabaseSession<APP>,
        neighbors: &Neighborhood
    ) -> Result<Vec<DistributorMessage>> {
        let mut heads = database.get_update_heads().to_vec();
        heads.sort();
        let mut temp = vec![DistributorMessage::ReportHeads {
            source: *self.ephemeral_node_id.get(),
            cutoff: database.current_cutoff_state()?,
            heads,
            neighbors: neighbors.peers.get_neighbors(),
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
    pub(crate) fn receive_message2<APP: Application>(
        &mut self,
        database: &mut Database<APP>,
        input: impl Iterator<Item = DistributorMessage>,
        neighborhood: &mut Neighborhood
    ) -> Result<Vec<DistributorMessage>> {
        let mut buf = QueryableOutbuffer::new(self.periodic_message_interval);
        self.receive_message(
            database,
            input.map(|x| (Address::from("src"), x)),
            &mut buf,
            neighborhood
        )?;
        Ok(buf.outbuf.into_iter().collect())
    }

    pub(crate) fn receive_message<APP: Application>(
        &mut self,
        database: &mut Database<APP>,
        input: impl Iterator<Item = (Address, DistributorMessage)>,
        outbuf: &mut QueryableOutbuffer,
        neighborhood: &mut Neighborhood,
    ) -> Result<()> {

        let mut database = database.begin_session_mut()?;
        let mut accumulated_heads: IndexMap<MessageId, Vec<EphemeralNodeId>> = IndexMap::new();
        let mut accumulated_request_upstream = IndexMap::new();
        let mut accumulated_upstream_responses:  IndexMap<MessageId, /*parents*/ (EphemeralNodeId/*src*/, EphemeralNodeId/*dst*/, MessageSubGraphNodeValue)> = IndexMap::new();
        let mut accumulated_send_msg_and_descendants = IndexMap::new();
        let mut accumulated_serialized = vec![];
        let mut accumulated_sync_all_queries = IndexSet::new();
        let mut accumulated_sync_all_requests = IndexSet::new();

        for (src, item) in input {
            neighborhood.record_message(&item, *self.ephemeral_node_id.get(), self.periodic_message_interval);

            self.distributor_state.have_heard_peer = true;
            match item {
                DistributorMessage::Forwarding {..} => {
                }
                DistributorMessage::SyncAllQuery(query) => {
                    accumulated_sync_all_queries.extend(query);
                }
                DistributorMessage::SyncAllRequest(requests) => {
                    accumulated_sync_all_requests.extend(requests);
                }

                DistributorMessage::ReportHeads{ source, cutoff: cutoff_hash, heads, neighbors } => {
                    for pass in 0..2 {
                        match database.is_acceptable_cutoff_hash(cutoff_hash)? {
                            Acceptability::Nominal => {
                                // If the neighbor has no neighbors of its own, it's just starting up.
                                // Let's wait a bit before acting on its messages
                                if self.sync_all_inprogress.idle() && neighbors.is_empty() == false {
                                    for head in heads {
                                        let sources = accumulated_heads.entry(head).or_default();
                                        if !sources.contains(&source) {
                                            sources.push(source);
                                        }
                                    }
                                }
                                debug!("Acceptability: Nominal");
                                self.distributor_state.most_recent_unsynced.remove(&src);
                                self.distributor_state.most_recent_clockdrift.remove(&src);

                                let peer= neighborhood.peers.get_insert_peer(source);
                                debug_assert!(neighbors.is_sorted());
                                peer.peer_neighbors.clone_from(&neighbors);
                                neighborhood.peers.recalculate_summary(*self.ephemeral_node_id.get());

                                break;
                            }
                            Acceptability::Unacceptable => {
                                debug!("Acceptability: Unacceptable");
                                self.distributor_state
                                    .most_recent_unsynced
                                    .insert(src, database.noatun_now());
                                if self.sync_all_inprogress.idle() {
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
                                    .insert(src, database.noatun_now());
                                break;
                            }
                        }
                    }
                }
                DistributorMessage::RequestUpstream { source, query, destination } => {
                    //if let Some(peer) = neighborhood.peers.get_peer(source) {
                        for (msg, count) in query {
                            // TODO: Consider if this is fast enough to do unbatched here? (and is batching really faster?)
                            if !database.contains_message(msg)? {
                                println!("Database {} doesn't contain {:?}", self.ephemeral_node_id.get(), msg);
                                continue;
                            }
                            trace!("Considering {:?} query: {:?}", source, msg);
                            if destination == *self.ephemeral_node_id.get() {
                                let accum_count = accumulated_request_upstream.entry(msg).or_insert((0usize, source));
                                accum_count.0 = (accum_count.0).max(count);
                                accum_count.1 = (accum_count.1).min(source);
                            }
                        }
                    //}
                }
                DistributorMessage::UpstreamResponse { source, dest, messages } => {
                    if dest == *self.ephemeral_node_id.get() {
                        for msg in messages {
                            let val = MessageSubGraphNodeValue {
                                parents: msg.parents,
                                query_count: msg.query_count,
                            };
                            match accumulated_upstream_responses.entry(msg.id) {
                                Entry::Occupied(mut e) => {
                                    if e.get_mut().0 < source {
                                        e.insert((source, dest,val));
                                    }
                                }
                                Entry::Vacant(e) => {
                                    e.insert((source, dest,val));
                                }
                            }
                        }
                    }
                }
                DistributorMessage::SendMessageAndAllDescendants { source, message_id, destination } => {
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
                DistributorMessage::Message{ source, message:mut msg, demand_ack: need_ack, origin } => {
                    database.remove_cutoff_parents(&mut msg.parents);
                    accumulated_serialized.push(AccumulatedMessage{msg, need_ack, source, origin});
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

        self.process_reported_heads(&mut database, accumulated_heads, outbuf, neighborhood)?;

        accumulated_request_upstream.sort_keys();

        self.process_request_upstream(&mut database, accumulated_request_upstream, outbuf)?;
        self.process_upstream_response(&mut database, accumulated_upstream_responses, outbuf, neighborhood)?;
        self.process_send_message_all_descendants(
            &mut database,
            accumulated_send_msg_and_descendants,
            outbuf,
            neighborhood
        )?;

        let mut forwarding_cmds = vec![];


        for i in 0..1000 {
            if i == 1000 -1 {
                error!("Unexpected iteration count {}", i);
            }
            // Temp is messages that previously couldn't be imported because they
            // were missing their parents. One or more parents have now become known.
            let temp = self.process_received_messages(&mut database, std::mem::take(&mut accumulated_serialized), outbuf, neighborhood, &mut forwarding_cmds)?;
            if temp.is_empty() {
                break;
            }
            accumulated_serialized = temp;
        }
        self.process_sync_all_queries(&mut database, accumulated_sync_all_queries, outbuf)?;
        self.process_sync_all_requests(&mut database, accumulated_sync_all_requests, outbuf, neighborhood)?;

        {
            let origins = vec![];
            for squelch in forwarding_cmds.iter().rev() {
                let origin = squelch.origin();
                if origins.contains(&origin) {
                    continue;
                }
                outbuf.push_back(DistributorMessage::Forwarding {
                    source: self.node_id(),
                    origin,
                    transmitter: origin,

                    action: match squelch {
                        SquelchAction::StartForwarding { .. } => {ForwardingChange::Add}
                        SquelchAction::CancelForwardering { .. } => {ForwardingChange::Remove}
                    },
                })
            }

        }



        Ok(())
    }

    fn process_sync_all_queries<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_sync_all_queries: IndexSet<MessageId>,
        output: &mut QueryableOutbuffer,
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
            output.push_back(DistributorMessage::SyncAllRequest(request));
        }
        if !acks.is_empty() {
            output.push_back(DistributorMessage::SyncAllAck(acks));
        }
        Ok(())
    }

    fn process_sync_all_requests<APP: Application>(
        &mut self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_sync_all_requests: IndexSet<MessageId>,
        output: &mut QueryableOutbuffer,
        neighborhood: &Neighborhood
    ) -> Result<()> {
        for request in accumulated_sync_all_requests {
            match database.load_message(request) {
                Ok(msg) => {
                    if output.recently_sent_message_ids.is_duplicate(msg.id()) == false {
                        output.push_back(DistributorMessage::Message {
                            source: *self.ephemeral_node_id.get(),
                            origin: neighborhood.recent_messages.recent_messages.get(&msg.id()).map(|x|x.original_source).unwrap_or(*self.ephemeral_node_id.get()),
                            message: SerializedMessage::new(msg)?,
                            demand_ack: true,
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

    fn process_reported_heads<APP: Application>(
        &mut self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_heads: IndexMap< MessageId, Vec<EphemeralNodeId>>,
        existing_outbuf: &mut QueryableOutbuffer,
        neighborhood: &mut Neighborhood //TODO: Maybe outbuf and neighborhood should be fields on distributor?
    ) -> Result<()> {
        self.distributor_state.nominal = true;

        let mut messages_to_request_from_source = IndexMap::<_,Vec<_>>::new();
        for (message, srcs) in accumulated_heads {

            assert!(!srcs.is_empty());
            if database.contains_message(message)? {
                continue;
            }

            messages_to_request_from_source.entry(srcs[0]).or_default().push(message);
        }
        for (src,msgs) in messages_to_request_from_source.into_iter() {
                println!("{:?} at {:?} Messages to request from {:?}: {:?}", test_elapsed(), self.ephemeral_node_id.get(), src,msgs);
                existing_outbuf.request_upstream(&msgs, *self.ephemeral_node_id.get(), src, neighborhood);
                self.distributor_state.nominal = false;
        }
        Ok(())
    }
    fn process_request_upstream<APP: Application>(
        &mut self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_heads: IndexMap<MessageId, (usize, /*src:*/EphemeralNodeId)>,
        output: &mut QueryableOutbuffer,
    ) -> Result<()> {
        let mut by_src : IndexMap<EphemeralNodeId, Vec<(MessageId, usize)>> = IndexMap::new();

        for (msg,(count,src)) in accumulated_heads {
            by_src.entry(src).or_default().push((msg, count));
        }
        for (src, heads) in by_src {
            let messages: Vec<MessageSubGraphNode> = database
                .get_upstream_of(heads.into_iter())?
                //TODO: Can we re-enable this? .filter(|x|output.upstream_response_blocked(x.0.id) == false)
                .map(|(msg, query_count)| MessageSubGraphNode {
                    id: msg.id,
                    parents: msg.parents,
                    query_count,
                })
                .collect();

            /*if neighbors.is_inhibited(request_from, self_node, |info|&mut info.request_inhibited_based_on_node_numbers) {
                return;
            }*/

            if !messages.is_empty() {
                output.push_back(DistributorMessage::UpstreamResponse { messages, dest: src, source: *self.ephemeral_node_id.get() });
            }
        }

        Ok(())
    }
    fn process_upstream_response<APP: Application>(
        &mut self,
        database: &mut DatabaseSessionMut<APP>,
        upstream_response: IndexMap<MessageId, /*parents*/ (EphemeralNodeId/*src*/, EphemeralNodeId/*dst*/, MessageSubGraphNodeValue)>,
        queryable_outbuffer: &mut QueryableOutbuffer,
        neighborhood: &mut Neighborhood,
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
            let have_all_parents = msg_value.parents.iter().all(|x| {
                database
                    .contains_message(*x)
                    .map_err(|e| {
                        eprintln!("Error: {e:?}");
                        err = Err(e);
                    })
                    .is_ok_and(|x| x)
            });
            debug_assert!(err.is_ok());
            err?;


            let all_parents_are_also_in_request = msg_value
                .parents
                .iter()
                .all(|x| upstream_response.contains_key(x));

            if have_all_parents || all_parents_are_also_in_request {
                // We have all the parents, a perfect msg to request!
                info!(
                    "Requesting msg.id={:?}, because we have all its parents: {:?}",
                    msg_id, msg_value.parents
                );
                if self.ephemeral_node_id.get().0 == 0 {
                    println!("process_upstream_response - adding cmd");
                }

                /*if neighborhood.is_inhibited(*msg_source, self.ephemeral_node_id, |info|&mut info.send_msg_and_descendants_based_on_node_numbers) {
                    continue;
                }*/
                send_cmds.insert(*msg_id, msg_source);
                continue;
            }
            if self.ephemeral_node_id.get().0 == 0 {
                println!("process_upstream_response {msg_id:?} - didn't have all parents, and wasn't requesting them");
            }
            if !all_parents_are_also_in_request {
                unknowns.entry(*msg_source).or_default().extend(
                    msg_value
                        .parents
                        .iter()
                        .map(|x| (*x, (2 * msg_value.query_count).min(256))),
                );
            }
        }
        if send_cmds.is_empty() == false {
            let mut msg_by_source = IndexMap::<_,Vec<_>>::new();
            for (k,v) in send_cmds {
                msg_by_source.entry(*v).or_default().push(k);
            }
            for (src, messages) in msg_by_source {
                queryable_outbuffer.push_back(DistributorMessage::SendMessageAndAllDescendants {
                    message_id: messages,
                    source: *self.ephemeral_node_id.get(),
                    destination: src
                });
            }
        }
        if unknowns.is_empty() == false {
            for (src_node,unknowns) in unknowns {
                if self.ephemeral_node_id.get().0 == 0 {
                    println!("process_upstream_response requesting {:?} from {:?}", unknowns, src_node);
                }
                let unknowns = unknowns.into_iter().map(|x|x.0).collect::<Vec<_>>();
                queryable_outbuffer.request_upstream(&unknowns, *self.ephemeral_node_id.get(), src_node, neighborhood);
            }
        }

        Ok(())
    }
    fn process_send_message_all_descendants<APP: Application>(
        &mut self,
        database: &mut DatabaseSessionMut<APP>,
        mut message_list: IndexMap<MessageId, EphemeralNodeId>,
        output: &mut QueryableOutbuffer,
        neighborhood: &mut Neighborhood
    ) -> Result<()> {
        let mut message_list : VecDeque<_> = message_list.drain(..).collect();
        while let Some((msg, src)) = message_list.pop_front() {
            println!("{} Considering request to send {:?}", self.ephemeral_node_id.get(), msg);
            let msg = match database.load_message(msg) {
                Ok(msg) => msg,
                Err(err) => {
                    tracing::warn!("could not find requested message {:?}: {:?}", msg, err);
                    println!("{} doesn't have message!", self.ephemeral_node_id.get());
                    continue;
                }
            };
            let msg_id = msg.id();

            if output.recently_sent_message_ids.is_duplicate(msg.id()) == false {
                println!("It's not recently sent!");
                output.push_back(DistributorMessage::Message {
                    origin: neighborhood.recent_messages.recent_messages.get(&msg_id).map(|x| x.original_source).unwrap_or(*self.ephemeral_node_id.get()),
                    message: SerializedMessage::new(msg)?,
                    source: *self.ephemeral_node_id.get(),
                    demand_ack: false,
                });
            }


            let mut children = database.get_message_children(msg_id)?;
            println!("Queueing it.");
            message_list.extend(children.iter().map(|x|(*x, src)));
            #[cfg(debug_assertions)]
            {
                let mut actual_children = vec![];
                for child_msg in database.get_all_messages()? {
                    if child_msg.header.parents.contains(&msg_id) {
                        actual_children.push(child_msg.id());
                    }
                }
                //println!("Actual: {:?}", actual_children);
                actual_children.sort();
                children.sort();
                assert_eq!(actual_children, children);
            }
        }

        Ok(())
    }

    /// Returns list of messages whose parents are now known
    fn process_received_messages<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        mut message_list: Vec<AccumulatedMessage>,
        output: &mut QueryableOutbuffer,
        neighborhood: &mut Neighborhood,
        forward_cmds: &mut Vec<SquelchAction>
    ) -> Result<Vec<AccumulatedMessage>> {
        let mut released_list = Vec::new();
        database.maybe_advance_cutoff()?;

        message_list.sort_by_key(|x| x.msg.id);

        let mut chosen_messages = IndexMap::new();
        'msg_iter: for AccumulatedMessage{msg, source, origin, need_ack } in message_list.into_iter() {
            for parent in msg.parents.iter() {
                if database.contains_message(*parent)? == false
                    && !chosen_messages.contains_key(parent)
                // message_list is sorted by id (i.e, also by time), so parent should be found here
                {
                    // we don't have this message's parents!
                    // this could mean we're missing a reliable path

                    neighborhood.record_tentative_missing_path(*parent, source, origin);

                    println!("{:?} {:?} MISSING PARENT {:?} of {:?}", test_elapsed(), *self.ephemeral_node_id.shared_get(), parent, msg.id);

                    warn!(
                        "Could not apply message {:?} because parent {:?} is not known",
                        msg.id, parent
                    );

                    let pathdata = output.origin_paths.entry(origin).or_default();
                    pathdata.missing_paths += 1;
                    if pathdata.missing_paths == 4 {
                        forward_cmds.push(SquelchAction::StartForwarding{origin, forwarder: source});
                    }

                    let parent = *parent;
                    let accum = AccumulatedMessage{ msg, source, origin, need_ack };
                    match output.parentless_messages.entry(parent) {
                        Entry::Occupied(mut e) => {
                            e.get_mut().push(accum);
                        }
                        Entry::Vacant(e) => {
                            e.insert(vec![accum]);
                        }
                    }
                    continue 'msg_iter;
                }
            }

            match output.origin_paths.entry(origin) {
                // We got something with original source 'origin', so maybe we have a forwarding path
                Entry::Occupied(o) => {
                    let data = o.get();
                    if let Some(forward_from) = data.forwarding_from {
                        if source < forward_from{
                            // Seems there's an alternate path, cancel previous forwarding path
                            forward_cmds.push(SquelchAction::CancelForwardering{origin, forwarder: forward_from});
                        }
                    }
                    o.swap_remove();
                }
                Entry::Vacant(_) => {}
            }

            // Apparently we now hear directly from this origin, surely we don't need any forwarding
            match output.origin_paths.entry(source) {
                Entry::Occupied(mut o) => {
                    let origindata = o.get_mut();
                    if let Some(forward_from) = origindata.forwarding_from {
                        forward_cmds.push(SquelchAction::CancelForwardering{origin: source, forwarder: forward_from});
                    }
                    o.swap_remove();
                }
                Entry::Vacant(_) => {
                }
            }

            neighborhood.check_if_tentative_missing_path_was_actually_ok(msg.id); //TODO: Remove this and the whole Quarantine type :)

            if let Some(released) = output.parentless_messages.swap_remove(&msg.id) {
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
            output.push_back(DistributorMessage::SyncAllAck(to_ack));
        }
        Ok(released_list)
    }
}

#[cfg(test)]
mod tests {
    use super::truncate_to_arraystring;
    #[test]
    fn do_test_truncate() {
        assert_eq!(truncate_to_arraystring("012345678901234567890123456789012345678﷽").0.as_str(), "012345678901234567890123456789012345678");
        assert_eq!(truncate_to_arraystring("0123456789012345678901234567890123456789A").0.as_str(), "0123456789012345678901234567890123456789");
        assert_eq!(truncate_to_arraystring("abcd").0.as_str(), "abcd");
        assert_eq!(truncate_to_arraystring("0123456789").0.as_str(), "0123456789");
        assert_eq!(truncate_to_arraystring("01234567890123456789012345678901234567◌").0.as_str(), "01234567890123456789012345678901234567");
    }
}
