use crate::colors::*;
use crate::communication::{Neighborhood, QueryableOutbuffer};
use crate::cutoff::{Acceptability, CutOffHashPos};
use crate::database::{DatabaseSession, DatabaseSessionMut};
use crate::{Application, Database, Message, MessageFrame, MessageHeader, MessageId, NoatunTime};
use anyhow::Result;
use arrayvec::ArrayString;
use indexmap::{IndexMap, IndexSet};
use savefile_derive::Savefile;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::time::Duration;
use rand::{thread_rng, Rng};
use tracing::{debug, error, info, warn};

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

#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash, Savefile)]
pub struct EphemeralNodeId(u16);

impl Display for EphemeralNodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}


static NON_RANDOM_EPHEMERAL_NODE_ID_COUNTER: std::sync::atomic::AtomicU16 =
    std::sync::atomic::AtomicU16::new(0);

impl EphemeralNodeId {
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
    },
    /// Response to a RequestUpstream, giving information about the message graph
    UpstreamResponse {
        source: EphemeralNodeId,
        messages: Vec<MessageSubGraphNode>,
    },
    /// Command the recipient to send all descendants of the given messages
    SendMessageAndAllDescendants {
        source: EphemeralNodeId,
        message_id: Vec<MessageId>,
    },
    /// Actual messages
    Message {
        source: EphemeralNodeId,
        message: SerializedMessage,
        demand_ack: bool
    },
    /// Inform 'transmitter' that messages from 'origin' should not be
    /// transmitted when 'source' asks for them (because 'source' knows
    /// that some other node is responding to this.
    Squelch {
        /// The node requesting the squelch
        source: EphemeralNodeId,
        /// The node that is to be squelched
        transmitter: EphemeralNodeId,
        /// Messages that 'source' didn't want to receive.
        /// 'transmitter' is intended to understand that any other messages with
        /// the same original source (from its perspective) shouldn't be distributed either.
        /// This, because apparently, 'source' has another path that allows it to receive
        /// 'messages'. The squelch has an unspecified but decaying lifetime.
        messages: Vec<MessageId>,
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
            DistributorMessage::Squelch { source, transmitter, messages } => {
                format!(
                    "{}: source: {source} transmitter: {transmitter}, messages: {:?}",
                    brightred("Squelch"),
                    messages,
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
            DistributorMessage::RequestUpstream { source, query } => {
                format!("{}: Query: {:?}, src: {:?}", red("RequestUpstream"), query, source)
            }
            DistributorMessage::UpstreamResponse { source, messages } => {
                format!(
                    "{}: Messages: {:?}, src: {:?}",
                    lightbrown("UpstreamResponse"),
                    messages,
                    source
                )
            }
            DistributorMessage::SendMessageAndAllDescendants { source, message_id } => {
                format!(
                    "{}: messages: {:?}, src: {:?}",
                    pink("SendMessageAndAllDescendants"),
                    message_id,
                    source
                )
            }
            DistributorMessage::Message {
                source, message, demand_ack
            } => {
                let msg: MessageFrame<M> = message.to_message_from_ref()?;
                format!(
                    "{}: id = {}, parents = {:?}, need_ack = {}, msg = {:?}, src = {:?}",
                    turqouise("Message"),
                    msg.header.id,
                    msg.header.parents,
                    demand_ack,
                    msg.payload,
                    source
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
    ephemeral_node_id: EphemeralNodeId,
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
        return Address::from(name);
    }
    for i in (1..=Address::MAX_LENGTH).rev() {
        if name.is_char_boundary(i) {
            return Address::from(name.split_at(i).0);
        }
    }
    Address::from("?")
}

impl QueryableOutbuffer {
    pub(crate) fn is_empty(&self) -> bool {
        self.outbuf.is_empty()
    }
    pub(crate) fn pop_front(&mut self) -> Option<DistributorMessage> {
        let msg = self.outbuf.pop_front();
        if let Some(msg) = &msg {
            self.recent_sent.push_back(msg.clone());
            if self.recent_sent.len() > MAX_RECENT_SENT_MEMORY {
                self.recent_sent.pop_front();
            }
        }
        msg
    }
    pub(crate) fn push_back(&mut self, msg: DistributorMessage) {
        self.outbuf.push_back(msg);
    }

    pub(crate) fn extend<I: IntoIterator<Item = DistributorMessage>>(&mut self, items: I) {
        self.outbuf.extend(items);
    }
    pub(crate) fn has_request_for(&mut self, message: MessageId) -> bool {
        self.outbuf.iter().chain(self.recent_sent.iter()).any(|x| {
            println!("has-request-for checking: {x:?}");
            if let DistributorMessage::RequestUpstream { query, .. } = x {
                query.iter().any(|x| x.0 == message)
            } else {
                false
            }
        })
    }
}

impl Distributor {
    const BATCH_SIZE: usize = 20;
    pub(crate) fn new(report_head_interval: Duration, initial_node_id: EphemeralNodeId) -> Distributor {
        Self {
            sync_all_inprogress: SyncAllState::NotActive,
            distributor_state: DistributorStatus::default(),
            periodic_message_interval: report_head_interval,
            ephemeral_node_id: initial_node_id
        }
    }

    pub fn node_id(&self) -> EphemeralNodeId {
        self.ephemeral_node_id
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
            source: self.ephemeral_node_id,
            cutoff: database.current_cutoff_state()?,
            heads,
            neighbors: neighbors.get_neighbors(),
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
    ) -> Result<Vec<DistributorMessage>> {
        let mut buf = QueryableOutbuffer {
            outbuf: Default::default(),
            recent_sent: Default::default(),
            request_upstream_message_inhibit: Default::default(),
            request_upstream_message_inhibit_counter: 0,
            periodic_message_interval: self.periodic_message_interval,
        };
        self.receive_message(
            database,
            input.map(|x| (Address::from("src"), x)),
            &mut buf,
            &mut Default::default()
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
        let mut accumulated_heads: IndexSet<MessageId> = IndexSet::new();
        let mut accumulated_upstream_queries = IndexMap::new();
        let mut accumulated_responses = IndexMap::new();
        let mut accumulated_send_msg_and_descendants = IndexSet::new();
        let mut accumulated_serialized = vec![];
        let mut accumulated_sync_all_queries = IndexSet::new();
        let mut accumulated_sync_all_requests = IndexSet::new();

        for (src, item) in input {
            neighborhood.record_message(&item, self.ephemeral_node_id);

            self.distributor_state.have_heard_peer = true;
            match item {
                DistributorMessage::Squelch {..} => {
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
                                if self.sync_all_inprogress.idle() {
                                    accumulated_heads.extend(heads);
                                }
                                info!("Acceptability: Nominal");
                                self.distributor_state.most_recent_unsynced.remove(&src);
                                self.distributor_state.most_recent_clockdrift.remove(&src);

                                let peer= neighborhood.get_insert_peer(source);
                                peer.peer_neighbors.clone_from(&neighbors);

                                break;
                            }
                            Acceptability::Unacceptable => {
                                info!("Acceptability: Unacceptable");
                                self.distributor_state
                                    .most_recent_unsynced
                                    .insert(src, database.noatun_now());
                                if self.sync_all_inprogress.idle() {
                                    self.sync_all_inprogress = SyncAllState::Starting;
                                }
                                break;
                            }
                            Acceptability::Undecided(advance) => {
                                info!("Acceptability: Undecided");
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
                                if self.sync_all_inprogress.idle() {
                                    accumulated_heads.extend(heads);
                                }
                                info!("Acceptability: Clockdrift");
                                self.distributor_state
                                    .most_recent_clockdrift
                                    .insert(src, database.noatun_now());
                                break;
                            }
                        }
                    }
                }
                DistributorMessage::RequestUpstream { source, query } => {
                    if let Some(peer) = neighborhood.get_peer(source) {
                        for (msg, count) in query {
                            //let origin = neighborhood.recent_messages.recent_messages.get(&msg);
                            if peer.we_should_answer_request(self.ephemeral_node_id) {
                                let accum_count = accumulated_upstream_queries.entry(msg).or_insert(0usize);
                                *accum_count = (*accum_count).max(count);
                            }
                        }
                    }
                }
                DistributorMessage::UpstreamResponse { source:_, messages } => {
                    for msg in messages {
                        let val = MessageSubGraphNodeValue {
                            parents: msg.parents,
                            query_count: msg.query_count,
                        };
                        accumulated_responses.insert(msg.id, val);
                    }
                }
                DistributorMessage::SendMessageAndAllDescendants { source:_, message_id } => {
                    accumulated_send_msg_and_descendants.extend(message_id);
                }
                DistributorMessage::Message{message:msg, demand_ack: need_ack, ..} => {
                    accumulated_serialized.push((msg, need_ack));
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

        self.process_reported_heads(&mut database, accumulated_heads, outbuf)?;

        accumulated_upstream_queries.sort_keys();

        self.process_request_upstream(&mut database, accumulated_upstream_queries, outbuf)?;
        self.process_upstream_response(&mut database, accumulated_responses, outbuf)?;
        self.process_send_message_all_descendants(
            &mut database,
            accumulated_send_msg_and_descendants,
            outbuf,
        )?;
        self.process_received_messages(&mut database, accumulated_serialized, outbuf)?;
        self.process_sync_all_queries(&mut database, accumulated_sync_all_queries, outbuf)?;
        self.process_sync_all_requests(&mut database, accumulated_sync_all_requests, outbuf)?;
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
        &self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_sync_all_requests: IndexSet<MessageId>,
        output: &mut QueryableOutbuffer,
    ) -> Result<()> {
        for request in accumulated_sync_all_requests {
            match database.load_message(request) {
                Ok(msg) => {
                    output.push_back(DistributorMessage::Message {
                        source: self.ephemeral_node_id,
                        message: SerializedMessage::new(msg) ?,
                        demand_ack: true,
                    });
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
        accumulated_heads: IndexSet<MessageId>,
        existing_outbuf: &mut QueryableOutbuffer,
    ) -> Result<()> {
        let mut messages_to_request = vec![];
        for message in accumulated_heads {
            if database.contains_message(message)? {
                continue;
            }
            if existing_outbuf.has_request_for(message) {
                continue;
            }
            if messages_to_request
                .iter()
                .any(|x: &(MessageId, usize)| x.0 == message)
            {
                continue;
            }
            //println!("REQUEST_UPSTREAM inner (from within process__reporteD_heads");
            messages_to_request.push((message, 4));
        }
        if !messages_to_request.is_empty() {
            //println!("REQUEST_UPSTREAM (from within process__reporteD_heads");
            existing_outbuf.request_upstream(messages_to_request.into_iter(), self.ephemeral_node_id);
            self.distributor_state.nominal = false;
        } else {
            self.distributor_state.nominal = true;
        }
        Ok(())
    }
    fn process_request_upstream<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_heads: IndexMap<MessageId, usize>,
        output: &mut QueryableOutbuffer,
    ) -> Result<()> {
        let messages: Vec<MessageSubGraphNode> = database
            .get_upstream_of(accumulated_heads.into_iter())?
            .map(|(msg, query_count)| MessageSubGraphNode {
                id: msg.id,
                parents: msg.parents,
                query_count,
            })
            .collect();
        if !messages.is_empty() {
            output.push_back(DistributorMessage::UpstreamResponse { messages, source: self.ephemeral_node_id });
        }
        Ok(())
    }
    fn process_upstream_response<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        upstream_response: IndexMap<MessageId, /*parents*/ MessageSubGraphNodeValue>,
        queryable_outbuffer: &mut QueryableOutbuffer,
    ) -> Result<()> {
        let mut unknowns: HashSet<(MessageId, usize)> = HashSet::new();
        let mut send_cmds = vec![];
        for (msg_id, msg_value) in upstream_response.iter() {
            if database.contains_message(*msg_id)? {
                continue; //We already have this one
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

            err?;
            if have_all_parents {
                // We have all the parents, a perfect msg to request!
                info!(
                    "Requesting msg.id={:?}, because we have all its parents: {:?}",
                    msg_id, msg_value.parents
                );
                send_cmds.push(*msg_id);
                continue;
            }

            let all_parents_are_also_in_request = msg_value
                .parents
                .iter()
                .all(|x| upstream_response.contains_key(x));
            if !all_parents_are_also_in_request {
                unknowns.extend(
                    msg_value
                        .parents
                        .iter()
                        .filter(|x| !queryable_outbuffer.has_request_for(**x))
                        .map(|x| (*x, (2 * msg_value.query_count).min(256))),
                );
            }
        }
        if send_cmds.is_empty() == false {
            queryable_outbuffer.push_back(DistributorMessage::SendMessageAndAllDescendants {
                message_id: send_cmds,
                source: self.ephemeral_node_id,
            });
        }
        if unknowns.is_empty() == false {
            //println!("REQUEST_UPSTREAM (from within process_upstream_response");
            queryable_outbuffer.request_upstream(unknowns.into_iter(), self.ephemeral_node_id);
        }

        Ok(())
    }
    fn process_send_message_all_descendants<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        mut message_list: IndexSet<MessageId>,
        output: &mut QueryableOutbuffer,
    ) -> Result<()> {
        while let Some(msg) = message_list.pop() {
            let msg = match database.load_message(msg) {
                Ok(msg) => msg,
                Err(err) => {
                    tracing::warn!("could not find requested message {:?}: {:?}", msg, err);
                    continue;
                }
            };
            let msg_id = msg.id();
            output.push_back(DistributorMessage::Message {
                message: SerializedMessage::new(msg)?,
                source: self.ephemeral_node_id,
                demand_ack: false,
            });

            let mut children = database.get_message_children(msg_id)?;
            message_list.extend(children.iter().copied());
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

    fn process_received_messages<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        mut message_list: Vec<(SerializedMessage, /*need ack*/ bool)>,
        output: &mut QueryableOutbuffer,
    ) -> Result<()> {
        database.maybe_advance_cutoff()?;

        message_list.sort_by_key(|x| x.0.id);

        let mut chosen_messages = IndexMap::new();
        'msg_iter: for (msg, need_ack) in message_list.into_iter() {
            for parent in msg.parents.iter() {
                if database.contains_message(*parent)? == false
                    && !chosen_messages.contains_key(parent)
                // message_list is sorted by id (i.e, also by time), so parent should be found here
                {
                    warn!(
                        "Could not apply message {:?} because parent {:?} is not known",
                        msg.id, parent
                    );
                    continue 'msg_iter;
                }
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::truncate_to_arraystring;
    #[test]
    fn do_test_truncate() {
        assert_eq!(&truncate_to_arraystring("abcd"), "abcd");
        assert_eq!(&truncate_to_arraystring("0123456789"), "0123456789");
        assert_eq!(&truncate_to_arraystring("0123456789A"), "0123456789");
        assert_eq!(&truncate_to_arraystring("012345678﷽"), "012345678");
        assert_eq!(&truncate_to_arraystring("01234567◌"), "01234567");
    }
}
