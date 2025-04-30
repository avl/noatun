use crate::cutoff::{Acceptability, CutOffHashPos};
use crate::{Application, Database, Message, MessageFrame, MessageHeader, MessageId, NoatunTime};
use anyhow::Result;
use arrayvec::ArrayString;
use indexmap::{IndexMap, IndexSet};
use savefile_derive::Savefile;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use tracing::{debug, error, info, warn};
use crate::database::{DatabaseSession, DatabaseSessionMut};
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
    id: MessageId,
    parents: Vec<MessageId>,
    query_count: usize,
}

pub struct MessageSubGraphNodeValue {
    parents: Vec<MessageId>,
    query_count: usize,
}

#[derive(Debug, Savefile, Clone)]
pub enum DistributorMessage {
    /// Report all update heads for the sender
    ReportHeads(CutOffHashPos, Vec<MessageId>, ArrayString<10>),
    /// A query if the listed messages are known.
    /// If they are, they should be requested by SyncAllRequest.
    /// The id of this query is the xor of all these message ids
    SyncAllQuery(Vec<MessageId>),
    /// The given messages should be sent.
    /// CutoffHash is xor of all messages in query
    SyncAllRequest(Vec<MessageId>),
    /// Sent only when doing a full sync
    SyncAllAck(Vec<MessageId>),
    /// Report a cut in the source node message graph
    RequestUpstream {
        /// the usize is How many levels to ascend from message
        query: Vec<(MessageId, usize)>,
    },
    UpstreamResponse {
        messages: Vec<MessageSubGraphNode>,
    },
    /// Command the recipient to send all descendants of the given message
    SendMessageAndAllDescendants {
        message_id: Vec<MessageId>,
    },
    Message(SerializedMessage, bool /*demand ack*/),
}

impl DistributorMessage {
    pub(crate) fn message_id(&self) -> Option<MessageId> {
        match self {
            DistributorMessage::Message(msg, _) => Some(msg.id),
            _ => None,
        }
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

#[derive(Debug, Default)]
pub struct DistributorStatus {
    nominal: bool,
    most_recent_clockdrift: HashMap<ArrayString<10>, NoatunTime>,
    most_recent_unsynced: HashMap<ArrayString<10>, NoatunTime>,
    have_heard_peer: bool,
}

pub struct Distributor {
    // A sync-all request is in progress.
    // It sends all Messages in MessageId-order (which guarantees that all
    // parents will be sent before any children.
    sync_all_inprogress: SyncAllState,

    distributor_state: DistributorStatus,
    own_name: ArrayString<10>,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Status {
    Nominal,
    BadClocksDetected,
    OutOfSync,
    NoPeers,
    Synchronizing,
}

pub fn truncate_to_arraystring(name: &str) -> ArrayString<10> {
    if name.len() <= 10 {
        return name.try_into().unwrap();
    }
    for i in (1..=10).rev() {
        if name.is_char_boundary(i) {
            return name.split_at(i).0.try_into().unwrap();
        }
    }
    "?".try_into().unwrap()
}


impl Distributor {
    const BATCH_SIZE: usize = 20;
    pub(crate)  fn new(node_name: &str) -> Distributor {
        Self {
            sync_all_inprogress: SyncAllState::NotActive,
            distributor_state: DistributorStatus::default(),
            own_name: truncate_to_arraystring(node_name),
        }
    }

    /// Returns the current status of the system with respect to clock drift.
    ///
    /// If clock drift has been observed with the last 60 seconds, it will be reported
    /// by this method.
    pub(crate) fn get_status(&self, now: NoatunTime) -> Status {
        for drift in self.distributor_state.most_recent_clockdrift.values() {
            if drift.elapsed_ms_since(now) < 60000
            {
                return Status::BadClocksDetected;
            }
        }
        for unsync in self.distributor_state.most_recent_unsynced.values() {
            let unsync_t = unsync.elapsed_ms_since(now);
            if unsync_t < 60000
            {
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
    ) -> Result<Vec<DistributorMessage>> {
        let mut temp = vec![DistributorMessage::ReportHeads(
            database.current_cutoff_state()?,
            database.get_update_heads().to_vec(),
            self.own_name,
        )];
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

    pub(crate) fn receive_message<APP: Application>(
        &mut self,
        database: &mut Database<APP>,
        input: impl Iterator<Item = DistributorMessage>,
    ) -> Result<Vec<DistributorMessage>> {
        let mut database = database.begin_session_mut()?;
        let mut accumulated_heads: IndexSet<MessageId> = IndexSet::new();
        let mut accumulated_upstream_queries = IndexMap::new();
        let mut accumulated_responses = IndexMap::new();
        let mut accumulated_send_msg_and_descendants = IndexSet::new();
        let mut accumulated_serialized = vec![];
        let mut accumulated_sync_all_queries = IndexSet::new();
        let mut accumulated_sync_all_requests = IndexSet::new();
        for item in input {
            self.distributor_state.have_heard_peer = true;
            match item {
                DistributorMessage::SyncAllQuery(query) => {
                    accumulated_sync_all_queries.extend(query);
                }
                DistributorMessage::SyncAllRequest(requests) => {
                    accumulated_sync_all_requests.extend(requests);
                }

                DistributorMessage::ReportHeads(cutoff_hash, heads, src) => {
                    accumulated_heads.extend(heads);
                    for pass in 0..2 {
                        match database.is_acceptable_cutoff_hash(cutoff_hash)? {
                            Acceptability::Nominal => {
                                info!("Acceptability: Nominal");
                                self.distributor_state.most_recent_unsynced.remove(&src);
                                self.distributor_state.most_recent_clockdrift.remove(&src);
                                break;
                            }
                            Acceptability::Unacceptable => {
                                info!("Acceptability: Unacceptable");
                                self.distributor_state
                                    .most_recent_unsynced
                                    .insert(src, database.noatun_now());
                                self.sync_all_inprogress = SyncAllState::Starting;
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
                                    error!("unexpected case, cutoff hash considered undecided twice")
                                }
                            }
                            Acceptability::UnacceptablePeerClockDrift => {
                                info!("Acceptability: Clockdrift");
                                self.distributor_state
                                    .most_recent_clockdrift
                                    .insert(src, database.noatun_now());
                                break;
                            }
                        }
                    }
                }
                DistributorMessage::RequestUpstream { query } => {
                    for (msg, count) in query {
                        let accum_count = accumulated_upstream_queries.entry(msg).or_insert(0usize);
                        *accum_count = (*accum_count).max(count);
                    }
                }
                DistributorMessage::UpstreamResponse { messages } => {
                    for msg in messages {
                        let val = MessageSubGraphNodeValue {
                            parents: msg.parents,
                            query_count: msg.query_count,
                        };
                        accumulated_responses.insert(msg.id, val);
                    }
                }
                DistributorMessage::SendMessageAndAllDescendants { message_id } => {
                    accumulated_send_msg_and_descendants.extend(message_id);
                }
                DistributorMessage::Message(msg, need_ack) => {
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
        let mut output = Vec::new();

        self.process_reported_heads(&mut database, accumulated_heads, &mut output)?;

        accumulated_upstream_queries.sort_keys();

        self.process_request_upstream(&mut database, accumulated_upstream_queries, &mut output)?;
        self.process_upstream_response(&mut database, accumulated_responses, &mut output)?;
        self.process_send_message_all_descendants(
            &mut database,
            accumulated_send_msg_and_descendants,
            &mut output,
        )?;
        self.process_received_messages(&mut database, accumulated_serialized, &mut output)?;
        self.process_sync_all_queries(&mut database, accumulated_sync_all_queries, &mut output)?;
        self.process_sync_all_requests(&mut database, accumulated_sync_all_requests, &mut output)?;
        Ok(output)
    }

    fn process_sync_all_queries<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_sync_all_queries: IndexSet<MessageId>,
        output: &mut Vec<DistributorMessage>,
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
            output.push(DistributorMessage::SyncAllRequest(request));
        }
        if !acks.is_empty() {
            output.push(DistributorMessage::SyncAllAck(acks));
        }
        Ok(())
    }

    fn process_sync_all_requests<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        accumulated_sync_all_requests: IndexSet<MessageId>,
        output: &mut Vec<DistributorMessage>,
    ) -> Result<()> {
        for request in accumulated_sync_all_requests {
            match database.load_message(request) {
                Ok(msg) => {
                    output.push(DistributorMessage::Message(
                        SerializedMessage::new(msg)?,
                        true,
                    ));
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
        output: &mut Vec<DistributorMessage>,
    ) -> Result<()> {
        let mut messages_to_request = vec![];
        for message in accumulated_heads {
            if database.contains_message(message)? {
                continue;
            }
            messages_to_request.push((message, 4));
        }
        if !messages_to_request.is_empty() {
            output.push(DistributorMessage::RequestUpstream {
                query: messages_to_request,
            });
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
        output: &mut Vec<DistributorMessage>,
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
            output.push(DistributorMessage::UpstreamResponse { messages });
        }
        Ok(())
    }
    fn process_upstream_response<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        upstream_response: IndexMap<MessageId, /*parents*/ MessageSubGraphNodeValue>,
        output: &mut Vec<DistributorMessage>,
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
                        eprintln!("Error: {:?}", e);
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
                        .map(|x| (*x, msg_value.query_count)),
                );
            }
        }
        if send_cmds.is_empty() == false {
            output.push(DistributorMessage::SendMessageAndAllDescendants {
                message_id: send_cmds,
            });
        }
        if unknowns.is_empty() == false {
            output.push(DistributorMessage::RequestUpstream {
                query: unknowns.into_iter().collect(),
            });
        }

        Ok(())
    }
    fn process_send_message_all_descendants<APP: Application>(
        &self,
        database: &mut DatabaseSessionMut<APP>,
        mut message_list: IndexSet<MessageId>,
        output: &mut Vec<DistributorMessage>,
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
            output.push(DistributorMessage::Message(
                SerializedMessage::new(msg)?,
                false,
            ));


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
        output: &mut Vec<DistributorMessage>,
    ) -> Result<()> {
        database.maybe_advance_cutoff()?;

        message_list.sort_by_key(|x| x.0.id);

        let mut chosen_messages = IndexMap::new();
        'msg_iter: for (msg, need_ack) in message_list.into_iter() {
            for parent in msg.parents.iter() {
                if database.contains_message(*parent)? == false
                    && !chosen_messages.contains_key(parent) // message_list is sorted by id (i.e, also by time), so parent should be found here
                {
                    // TODO: Does this still happen?
                    // There is an edge-case, where a message is removed immediately after having been
                    // received, because it's before cutoff and it has no effect.
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
            output.push(DistributorMessage::SyncAllAck(to_ack));
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
