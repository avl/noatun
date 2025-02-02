use crate::cutoff::CutoffHash;
use crate::message_store::{ReadPod, WritePod};
use crate::{Application, Database, Message, MessageHeader, MessageId, MessagePayload};
use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use indexmap::{IndexMap, IndexSet};
use libc::send;
use savefile_derive::Savefile;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::ops::Range;
use tracing::{debug, error, info};
// Principle
// The node that is 'most ahead' (highest MessageId) has responsibility.
// If knows all the heads of other node, just sends perfect updates.
// Otherwise:
// Must request messages until it has complete picture

#[derive(Debug, Savefile, Clone)]
pub struct SerializedMessage {
    /// TODO: The serialized part should be just the user-part of the message.
    /// Parents and id should be serialized by noatun directly.
    id: MessageId,
    parents: Vec<MessageId>,
    data: Vec<u8>,
}
impl SerializedMessage {
    pub fn to_message<M: MessagePayload>(self) -> Result<Message<M>> {
        let mut reader = Cursor::new(&self.data);
        Ok(Message {
            header: MessageHeader {
                id: self.id,
                parents: self.parents,
            },
            payload: M::deserialize(&self.data[reader.position() as usize..])?,
        })
    }
    pub fn new<M: MessagePayload>(m: Message<M>) -> Result<SerializedMessage> {
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
    ReportHeads(CutoffHash, Vec<MessageId>),
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
    BeginQuery(MessageId),
    QueryActive(MessageId, MessageId, IndexSet<MessageId>),
}

pub struct Distributor {
    // A sync-all request is in progress.
    // It sends all Messages in MessageId-order (which guarantees that all
    // parents will be sent before any children.
    sync_all_inprogress: SyncAllState,
}

impl Default for Distributor {
    fn default() -> Self {
        Self::new()
    }
}
impl Distributor {
    const BATCH_SIZE: usize = 20;
    pub fn new() -> Distributor {
        Self {
            sync_all_inprogress: SyncAllState::NotActive,
        }
    }

    /// Call this to retrieve a message that should be sent periodically
    pub fn get_periodic_message<APP: Application>(
        &mut self,
        database: &Database<APP>,
    ) -> Result<Vec<DistributorMessage>> {
        let mut temp = vec![DistributorMessage::ReportHeads(
            database.nominal_cutoffhash()?,
            database.get_update_heads().to_vec(),
        )];
        let sync_from = match &self.sync_all_inprogress {
            SyncAllState::NotActive => None,
            SyncAllState::Starting => Some(MessageId::ZERO),
            SyncAllState::BeginQuery(start) => Some(*start),
            SyncAllState::QueryActive(from, to, request_identity) => Some(*from),
        };
        if let Some(sync_from) = sync_from {
            let cur_batch = database.get_messages_at_or_after(sync_from, Self::BATCH_SIZE)?;
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

    pub fn receive_message<APP: Application>(
        &mut self,
        database: &mut Database<APP>,
        input: impl Iterator<Item = DistributorMessage>,
    ) -> Result<Vec<DistributorMessage>> {
        let mut accumulated_heads: IndexSet<MessageId> = IndexSet::new();
        let mut accumulated_upstream_queries = IndexMap::new();
        let mut accumulated_responses = IndexMap::new();
        let mut accumulated_send_msg_and_descendants = IndexSet::new();
        let mut accumulated_serialized = vec![];
        let mut accumulated_sync_all_queries = IndexSet::new();
        let mut accumulated_sync_all_requests = IndexSet::new();
        for item in input {
            match item {
                DistributorMessage::SyncAllQuery(query) => {
                    accumulated_sync_all_queries.extend(query);
                }
                DistributorMessage::SyncAllRequest(requests) => {
                    accumulated_sync_all_requests.extend(requests);
                }

                DistributorMessage::ReportHeads(cutoff_hash, heads) => {
                    if database.is_acceptable_cutoff_hash(cutoff_hash)? {
                        accumulated_heads.extend(heads);
                    } else {
                        self.sync_all_inprogress = SyncAllState::Starting;
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
                        for ack in acked {
                            items.swap_remove(&ack);
                        }
                        if items.is_empty() {
                            self.sync_all_inprogress = SyncAllState::BeginQuery(*to);
                        }
                    }
                    SyncAllState::NotActive => {}
                    SyncAllState::Starting => {}
                    SyncAllState::BeginQuery(_) => {}
                },
            }
        }
        let mut output = Vec::new();

        self.process_reported_heads(database, accumulated_heads, &mut output);

        // TODO: Consider if this is the best solution.
        // process_request_upstream presently requires sorted input. But should it?
        accumulated_upstream_queries.sort_keys();

        self.process_request_upstream(database, accumulated_upstream_queries, &mut output);
        self.process_upstream_response(database, accumulated_responses, &mut output);
        self.process_send_message_all_descendants(
            database,
            accumulated_send_msg_and_descendants,
            &mut output,
        );
        self.process_received_messages(database, accumulated_serialized, &mut output);
        self.process_sync_all_queries(database, accumulated_sync_all_queries, &mut output);
        self.process_sync_all_requests(database, accumulated_sync_all_requests, &mut output);
        Ok(output)
    }

    fn process_sync_all_queries<APP: Application>(
        &self,
        database: &mut Database<APP>,
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
        database: &mut Database<APP>,
        accumulated_sync_all_requests: IndexSet<MessageId>,
        output: &mut Vec<DistributorMessage>,
    ) -> Result<()> {
        for request in accumulated_sync_all_requests {
            let msg = database.load_message(request)?;
            output.push(DistributorMessage::Message(
                SerializedMessage::new(msg)?,
                true,
            ));
        }
        Ok(())
    }

    fn process_reported_heads<APP: Application>(
        &self,
        database: &mut Database<APP>,
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
        }
        Ok(())
    }
    fn process_request_upstream<APP: Application>(
        &self,
        database: &mut Database<APP>,
        accumulated_heads: IndexMap<MessageId, usize>,
        output: &mut Vec<DistributorMessage>,
    ) -> Result<()> {
        let mut response: IndexMap<MessageId, APP::Message> = IndexMap::new();
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
        database: &mut Database<APP>,
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
        database: &mut Database<APP>,
        mut message_list: IndexSet<MessageId>,
        output: &mut Vec<DistributorMessage>,
    ) -> Result<()> {
        while let Some(msg) = message_list.pop() {
            let msg = database.load_message(msg)?;
            let msg_id = msg.id();
            output.push(DistributorMessage::Message(
                SerializedMessage::new(msg)?,
                false,
            ));

            //TODO: Make smarter, this is super-inefficient
            for child_msg in database.get_all_messages()? {
                if child_msg.header.parents.iter().any(|x| *x == msg_id) {
                    message_list.insert(child_msg.id());
                }
            }
        }

        Ok(())
    }

    fn process_received_messages<APP: Application>(
        &self,
        database: &mut Database<APP>,
        message_list: Vec<(SerializedMessage, /*need ack*/ bool)>,
        output: &mut Vec<DistributorMessage>,
    ) -> Result<()> {
        for (msg,_) in message_list.iter() {
            for parent in msg.parents.iter() {
                if database.contains_message(*parent)? == false {
                    compile_error!("Consider why this happens. Packet loss? What do we do?");
                    panic!("Should never apply msg with unknown parent")
                }
            }
        }

        let mut to_ack = vec![];
        let messages = message_list
            .into_iter()
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
                debug!(
                    "Append received message: {:?}",
                    x
                );
            });

        database.append_many(messages, false)?;
        if !to_ack.is_empty() {
            output.push(DistributorMessage::SyncAllAck(to_ack));
        }
        Ok(())
    }
}
