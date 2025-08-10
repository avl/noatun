use crate::distributor::{
    Address, Distributor, DistributorMessage, EphemeralNodeId, SerializedMessage, Status,
};

use crate::colors::rgb;
use crate::communication::size_limit_vec_deque::{MeasurableSize, SizeLimitVecDeque};
use crate::communication::udp::TokioUdpDriver;
use crate::{track_node, Database, Message, NoatunTime};
use anyhow::{anyhow, bail, Result};
use arrayvec::ArrayString;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Utc};
use indexmap::{IndexMap, IndexSet};
use savefile::{
    Deserialize, Deserializer, Introspect, IntrospectItem, Packed, SavefileError, Schema,
    Serialize, Serializer, WithSchema, WithSchemaContext,
};
use savefile_derive::Savefile;

use crate::mini_pather::MiniPather;
use crate::xxh3_vendored::xxh3::Xxh3Default;
use arcshift::ArcShift;
use indexmap::map::Entry;
use std::collections::{BTreeSet, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;
use std::{io, thread};
use metrics::{counter, describe_counter, Counter, Unit};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::error::Elapsed;
use tokio::time::Instant;
use tokio::{select, spawn};
use tracing::{debug, error, info, instrument, trace, warn};
use crate::diagnostics::DiagnosticsData;
use crate::diagnostics::{MessageRow, PacketRow};

pub mod size_limit_vec_deque;
pub mod udp;

#[doc(hidden)]
#[derive(Savefile, Debug)]
pub enum NetworkPacket {
    Data(TransmittedEntitySortable),
    RetransmitRequest {
        /// The destination for the retransmit-request
        who: EphemeralNodeId,
        what: Vec<u64>,
    },
}

const IP_HEADER_SIZE: usize = 20;
const UDP_HEADER_SIZE: usize = 8;
const NOATUN_NETWORK_PACKET_OVERHEAD: usize = 1;
const NOATUN_TRANSMITTED_ENTITY_OVERHEAD: usize = 6;
const APPROX_HEADER_SIZE: usize = IP_HEADER_SIZE
    + UDP_HEADER_SIZE
    + NOATUN_NETWORK_PACKET_OVERHEAD
    + NOATUN_TRANSMITTED_ENTITY_OVERHEAD;

#[allow(async_fn_in_trait)] //For now
pub trait CommunicationDriver: Sync + Send {
    type Receiver: CommunicationReceiveSocket<Self::Endpoint> + Send + Sync;
    type Sender: CommunicationSendSocket<Self::Endpoint> + Send + Sync;

    /// This is the address type used by this driver.
    ///
    /// Apart from the trait bounds, there are few requirements on these addresses.
    /// There are no hard requirements, but performance will be better if the following
    /// is fulfilled:
    ///  * No two neighboring nodes should appear to have the same address
    ///
    /// However, there are some things which are explicitly not required:
    ///  * Remote nodes can be behind NAT. This means that there's no requirement that
    ///    the addresses we see are the same addresses seen by other nodes. However,
    ///    performance may be better if all nodes see the same source addresses for all
    ///    packets.
    ///  * Specifically, remote nodes may not perceive us as having the address
    ///    Self::own_address
    ///  * Addresses don't have to be globally unique. It's completely okay for two
    ///    nodes to have the same address. However, if they are within communication
    ///    distance of each other, sharing the same address can lead to decreased performance.
    type Endpoint: Eq + Debug + Hash + Send + Sync + Copy + Display;
    async fn initialize(
        &mut self,
        bind_address: &str,
        multicast_group: &str,
        mtu: usize,
    ) -> Result<(Self::Sender, Self::Receiver)>;
    fn parse_endpoint(s: &str) -> Result<Self::Endpoint>;
}
#[allow(async_fn_in_trait)] //For now
pub trait CommunicationReceiveSocket<Endpoint: PartialEq + Debug + Send> {
    /// Receive a message from the network.
    ///
    /// Loopback messages should be avoided - implementations are encouraged
    /// to avoid receiving messages that were transmitted by the same noatun instance,
    /// for performance reasons. Correctness is preserved in any case.
    fn recv_buf_from<B: bytes::buf::BufMut + Send>(
        &mut self,
        buf: &mut B,
    ) -> impl std::future::Future<Output = std::io::Result<(usize, Option<Endpoint>)>> + Send;
}
#[allow(async_fn_in_trait)] //For now
pub trait CommunicationSendSocket<Endpoint: PartialEq + Debug + Send> {
    /// In many networks, it's possible to uniquely know the address that
    /// transmitted messages will appear to originate from. This information,
    /// if available, is only used by Noatun for logging and debugging. It
    /// can be particularly useful in unit tests, to more easily understand
    /// the origin of messages.
    ///
    /// Noatun does not require implementors to implement this, and it does
    /// not affect Noatun behavior.
    fn local_addr(&self) -> Result<Option<Endpoint>> {
        Ok(None)
    }
    fn send_to(&mut self, buf: &[u8]) -> impl std::future::Future<Output = io::Result<()>> + Send;
}

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct TransmittedEntitySortable {
    #[doc(hidden)]
    pub seq: u16,
    #[doc(hidden)]
    pub data: Vec<u8>,
    // Note, u16::MAX signifies "no boundary"
    #[doc(hidden)]
    pub first_boundary: u16,
    #[doc(hidden)]
    pub src: EphemeralNodeId,
}

impl Introspect for TransmittedEntitySortable {
    fn introspect_value(&self) -> String {
        format!(
            "TransmittedEntity(#{},first_boundary={},len={})",
            self.seq,
            self.first_boundary,
            self.data.len()
        )
    }

    fn introspect_child<'a>(&'a self, _index: usize) -> Option<Box<dyn IntrospectItem<'a> + 'a>> {
        None
    }
}

impl WithSchema for TransmittedEntitySortable {
    fn schema(_version: u32, _context: &mut WithSchemaContext) -> Schema {
        Schema::Custom("TransmittedEntity".into())
    }
}
impl Packed for TransmittedEntitySortable {}
impl Serialize for TransmittedEntitySortable {
    fn serialize(
        &self,
        serializer: &mut Serializer<impl Write>,
    ) -> std::result::Result<(), SavefileError> {
        serializer.write_u16(self.seq)?;
        serializer.write_u16(self.src.raw_u16())?;
        assert!(self.data.len() <= u16::MAX as usize);
        serializer.write_u16(self.data.len() as u16)?;
        serializer.write_bytes(&self.data)?;
        serializer.write_u16(self.first_boundary)?;
        Ok(())
    }
}
impl Deserialize for TransmittedEntitySortable {
    fn deserialize(
        deserializer: &mut Deserializer<impl Read>,
    ) -> std::result::Result<Self, SavefileError> {
        let seq = deserializer.read_u16()?;
        let src = EphemeralNodeId::new(deserializer.read_u16()?);
        let datalen = deserializer.read_u16()?;
        let data = deserializer.read_bytes(datalen as usize)?;
        let first_boundary = deserializer.read_u16()?;
        Ok(Self {
            seq,
            data,
            first_boundary,
            src,
        })
    }
}

impl TransmittedEntitySortable {
    fn free(&self, max_payload: usize) -> usize {
        max_payload.saturating_sub(self.data.len())
    }
}

/// This represents a transmitted entity with a full (possibly reconstructed) 64 bit
/// sequence number
#[derive(Debug)]
struct TransmittedEntityWithFullSeq {
    reconstructed_seq: u64,
    entity: TransmittedEntitySortable,
}

impl MeasurableSize for TransmittedEntityWithFullSeq {
    fn size_bytes(&self) -> usize {
        self.entity.data.len() + size_of_val(&self.reconstructed_seq) + size_of_val(&self.entity)
    }
}

struct ReceiveTrack {
    have_valid_accum: bool,
    accum: VecDeque<u8>,
    expected_next: u64,
    sorted_packets: VecDeque<TransmittedEntityWithFullSeq>,
    retransmit_interval: Duration,
    disable_retransmit: bool,
    last_success: Instant,
}

enum ReceiveResult {
    Nominal,
    RestartTrack,
}

impl ReceiveTrack {
    /// How long are packets kept in the retransmit window.
    /// I.e, after this has passed, they're lost forever.
    pub const RECEIVER_RETRANSMIT_WINDOW: usize = 1000;
    pub const SENDER_RETRANSMIT_WINDOW: usize = ReceiveTrack::RECEIVER_RETRANSMIT_WINDOW / 5;
    pub const RETRANSMIT_WINDOW_U16: u16 = if Self::RECEIVER_RETRANSMIT_WINDOW > u16::MAX as usize {
        panic!("RETRANSMIT_WINDOW constant value too large")
    } else {
        Self::RECEIVER_RETRANSMIT_WINDOW as u16
    };

    pub(crate) fn new(retransmit_interval: Duration, retransmit_disabled: bool) -> Self {
        ReceiveTrack {
            have_valid_accum: true,
            accum: Default::default(),
            expected_next: 0,
            sorted_packets: Default::default(),
            retransmit_interval,
            disable_retransmit: retransmit_disabled,
            last_success: Instant::now(),
        }
    }

    pub(crate) fn reconstruct_seq(&self, seq: u16) -> u64 {
        Self::reconstruct_seq_impl(self.expected_next, seq)
    }
    pub(crate) fn reconstruct_seq_impl(expected_next: u64, seq: u16) -> u64 {
        let short_delta = seq.wrapping_sub(expected_next as u16);

        if short_delta >= 65535 - Self::RETRANSMIT_WINDOW_U16 {
            // A retransmission, that we don't actually need
            let diff = (-(short_delta as i64 - 65536)) as u64;
            if diff <= expected_next {
                return expected_next - diff;
            }
        }
        // Interpret as future value
        expected_next + short_delta as u64
    }

    async fn process(
        &mut self,
        packet: TransmittedEntitySortable,
        raw_address: Option<Address>,
        packet_source: EphemeralNodeId,

        // Messages that have been received by socket, and are to be signaled
        // to upper layer
        message_tx: &mut Vec<(Option<Address>, EphemeralNodeId, Vec<u8>)>,
        retransmit_requests: &mut IndexMap<EphemeralNodeId, RetransmitInfo>,

        retransmit_responsibility_query: &mut (dyn FnMut(EphemeralNodeId) -> Duration
                  + 'static
                  + Sync
                  + Send),
        new_track: bool,
    ) -> Result<()> {
        let reconstructed_seq = self.reconstruct_seq(packet.seq);

        let packet = TransmittedEntityWithFullSeq {
            reconstructed_seq,
            entity: packet,
        };
        let Err(insert_point) = self
            .sorted_packets
            .binary_search_by_key(&packet.reconstructed_seq, |x| x.reconstructed_seq)
        else {
            // Already existed
            debug!("Already had packet: {:?}", &packet);
            return Ok(());
        };
        self.sorted_packets.insert(insert_point, packet);

        let packet_count = self.sorted_packets.len();
        if reconstructed_seq == 0 {
            self.have_valid_accum = true;
            debug_assert!(self.accum.is_empty());
            self.accum.clear();
        }
        while let Some(first) = self.sorted_packets.front() {
            if let Some(accum_retransmits) = retransmit_requests.get_mut(&packet_source) {
                if accum_retransmits.items.remove(&reconstructed_seq) {
                    accum_retransmits.outstanding_retransmit = None;
                }

                if accum_retransmits.items.is_empty() {
                    retransmit_requests.swap_remove(&packet_source);
                }
            }

            if first.reconstructed_seq != self.expected_next {
                let mut give_up_on_retransmit = false;
                if let Some(accum_retransmits) = retransmit_requests.get_mut(&packet_source) {
                    if let Some(outstanding) = accum_retransmits.outstanding_retransmit {
                        if Instant::now().saturating_duration_since(outstanding)
                            > 2 * self.retransmit_interval
                        {
                            give_up_on_retransmit = true;
                            accum_retransmits.outstanding_retransmit = None;
                            accum_retransmits.items.clear();
                            accum_retransmits.wait_until = None;
                        }
                    }
                }

                if self.disable_retransmit
                    || packet_count > Self::SENDER_RETRANSMIT_WINDOW
                    || give_up_on_retransmit
                    || new_track
                {
                    self.accum.clear();
                    self.have_valid_accum = false;
                    self.expected_next = first.reconstructed_seq;
                } else {
                    trace!(
                        "Expected next: {}, actual (reconstructed) seq: {}",
                        self.expected_next,
                        first.reconstructed_seq
                    );
                    if first.reconstructed_seq < self.expected_next {
                        info!(
                            "duplicate packet ignored (expected {}, got {})",
                            self.expected_next, first.reconstructed_seq
                        );
                        self.sorted_packets.pop_front();
                        continue;
                    }

                    debug_assert_eq!(first.entity.src, packet_source);
                    // See: #packet_retransmit_logic
                    let retransmission_delay = retransmit_responsibility_query(first.entity.src);

                    trace!(
                        "Appending retransmit-request for {:?} {:?} (delay: {:?})",
                        packet_source,
                        self.expected_next,
                        retransmission_delay
                    );
                    let accum_retransmits = retransmit_requests.entry(packet_source).or_default();
                    for x in self.expected_next..first.reconstructed_seq {
                        accum_retransmits.items.insert(x);
                    }
                    if accum_retransmits.wait_until.is_none() {
                        accum_retransmits.wait_until = Some(Instant::now() + retransmission_delay);
                    }
                    let mut cur = first.reconstructed_seq;
                    'loop1: for key in &self.sorted_packets {
                        while cur < key.reconstructed_seq {
                            accum_retransmits.items.insert(cur);
                            cur += 1;
                            if accum_retransmits.items.len() > 100 {
                                break 'loop1;
                            }
                        }
                        cur = key.reconstructed_seq + 1;
                    }
                    assert!(accum_retransmits.items.is_empty() == false);

                    return Ok(());
                }
            }

            self.last_success = Instant::now();

            if first.entity.first_boundary == u16::MAX {
                if self.have_valid_accum {
                    self.accum.extend(&first.entity.data);
                }
            } else {
                if self.have_valid_accum {
                    self.accum
                        .extend(&first.entity.data[0..first.entity.first_boundary as usize]);
                }
                if !self.accum.is_empty() {
                    /*tx_finished_received_frame
                    .try_send((
                        Address::from(packet_source),
                        self.accum.iter().copied().collect(),
                    ))
                    .unwrap();*/
                    message_tx.push((
                        raw_address,
                        packet_source,
                        self.accum.iter().copied().collect(),
                    ));
                    self.accum.clear();
                }
                self.have_valid_accum = true;
                let mut cur_boundary = first.entity.first_boundary as usize;
                let mut reader = Cursor::new(&first.entity.data);
                reader.seek(SeekFrom::Start(cur_boundary as u64))?;

                while cur_boundary < first.entity.data.len() {
                    let next_size = reader.read_u16::<LittleEndian>()? as usize;
                    cur_boundary += 2;
                    if next_size == u16::MAX as usize {
                        break;
                    }
                    let mut temp = vec![0; next_size];
                    reader.read_exact(&mut temp)?;
                    if !temp.is_empty() {
                        /*tx_finished_received_frame
                        .try_send((Address::from(packet_source), temp.clone()))
                        .unwrap();*/
                        message_tx.push((raw_address, packet_source, temp));
                    }

                    cur_boundary += next_size;
                }
                self.accum.extend(&first.entity.data[cur_boundary..]);
            }
            self.expected_next = self.expected_next.wrapping_add(1);
            self.sorted_packets.pop_front();
        }
        Ok(())
    }
}

struct RetransmitInfo {
    wait_until: Option<Instant>,
    items: BTreeSet<u64>,

    /// When sending a retransmit message, this is set to Some(now) (if not already Some).
    /// Whenever a retransmission is received, we set it to None.
    /// If enough time passes without us hearing the requested messages retransmitted,
    /// we give up on the retransmission, and clear all retransmit items
    outstanding_retransmit: Option<Instant>,
}
impl Default for RetransmitInfo {
    fn default() -> Self {
        Self {
            wait_until: None,
            items: Default::default(),
            outstanding_retransmit: None,
        }
    }
}

impl<Socket: CommunicationDriver> SenderLoopTrait<Socket::Endpoint>
    for MulticastSenderLoop<Socket>
{
    fn make_context(&self) -> Result<ExecutionContext<Socket::Endpoint>> {
        self.create_context()
    }

    fn has_send_capacity(&self, context: &mut ExecutionContext<Socket::Endpoint>) -> bool {
        context.cursend.is_empty() && self.queue.is_empty()
    }

    //TODO(future): Think about if this still makes sense. Should we have two select loops nested
    // like this? (This method is a select loop, and it's called in a select loop).
    async fn pump(
        &mut self,
        context: &mut ExecutionContext<Socket::Endpoint>,
        message_tx: &mut Vec<(Option<Address>, EphemeralNodeId, Vec<u8>)>,
        message_rx: &mut Vec<Vec<u8>>,
        node_id_collision_detected: &mut bool,
        diagnostics: Option<&Mutex<DiagnosticsData>>,
    ) -> Result<bool> {
        self.run(context, message_tx, message_rx, node_id_collision_detected, diagnostics)
            .await
    }
}

struct MulticastSenderLoop<Socket: CommunicationDriver> {
    send_socket: Socket::Sender,
    receive_socket: Socket::Receiver,
    bind_address: Option<Socket::Endpoint>,
    /// Transmitted messages kept in out queue, to allow retransmitting
    history: SizeLimitVecDeque<TransmittedEntityWithFullSeq>,
    queue: VecDeque<TransmittedEntityWithFullSeq>,
    outgoing_retransmit_requests: IndexMap<EphemeralNodeId, RetransmitInfo>,
    receive_track: IndexMap<(EphemeralNodeId, Option<Socket::Endpoint>), ReceiveTrack>,
    quit_rx: tokio::sync::oneshot::Receiver<()>,
    /// Sent to net
    //message_rx: Receiver<Vec<u8>>,
    /// Received from net
    //message_tx: Sender<(Address /*src*/, Vec<u8>)>,
    last_send: Instant,
    last_send_size: usize,
    recvbuf: Vec<u8>,
    max_payload_per_packet: usize,
    next_send_seq: u64,
    limiter: BwLimiter,
    /// The timeout before a retransmit is initiated
    retransmit_interval: Duration,
    disable_retransmit: bool,
    ephemeral_node_id: ArcShift<EphemeralNodeId>,
    retransmit_responsibility_query:
        Box<dyn FnMut(/*src: */ EphemeralNodeId) -> Duration + Sync + Send + 'static>,
    hasher: Xxh3Default,
    sent_packets: IndexSet<u128>,
    gc_time: Instant,
    raw_packets_received: Counter,
    valid_packets_received: Counter,
    packets_sent: Counter,
    retransmit_request_count: Counter,
}
impl<Socket: CommunicationDriver> Drop for MulticastSenderLoop<Socket> {
    fn drop(&mut self) {
        trace!("Dropping MulticastSenderLoop loop");
    }
}

struct BwLimiter {
    bytes_per_second: u64,
    last_update: Instant,
    debt: u64,
}

impl BwLimiter {
    fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            last_update: tokio::time::Instant::now(),
            debt: 0,
        }
    }
    async fn wait_debt_free(&self) {
        let payment = self.last_update.elapsed().as_secs_f32() * self.bytes_per_second as f32;
        let debt = self.debt as f32 - payment;
        if debt > 0.0 {
            let time_to_pay_seconds = debt / (self.bytes_per_second as f32);
            tokio::time::sleep(tokio::time::Duration::from_secs_f32(time_to_pay_seconds)).await;
        }
    }
    fn is_debt_free(&self) -> bool {
        let payment =
            (self.last_update.elapsed().as_secs_f32() * self.bytes_per_second as f32) as u64;
        payment >= self.debt
    }
    fn incur_debt(&mut self, new_debt: u64) {
        let now = tokio::time::Instant::now();
        let elapsed = now.duration_since(self.last_update);
        let payment = (elapsed.as_secs_f32() * self.bytes_per_second as f32) as u64;
        self.debt = self.debt.saturating_sub(payment) + new_debt;
        self.last_update = now;
    }
}

pub struct ExecutionContext<T> {
    /// Empty if not in use.
    /// We assume any real packet will be at least 1 byte
    cursend: Vec<u8>,
    cursend_is_retransmit:bool,
    send_local_addr: Option<T>,
    next_retransmit: Instant,
    next_retransmit_active: bool,
}

impl<Socket: CommunicationDriver> MulticastSenderLoop<Socket> {
    async fn new(
        driver: &mut Socket,
        bind_address: &str,
        multicast_group: &str,
        //message_tx: Sender<(Address, Vec<u8>)>,
        //message_rx: Receiver<Vec<u8>>,
        bandwidth_bytes_per_second: u64,
        quit_rx: tokio::sync::oneshot::Receiver<()>,
        mtu: usize,
        retransmit_interval: Duration,
        retransmit_buffer_size_bytes: usize,
        disable_retransmit: bool,
        ephemeral_node_id: ArcShift<EphemeralNodeId>,
        retransmit_responsibility_query: Box<
            dyn FnMut(/*src: */ EphemeralNodeId) -> Duration + 'static + Send + Sync,
        >,
    ) -> Result<MulticastSenderLoop<Socket>> {
        let (send_socket, receive_socket) = driver
            .initialize(bind_address, multicast_group, mtu)
            .await?;
        let max_payload_per_packet = mtu.saturating_sub(APPROX_HEADER_SIZE);
        debug!("Send socket bind-address: {:?}", send_socket.local_addr()?);

        if max_payload_per_packet < 100 {
            bail!("Unreasonably small MTU specified: {}", mtu);
        }

        let raw_packets_received = counter!("raw_packets_received");
        describe_counter!(
            "raw_packets_received",
            Unit::Count,
            "Number of raw network packets received"
        );
        let valid_packets_received = counter!("valid_packets_received");
        describe_counter!(
            "valid_packets_received",
            Unit::Count,
            "Number of valid network packets received"
        );
        let packets_sent = counter!("packets_sent");
        describe_counter!(
            "packets_sent",
            Unit::Count,
            "Number of network packets sent"
        );
        let retransmit_request_count = counter!("retransmit_request_count");
        describe_counter!(
            "retransmit_request_count",
            Unit::Count,
            "Number of retransmit-requests sent"
        );

        Ok(Self {
            quit_rx,
            bind_address: send_socket.local_addr()?,
            send_socket,
            receive_socket,
            history: SizeLimitVecDeque::new(retransmit_buffer_size_bytes),
            queue: Default::default(),
            disable_retransmit,
            outgoing_retransmit_requests: Default::default(),
            receive_track: Default::default(),
            limiter: BwLimiter::new(bandwidth_bytes_per_second),
            //message_rx,
            //message_tx,
            last_send: Instant::now(),
            last_send_size: 0,
            recvbuf: Vec::with_capacity(mtu),
            max_payload_per_packet,
            next_send_seq: 0,
            retransmit_interval,
            ephemeral_node_id,
            retransmit_responsibility_query,
            hasher: Default::default(),
            sent_packets: Default::default(),
            gc_time: Instant::now(),
            raw_packets_received,
            valid_packets_received,
            packets_sent,
            retransmit_request_count,
        })
    }
    pub(crate) fn send_buf(
        queue: &mut VecDeque<TransmittedEntityWithFullSeq>,
        max_payload_per_packet: usize,
        next_send_seq: &mut u64,
        buffer: Vec<u8>,
        own_id: EphemeralNodeId,
    ) -> Result<()> {
        let mut is_first;
        let buffer: &[u8] = if let Some(last) = queue.back_mut() {
            if last.entity.first_boundary != u16::MAX {
                if last.entity.free(max_payload_per_packet) >= 2 + buffer.len() {
                    last.entity
                        .data
                        .write_u16::<LittleEndian>(buffer.len().try_into().unwrap())?;
                    last.entity.data.extend(buffer);
                    return Ok(());
                }
                last.entity.data.write_u16::<LittleEndian>(u16::MAX)?;
                let free_now = last.entity.free(max_payload_per_packet);
                last.entity.data.extend(&buffer[0..free_now]);
                is_first = false;
                &buffer[free_now..]
            } else {
                is_first = true;
                &buffer
            }
        } else {
            is_first = true;
            &buffer
        };

        let mut reader_pos = 0;
        loop {
            let remaining = buffer.len() - reader_pos;
            if remaining == 0 {
                break;
            }
            let overhead = if is_first { 2 } else { 0 };
            let max_payload_this_packet = max_payload_per_packet - overhead;
            let chunk = remaining.min(max_payload_this_packet);
            let mut data = Vec::with_capacity(chunk + overhead);
            if is_first {
                data.write_u16::<LittleEndian>(if buffer.len() <= max_payload_this_packet {
                    buffer.len().try_into().unwrap()
                } else {
                    u16::MAX
                })
                .unwrap();
            }
            data.write_all(&buffer[reader_pos..reader_pos + chunk])?;
            reader_pos += chunk;
            queue.push_back(TransmittedEntityWithFullSeq {
                reconstructed_seq: *next_send_seq,
                entity: TransmittedEntitySortable {
                    seq: *next_send_seq as u16,
                    data,
                    first_boundary: if is_first { 0 } else { u16::MAX },
                    src: own_id,
                },
            });
            is_first = false;
            *next_send_seq += 1;
        }
        if let Some(last) = queue.back_mut() {
            if last.entity.first_boundary == u16::MAX {
                last.entity.first_boundary = last.entity.data.len().try_into().unwrap();
            }
        }
        Ok(())
    }
    pub(crate) fn queue_retransmits(&mut self, what: &[u64]) {
        for what in what {
            let Ok(index) = self
                .history
                .binary_search_by_key(what, |x| x.reconstructed_seq)
            else {
                return;
            };
            let Some(history_item) = self.history.remove(index) else {
                warn!("history did not contain {:?}", what);
                return;
            };
            trace!("enqueued {:?} from history", what);
            let (Ok(insert_point) | Err(insert_point)) = self
                .queue
                .binary_search_by_key(what, |x| x.reconstructed_seq);

            self.queue.insert(insert_point, history_item);
        }
    }

    pub fn create_context(&self) -> Result<ExecutionContext<Socket::Endpoint>> {
        Ok(ExecutionContext {
            cursend: vec![],
            cursend_is_retransmit: false,
            send_local_addr: self.send_socket.local_addr()?,
            next_retransmit: Instant::now(),
            next_retransmit_active: false,
        })
    }

    #[instrument(skip(self, context),fields(local=?self.bind_address, diagnostics))]
    pub async fn run(
        &mut self,
        context: &mut ExecutionContext<Socket::Endpoint>,
        messages_received_new_buffer: &mut Vec<(Option<Address>, EphemeralNodeId,  Vec<u8>)>,
        messages_transmit_new_buffer: &mut Vec<Vec<u8>>,
        node_id_collision_detected: &mut bool,
        mut diagnostics: Option<&Mutex<DiagnosticsData>>,
    ) -> Result<bool /*quit*/> {

        self.recvbuf.clear();
        let receive = self.receive_socket.recv_buf_from(&mut self.recvbuf);

        for buf in messages_transmit_new_buffer.drain(..) {
            Self::send_buf(
                &mut self.queue,
                self.max_payload_per_packet,
                &mut self.next_send_seq,
                buf,
                *self.ephemeral_node_id.get(),
            )?;
        }

        let now = Instant::now();
        if self.gc_time < now {
            self.receive_track
                .retain(|_k, v| now.duration_since(v.last_success).as_secs() < 300);
            self.gc_time = now + Duration::from_secs(60);
        }

        if context.cursend.is_empty() {
            if self.outgoing_retransmit_requests.is_empty() == false {
                let first_key = *self
                    .outgoing_retransmit_requests
                    .iter()
                    .min_by_key(|(_k, v)| v.wait_until)
                    .unwrap()
                    .0;
                let first_val = self
                    .outgoing_retransmit_requests
                    .get_mut(&first_key)
                    .unwrap();
                if first_val.wait_until <= Some(now) {
                    let mut what = Vec::with_capacity(20);
                    while what.len() < 20 {
                        let Some(item) = first_val.items.pop_first() else {
                            break;
                        };
                        what.push(item);
                    }
                    let packet = NetworkPacket::RetransmitRequest {
                        who: first_key,
                        what,
                    };


                    Serializer::bare_serialize(&mut context.cursend, 0, &packet).unwrap();

                    if let Some(diagnostics) = diagnostics {
                        let mut diagnostics = diagnostics.lock().unwrap();
                        diagnostics.sent_packets.push_back(PacketRow {
                            time: Instant::now(),
                            packet: format!("{:?}", packet),
                            size: context.cursend.len(),
                        });
                        if diagnostics.sent_packets.len() > 10 {
                            diagnostics.sent_packets.pop_front();
                        }
                    }

                    context.cursend_is_retransmit = true;
                    trace!(
                        "Sending raw retransmit {:?} ({} bytes)",
                        packet,
                        context.cursend.len()
                    );

                    if first_val.items.is_empty() {
                        self.outgoing_retransmit_requests.swap_remove(&first_key);
                    }
                } else {
                    // This if is probably dead code, since we only get here if wait_until > Some(_),
                    // which actually implies wait_until != None
                    if let Some(wait) = first_val.wait_until {
                        trace!(
                            "Scheduling raw retransmit in {:?}",
                            wait.duration_since(Instant::now())
                        );
                        context.next_retransmit = wait;
                        context.next_retransmit_active = true;
                    }
                }
            }
            if context.cursend.is_empty() {
                if let Some(x) = self.queue.pop_front() {
                    // Consider if savefile really is the best here. Some more space efficiency
                    // wouldn't hurt!
                    let packet = NetworkPacket::Data(x.entity.clone());
                    Serializer::bare_serialize(
                        &mut context.cursend,
                        0,
                        &packet,
                    )
                    .unwrap();

                    if let Some(diagnostics) = diagnostics.as_mut() {
                        let mut diagnostics = diagnostics.lock().unwrap();
                        diagnostics.sent_packets.push_back(PacketRow {
                            time: Instant::now(),
                            packet: format!("{:?}", packet),
                            size: context.cursend.len(),
                        });
                        if diagnostics.sent_packets.len() > 10 {
                            diagnostics.sent_packets.pop_front();
                        }
                    }

                    context.cursend_is_retransmit = false;

                    self.hasher.reset();
                    self.hasher.update(&context.cursend);
                    self.sent_packets.insert(self.hasher.digest128());
                    if self.sent_packets.len() >= 1000 {
                        self.sent_packets.drain(0..500);
                    }

                    let (Ok(insert_point) | Err(insert_point)) = self
                        .history
                        .binary_search_by_key(&x.reconstructed_seq, |x| x.reconstructed_seq);
                    self.history.insert(insert_point, x);
                };
            }
        }

        let send = async {
            if context.cursend.is_empty() == false {
                let send_size = context.cursend.len();
                self.limiter.wait_debt_free().await;

                match self.send_socket.send_to(&context.cursend).await {
                    Ok(()) => {
                        if context.cursend_is_retransmit {
                            self.retransmit_request_count.increment(1);
                        }
                        self.packets_sent.increment(1);
                        self.limiter.incur_debt(send_size as u64);

                        //trace!("Actually sent {} raw bytes", sent);
                        context.cursend.clear();
                    }
                    Err(err) => {
                        // Apply rate limit even on failure
                        self.limiter.incur_debt(send_size as u64);
                        context.cursend.clear();
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        error!("Send error: {:?}", err);
                    }
                }
            } else {
                std::future::pending().await
            }
        };

        //let get_cmd = self.message_rx.recv();
        select! {
            biased;
            _ = &mut self.quit_rx => {
                info!("quit_rx signalled, background task shutting down");
                return Ok(true);
            }
            _ = send => {
                Ok(false)
            }
            msg = receive => {
                //TODO: Should we perhaps not `expect` here?
                let (size, src_addr) = msg.expect("network should not fail");
                self.raw_packets_received.increment(1);

                assert_eq!(size, self.recvbuf.len());
                let Ok(packet): Result<NetworkPacket,_> = Deserializer::bare_deserialize(&mut Cursor::new(&self.recvbuf),0) else {
                    error!("Invalid packet received");
                    return Ok(false);
                };


                if let NetworkPacket::Data(TransmittedEntitySortable{src,..}) = &packet {
                    if *src  == *self.ephemeral_node_id.get() {
                        // We received a message from a node with the same node-id as we do.
                        // This could be a node id collision. But first, let's heuristically
                        // determine if we've simply overheard one of our own packets.

                        self.hasher.reset();
                        self.hasher.update(&self.recvbuf);
                        let hash = self.hasher.digest128();
                        if self.sent_packets.contains(&hash) {
                            // We sent this ourselves, don't deliver it.
                            return Ok(false);
                        } else {
                            *node_id_collision_detected = true;
                        }
                    }
                }
                self.valid_packets_received.increment(1);

                if let Some(diagnostics) = diagnostics {
                    let mut diagnostics = diagnostics.lock().unwrap();
                    diagnostics.received_packets.push_back(PacketRow {
                        time: Instant::now(),
                        packet: format!("{:?}", packet),
                        size: self.recvbuf.len(),
                    });
                    if diagnostics.received_packets.len() > 10 {
                        diagnostics.received_packets.pop_front();
                    }

                }




                match packet {
                    NetworkPacket::Data(entity) => {
                        let src_node = entity.src;

                        let (track, new_track) = match self.receive_track.entry((src_node, src_addr)) {
                            Entry::Occupied(e) => {
                                (e.into_mut(), false)
                            }
                            Entry::Vacant(e) => {
                                (e.insert(ReceiveTrack::new(self.retransmit_interval, self.disable_retransmit)), true)
                            }
                        };

                        match track.process(entity, src_addr.map(|x|Address::from(x)), src_node, messages_received_new_buffer, &mut self.outgoing_retransmit_requests, &mut self.retransmit_responsibility_query, new_track).await {
                            Ok(()) => {
                                Ok(false)
                            }
                            Err(err) => {
                                error!("Receive error: {:?}", err);
                                return Ok(false);
                            }
                        }
                    }
                    NetworkPacket::RetransmitRequest{who, what  } => {
                        if who == *self.ephemeral_node_id.get() {
                            self.queue_retransmits(&what);
                        }
                        Ok(false)
                    }
                }
            }
            _ = tokio::time::sleep_until(context.next_retransmit), if context.next_retransmit_active => {
                context.next_retransmit_active = false;
                Ok(false)
            }
        }
    }
}

#[derive(Debug)]
pub struct DebugEvent {
    pub node: ArrayString<20>,
    pub time: Instant,
    pub msg: DebugEventMsg,
}

pub enum DebugEventMsg {
    Send(String /*msg*/),
    Receive(String /*msg*/),
}

impl DebugEvent {
    pub fn is_send_of(&self, receive: &DebugEvent) -> bool {
        match (&self.msg, &receive.msg) {
            (DebugEventMsg::Send(s), DebugEventMsg::Receive(r)) => s == r,
            (_s, _r) => false,
        }
    }
}

impl Debug for DebugEventMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DebugEventMsg::Send(s) => {
                write!(f, "{}({})", rgb("Send", 100, 225, 255), s)
            }
            DebugEventMsg::Receive(s) => {
                write!(f, "{}({})", rgb("Receive", 255, 200, 100), s)
            }
        }
    }
}

enum Cmd<MSG: Message> {
    AddMessage(Option<NoatunTime>, MSG, oneshot::Sender<Result<()>>),
    Quit(oneshot::Sender<()>),
    GetStatus(oneshot::Sender<Status>),
    InstallDebugEventLogger(
        Box<dyn FnMut(DebugEvent) + 'static + Send + Sync>,
        oneshot::Sender<()>,
    ),
    GetEphemeralNodeId(oneshot::Sender<EphemeralNodeId>),
}

struct FillInFrequency {
    who: Address,
}

struct SeenBy {
    who: Address,
    when: Instant,
}

struct DatabaseCommunicationLoop<MSG: Message + Send>
where
    Self: Send,
{
    database: Arc<Mutex<Database<MSG>>>,
    /// Join handle for MulticastSenderLoop
    //jh: tokio::task::JoinHandle<Result<()>>,
    //sender_tx: Sender<Vec<u8>>,
    //receiver_rx: Receiver<(Address, Vec<u8>)>,
    distributor: Distributor,
    cmd_rx: Receiver<Cmd<MSG>>,

    /// When the first item was put into the buffer
    buffer_life_start: Instant,
    next_periodic: tokio::time::Instant,
    buffered_incoming_messages: Vec<(Option<Address> /*src*/, EphemeralNodeId, DistributorMessage)>,
    nextsend: Vec<u8>,
    nextsend_obj: Option<DistributorMessage>,
    node: String, //Address as string
    debug_event_logger: Option<Box<dyn FnMut(DebugEvent) + 'static + Send + Sync>>,
    report_head_interval: Duration,
    diagnostics: Option<Arc<Mutex<DiagnosticsData>>>
}

impl<MSG: Message + Send> Drop for DatabaseCommunicationLoop<MSG>
where
    Self: Send,
{
    fn drop(&mut self) {
        trace!("Dropping com loop");
    }
}

pub struct DatabaseCommunicationConfig {
    pub listen_address: String,
    pub multicast_address: String,
    pub mtu: usize,
    pub bandwidth_limit_bytes_per_second: u64,
    pub retransmit_interval_seconds: f32,
    pub retransmit_buffer_size_bytes: usize,
    pub debug_logger: Option<Box<dyn FnMut(DebugEvent) + 'static + Send + Sync>>,
    pub periodic_message_interval: Duration,
    /// Specifying this is only useful for debugging. The ephemeral node id is chosen
    /// automatically, and is also automatically changed if conflicts are
    /// detected.
    pub initial_ephemeral_node_id: Option<EphemeralNodeId>,
    /// Disable the NACK-based low-level retransmit protocol.
    ///
    /// This is basically never recommended, but can possibly be useful for troubleshooting.
    pub disable_retransmit: bool,
    /// Enable collection of in-memory diagnostics logs.
    ///
    /// This in turn enables use of the ratatui feature, to render a TUI debug UI
    pub enable_diagnostics: bool,
}
const REPORT_HEAD_INTERVAL_DEFAULT: Duration = Duration::from_secs(5);

impl Default for DatabaseCommunicationConfig {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:0".to_string(),
            multicast_address: "230.230.230.230:7777".to_string(),
            mtu: 1000,
            bandwidth_limit_bytes_per_second: 1000,
            retransmit_interval_seconds: 1.0,
            retransmit_buffer_size_bytes: 10000000,
            debug_logger: None,
            periodic_message_interval: REPORT_HEAD_INTERVAL_DEFAULT,
            initial_ephemeral_node_id: None,
            disable_retransmit: false,
            enable_diagnostics: false,
        }
    }
}

#[allow(async_fn_in_trait)]
pub(crate) trait SenderLoopTrait<E> {
    fn make_context(&self) -> Result<ExecutionContext<E>>;
    fn has_send_capacity(&self, context: &mut ExecutionContext<E>) -> bool;
    async fn pump(
        &mut self,
        context: &mut ExecutionContext<E>,
        messages_received: &mut Vec<(Option<Address>, EphemeralNodeId, Vec<u8>)>,
        messages_to_transmit_rx: &mut Vec<Vec<u8>>,
        node_id_collision_detected: &mut bool,
        inspector: Option<&Mutex<DiagnosticsData>>,
    ) -> Result<bool /*quit*/>;
}

impl<MSG: Message + 'static + Send> DatabaseCommunicationLoop<MSG> {
    fn process_packet(&mut self, src_addr: Option<Address>, src: EphemeralNodeId, packet: Vec<u8>) -> Result<()> {
        let msg: DistributorMessage = Deserializer::bare_deserialize(&mut Cursor::new(&packet), 0)?;
        trace!("Received DistributorMessage {:?}", msg);
        self.buffered_incoming_messages.push((src_addr, src, msg));
        Ok(())
    }
    fn process_messages(&mut self, now: tokio::time::Instant) -> Result<()> {
        if self.buffered_incoming_messages.is_empty() {
            return Ok(());
        }
        let mut database = self.database.lock().unwrap();
        if self.debug_event_logger.is_some() || self.diagnostics.is_some() {
            for msg in self.buffered_incoming_messages.iter() {

                if let Some(diagnostics) = self.diagnostics.as_ref() {
                    let mut diagnostics = diagnostics.lock().unwrap();
                    diagnostics.received_messages.push_back(
                        MessageRow {
                            time: Instant::now(),
                            //TODO: Better format here
                            message: msg.2.debug_format::<MSG>()?,
                            from: msg.1,
                            src_addr: msg.0
                        }
                    );
                    if diagnostics.received_messages.len() > 10 {
                        diagnostics.received_messages.pop_front();
                    }
                }

                if let Some(dbg) = &mut self.debug_event_logger {
                    dbg(DebugEvent {
                        node: ArrayString::from(&self.node).unwrap_or_else(|_| Default::default()),
                        time: Instant::now(),
                        msg: DebugEventMsg::Receive(msg.2.debug_format::<MSG>()?),
                    });
                }
            }
        }

        self.distributor.receive_message(
            &mut *database,
            self.buffered_incoming_messages.drain(..),
            now,
        )?;
        drop(database);
        Ok(())
    }

    fn debug_record_sent(&mut self, msg: &DistributorMessage) -> Result<()> {
        if let Some(data) = &mut self.diagnostics {
            let mut data = data.lock().unwrap();
            data.sent_messages.push_back(MessageRow {
                time: Instant::now(),
                //TODO: Add "ratatui_format" method, with ratatui coloring!
                message: msg.debug_format::<MSG>()?,
                from: *self.distributor.ephemeral_node_id.get(),
                src_addr: None,
            });
            if data.sent_messages.len() > 10 {
                data.sent_messages.pop_front();
            }

        }
        if let Some(dbg) = &mut self.debug_event_logger {
            dbg(DebugEvent {
                node: ArrayString::from(&self.node).unwrap_or_else(|_| Default::default()),
                time: Instant::now(),
                msg: DebugEventMsg::Send(msg.debug_format::<MSG>()?),
            });
        }
        Ok(())
    }

    pub(crate) async fn run<E>(
        self,
        senderloop_quit_tx: oneshot::Sender<()>,
        sender: &mut impl SenderLoopTrait<E>,
    ) -> Result<()> {
        // Run the actual loop
        let result = self.run_loop(sender).await;

        // Shutting down
        _ = senderloop_quit_tx.send(());
        info!("Communication terminated: {:?}", result);
        match result {
            Ok(Some(sender)) => {
                sender.send(()).unwrap();
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        }
    }

    #[instrument(skip(self, sender), fields(node=?self.node))]
    pub(crate) async fn run_loop<E>(
        mut self,
        sender: &mut impl SenderLoopTrait<E>,
    ) -> Result<Option<tokio::sync::oneshot::Sender<()>>> {
        self.nextsend.clear();
        let mut buffer_timer_instant = None;
        let mut context = sender.make_context()?;
        let mut messages_received: Vec<(Option<Address>, EphemeralNodeId, Vec<u8>)> = Vec::new();
        let mut message_to_transmit: Vec<Vec<u8>> = Vec::new();
        loop {
            track_node!(self.distributor.ephemeral_node_id.get().raw_u16());

            for message in messages_received.drain(..) {
                if let Err(err) = self.process_packet(message.0,message.1, message.2.clone()) {
                    error!("Error processing incoming packet: {:?}", err);
                }
            }

            // For buffered incoming messages
            let buffer_len = self.buffered_incoming_messages.len();
            let buffer_life_start = self.buffer_life_start;

            let buffering_timer = async move {
                if buffer_len > 0 {
                    if buffer_len > 1000 || buffer_life_start.elapsed() > Duration::from_secs(1) {
                    } else {
                        info!("Sleeping 10ms, waiting for buffer to fill");
                        let sleep_target = buffer_timer_instant
                            .get_or_insert_with(|| Instant::now() + Duration::from_millis(10));
                        tokio::time::sleep_until(*sleep_target).await;
                    }
                } else {
                    std::future::pending().await
                }
            };

            if self.nextsend.is_empty() && !self.distributor.outbuf.is_empty() {
                let mut msg = self.distributor.outbuf.pop_front().unwrap();

                let mut inhibit_send = false;
                if let DistributorMessage::Message { message: msg, .. } = &mut msg {
                    let mut db = self
                        .database
                        .lock()
                        .map_err(|e| anyhow!("mutex lock failed: {:?}", e))?;
                    let mut sess = db.begin_session_mut()?;
                    if !sess.mark_transmitted(msg.message_id())? {
                        inhibit_send = true;
                    }
                    msg.retain_parents(|message_id|{
                        sess.contains_message(message_id).unwrap_or(false)
                    });
                }
                if !inhibit_send {
                    let result = Serializer::bare_serialize(&mut self.nextsend, 0, &msg);
                    if result.is_err() {
                        self.nextsend.clear();
                    }
                    result?;
                    if self.debug_event_logger.is_some() || self.diagnostics.is_some() {
                        self.nextsend_obj = Some(msg);
                    }
                }
            }

            if !self.nextsend.is_empty() && sender.has_send_capacity(&mut context) {
                //let permit = self.sender_tx.reserve().await?;
                if !self.nextsend.is_empty() {
                    if let Some(msg) = self.nextsend_obj.take() {
                        self.debug_record_sent(&msg)?;
                    }
                    message_to_transmit.push(std::mem::take(&mut self.nextsend));
                }
                //permit.send(std::mem::take(&mut self.nextsend));
            }

            let mut node_id_collision = false;

            select!(
                biased;
                _ = sender.pump(&mut context, &mut messages_received, &mut message_to_transmit, &mut node_id_collision, self.diagnostics.as_ref().map(|x|&**x)) => {

                }
                /*res = sendtask => {
                    res?;
                }*/
                _process_incoming = buffering_timer => {
                    track_node!(self.distributor.ephemeral_node_id.get().raw_u16());
                    buffer_timer_instant = None;
                    self.process_messages(Instant::now())?;
                }
                _periodic = tokio::time::sleep_until(self.next_periodic) => {
                    track_node!(self.distributor.ephemeral_node_id.get().raw_u16());
                    let database = self.database.lock().unwrap();
                    let session = database.begin_session()?;
                    let msgs = self.distributor.get_periodic_message(&session, Instant::now().into())?;
                    trace!("Time for periodic messages: {:?}", msgs);
                    self.distributor.outbuf.extend(msgs);
                    self.next_periodic += self.report_head_interval;
                }
                cmd = self.cmd_rx.recv() => {
                    track_node!(self.distributor.ephemeral_node_id.get().raw_u16());
                    let Some(cmd) = cmd else {

                        info!("cmd rx, sender is gone");
                        return Ok(None); //Done
                    };
                    match cmd {
                        Cmd::InstallDebugEventLogger(debug_event_logger, oneshot) =>  {
                            self.debug_event_logger = Some(debug_event_logger);
                            _ = oneshot.send(());
                        }
                        Cmd::Quit(sender) => {
                            info!("cmd rx received quit");
                            return Ok(Some(sender));
                        }
                        Cmd::GetStatus(sender) => {
                            //let db = self.database.lock().unwrap();
                            //let now = db.noatun_now();
                            sender.send(self.distributor.get_status(Instant::now())).map_err(|_|anyhow!("oneshot sender failed"))?;
                        }
                        Cmd::AddMessage(time, msg,result) => {

                            let mut database = self.database.lock().unwrap();
                            let mut sess = database.begin_session_mut()?;
                            let msg = sess.create_message_frame(time, msg)
                                .and_then(|msg|{
                                    sess.append_single(&msg, true)?;
                                    Ok(msg)
                                });

                            match msg {
                                Ok(msg) => {
                                    let my_node_id = self.distributor.node_id();
                                    self.distributor.outbuf.push_back(
                                        DistributorMessage::Message{
                                            source: my_node_id,
                                            message:SerializedMessage::new(msg)?,
                                            demand_ack: false,
                                            origin: my_node_id,
                                            explicit_retransmit: false
                                    });
                                    _ = result.send(Ok(()));
                                }
                                Err(err) => {
                                    _ = result.send(Err(err));
                                }
                            }
                        }
                        Cmd::GetEphemeralNodeId(sender) => {
                            _ = sender.send(*self.distributor.ephemeral_node_id.get());
                        }
                    }
                }

            );

            if node_id_collision {
                self.distributor.report_node_id_collision();
            }
        }
    }
}

pub struct DatabaseCommunication<MSG: Message> {
    database: Arc<Mutex<Database<MSG>>>,
    cmd_tx: Sender<Cmd<MSG>>,
    node: String,
    initial_node_id: EphemeralNodeId,
    diagnostics: Option<Arc<Mutex<DiagnosticsData>>>,
}

impl<MSG: Message + 'static + Send> DatabaseCommunication<MSG> {
    /// This is just available for debugging
    pub async fn ephemeral_node_id(&self) -> Result<EphemeralNodeId> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let send_result = self.cmd_tx.send(Cmd::GetEphemeralNodeId(oneshot_tx)).await;
        match send_result {
            Ok(()) => Ok(oneshot_rx.await?),
            Err(err) => {
                bail!("Failed to send command to background thread {:?}", err);
            }
        }
    }

    pub fn inspector_data(&self) -> Option<DiagnosticsData> {
        Some(self.diagnostics.as_ref()?.lock().unwrap().clone())
    }

    pub async fn get_status(&self) -> Result<Status> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let status = self.cmd_tx.send(Cmd::GetStatus(oneshot_tx)).await;
        match status {
            Ok(()) => {
                let result: Status = oneshot_rx.await?;
                Ok(result)
            }
            Err(err) => {
                bail!("Failed to send command to background thread {:?}", err);
            }
        }
    }

    pub fn get_status_blocking(&self) -> Result<Status> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let status = self.cmd_tx.blocking_send(Cmd::GetStatus(oneshot_tx));
        match status {
            Ok(()) => {
                let result: Status = oneshot_rx.blocking_recv()?;
                Ok(result)
            }
            Err(err) => {
                bail!("Failed to send command to background thread {:?}", err);
            }
        }
    }

    /// Install a callback that will receive events describing the operation of the
    /// communication. This is only meant for debugging/logging.
    pub async fn install_debug_logger(
        &self,
        f: impl FnMut(DebugEvent) + 'static + Send + Sync,
    ) -> Result<()> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let status = self
            .cmd_tx
            .send(Cmd::InstallDebugEventLogger(Box::new(f), oneshot_tx))
            .await;
        match status {
            Ok(()) => {
                oneshot_rx.await?;
                Ok(())
            }
            Err(err) => {
                bail!("Failed to send command to background thread {:?}", err);
            }
        }
    }

    /// Return a reference to the inner noatun database.
    ///
    /// Warning! Adding or deleting messages directly to the inner database is not advised. Such
    /// messages will not be immediately picked up by the communication, causing inefficiency.
    /// Instead, use [`Self::add_message`] and similar methods on [`DatabaseCommunication`].
    pub fn inner_database(&self) -> MutexGuard<'_, Database<MSG>> {
        self.database.lock().unwrap()
    }

    pub async fn close(self) -> Result<()> {
        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        debug!("Sending quit");
        let e = self.cmd_tx.send(Cmd::Quit(oneshot_tx)).await;
        debug!("Quit sent!");
        match e {
            Ok(_) => {}
            Err(_err) => {
                bail!("Instance already closed");
            }
        }
        match tokio::time::timeout(Duration::from_secs(1), oneshot_rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                bail!(
                    "Couldn't shut down DatabaseCommunication gracefully: {:?}",
                    err
                );
            }
            Err(Elapsed { .. }) => {
                bail!("Timeout - couldn't shut down DatabaseCommunication gracefully.");
            }
        }
    }

    pub async fn add_message(&self, msg: MSG) -> Result<()> {
        self.add_message_impl(None, msg).await
    }

    pub async fn add_message_at(&self, time: NoatunTime, msg: MSG) -> Result<()> {
        self.add_message_impl(Some(time), msg).await
    }
    async fn add_message_impl(&self, time: Option<NoatunTime>, msg: MSG) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        match self
            .cmd_tx
            .send(Cmd::AddMessage(time, msg, response_tx))
            .await
        {
            Ok(()) => {}
            Err(err) => {
                bail!(
                    "Failed to AddMessage - background thread no longer running: {:?}",
                    err
                );
            }
        }
        response_rx.await??;
        Ok(())
    }
    pub fn blocking_add_message_at(&self, time: NoatunTime, msg: MSG) -> Result<()> {
        self.blocking_add_message_at_opt(Some(time), msg)
    }
    pub fn blocking_add_message(&self, msg: MSG) -> Result<()> {
        self.blocking_add_message_at_opt(None, msg)
    }
    /// Must *not* be called from within a tokio runtime.
    pub fn blocking_add_message_at_opt(&self, time: Option<NoatunTime>, msg: MSG) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        match self
            .cmd_tx
            .blocking_send(Cmd::AddMessage(time, msg, response_tx))
        {
            Ok(()) => {}
            Err(err) => {
                bail!(
                    "Failed to AddMessage - background thread no longer running: {:?}",
                    err
                );
            }
        }
        response_rx.blocking_recv()??;
        Ok(())
    }
    pub fn with_root<R>(&self, f: impl FnOnce(&MSG::Root) -> R) -> R {
        let db = self.database.lock().unwrap();
        db.with_root(f)
    }

    pub fn count_messages(&self) -> usize {
        self.database.lock().unwrap().count_messages()
    }

    #[instrument(skip(self),fields(node=?self.node))]
    pub fn set_mock_time(&mut self, time: NoatunTime) -> Result<()> {
        let mut db = self.database.lock().unwrap();
        let mut sess = db.begin_session_mut()?;
        track_node!(self.initial_node_id.raw_u16());
        sess.set_mock_time(time)?;
        Ok(())
    }

    pub fn get_update_heads(&self) -> Result<Vec<crate::MessageId>> {
        let db = self.database.lock().unwrap();
        let sess = db.begin_session()?;
        Ok(sess.get_update_heads().to_vec())
    }
    pub fn get_cutoff_time(&self) -> Result<NoatunTime> {
        let db = self.database.lock().unwrap();
        let sess = db.begin_session()?;
        sess.current_cutoff_time()
    }
    pub fn with_root_preview<R>(
        &self,
        time: DateTime<Utc>,
        preview: impl Iterator<Item = MSG>,
        f: impl FnOnce(&MSG::Root) -> R,
    ) -> Result<R> {
        let mut db = self.database.lock().unwrap();
        let mut sess = db.begin_session_mut()?;
        sess.with_root_preview(time, preview, f)
    }

    pub fn set_projection_time_limit(&mut self, limit: NoatunTime) -> Result<()> {
        let mut db = self.database.lock().unwrap();
        let mut sess = db.begin_session_mut()?;
        sess.set_projection_time_limit(limit)
    }

    /// Spawns the communication system as a future on the current tokio runtime, using
    /// the communication driver `driver`. By implementing [`CommunicationDriver`], you can
    /// use noatun with any kind of message based transport.
    pub async fn new_custom<Driver: CommunicationDriver + 'static>(
        driver: &mut Driver,
        database: Database<MSG>,
        config: DatabaseCommunicationConfig,
    ) -> Result<DatabaseCommunication<MSG>> {
        //let (sender_tx, sender_rx) = tokio::sync::mpsc::channel(1);
        let (quit_tx, quit_rx) = tokio::sync::oneshot::channel();
        //let (receiver_tx, receiver_rx) = tokio::sync::mpsc::channel(1000);
        let mut our_node_id = ArcShift::new(
            config
                .initial_ephemeral_node_id
                .unwrap_or_else(EphemeralNodeId::random),
        );

        let retransmit_interval = Duration::from_secs_f32(config.retransmit_interval_seconds);

        let mini_pather = Arc::new(RwLock::new(MiniPather::new(our_node_id.get().raw_u16())));
        let mini_pather2 = mini_pather.clone();

        let should_ask_for_retransmission: Box<
            dyn FnMut(EphemeralNodeId) -> Duration + Send + Sync,
        > = Box::new(move |peer| -> Duration {
            let Some(ordinal) = mini_pather2
                .write()
                .unwrap()
                .should_i_ask_for_retransmission(peer.raw_u16())
            else {
                // If MiniPather says straight up no, it's because it believes
                // the other node can't actually hear us. So there's no point in sending a request.
                // We impose a very long timeout in this case.
                return 10 * retransmit_interval;
            };
            (ordinal as u32) * retransmit_interval
        });
        let mut sender_loop = MulticastSenderLoop::new(
            driver,
            &config.listen_address,
            &config.multicast_address,
            //receiver_tx,
            //sender_rx,
            config.bandwidth_limit_bytes_per_second,
            quit_rx,
            config.mtu,
            retransmit_interval,
            config.retransmit_buffer_size_bytes,
            config.disable_retransmit,
            our_node_id.clone(),
            should_ask_for_retransmission,
        )
        .await?;

        let node = sender_loop
            .bind_address
            .map(|x| x.to_string())
            .unwrap_or("?".to_string());

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1000);

        let database = Arc::new(Mutex::new(database));

        let initial_node_id = *our_node_id.get();

        let diagnostics = config.enable_diagnostics.then_some(
            Arc::new(Mutex::new(DiagnosticsData::default()))
        );

        let main = DatabaseCommunicationLoop {
            distributor: Distributor::new(
                config.periodic_message_interval,
                our_node_id,
                Instant::now().into(),
                Some(mini_pather),
            ),
            node: node.clone(),
            database: database.clone(),
            //sender_tx,
            //receiver_rx,
            cmd_rx,
            buffer_life_start: Instant::now(),
            next_periodic: tokio::time::Instant::now(),
            buffered_incoming_messages: vec![],
            nextsend: vec![],
            debug_event_logger: config.debug_logger,
            report_head_interval: config.periodic_message_interval,
            nextsend_obj: None,
            diagnostics: diagnostics.clone()
        };

        spawn(async move { main.run(quit_tx, &mut sender_loop).await });

        Ok(DatabaseCommunication {
            database,
            cmd_tx,
            node,
            initial_node_id,
            diagnostics
        })
    }

    fn start_async_runtime<Driver: CommunicationDriver + Sync + Send + 'static>(
        driver: &mut Driver,
        database: Database<MSG>,
        config: DatabaseCommunicationConfig,
    ) -> Result<(Runtime, DatabaseCommunication<MSG>)> {
        let runtime = Runtime::new()?;
        let com: DatabaseCommunication<MSG> =
            runtime.block_on(Self::new_custom(driver, database, config))?;
        Ok((runtime, com))
    }

    /// Create a new instance for communication over UDP multicast.
    pub fn new(
        database: Database<MSG>,
        config: DatabaseCommunicationConfig,
    ) -> Result<DatabaseCommunication<MSG>> {
        let (res_tx, res_rx) = tokio::sync::oneshot::channel();
        thread::spawn(move || {
            match Self::start_async_runtime(&mut TokioUdpDriver, database, config) {
                Ok((runtime, app)) => {
                    if let Err(_err) = res_tx.send(Ok(app)) {
                        error!("Failed to start communication, client closed result channel.");
                        return;
                    }
                    runtime.block_on(std::future::pending::<()>());
                    info!("Dropping runtime");
                }
                Err(err) => {
                    info!("start runtime failed: {:?}", err);
                    _ = res_tx.send(Err(err));
                }
            }
        });

        let app = res_rx.blocking_recv()??;
        Ok(app)
    }
}

#[cfg(test)]
mod tests {
    use crate::communication::udp::TokioUdpDriver;
    use crate::communication::{MulticastSenderLoop, ReceiveTrack};
    use crate::distributor::EphemeralNodeId;
    use crate::tests::setup_tracing;
    use arcshift::ArcShift;
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio::spawn;

    #[test]
    fn reconstruct_seq_logic() {
        assert_eq!(ReceiveTrack::reconstruct_seq_impl(0, 0), 0);
        assert_eq!(ReceiveTrack::reconstruct_seq_impl(1, 0), 0);
        assert_eq!(ReceiveTrack::reconstruct_seq_impl(0, 1), 1);
        assert_eq!(ReceiveTrack::reconstruct_seq_impl(1500, 0), 65536);

        assert_eq!(ReceiveTrack::reconstruct_seq_impl(10, 9), 9);
        assert_eq!(ReceiveTrack::reconstruct_seq_impl(10, 7), 7);
        assert_eq!(ReceiveTrack::reconstruct_seq_impl(65536, 0), 65536);
        assert_eq!(ReceiveTrack::reconstruct_seq_impl(65535, 1), 65537);
        assert_eq!(
            ReceiveTrack::reconstruct_seq_impl(65536 + 65535, 1),
            65536 + 65537
        );
    }

    #[tokio::test]
    async fn test_sender_ipv4() {
        setup_tracing();
        let (_quit_tx1, quit_rx1) = tokio::sync::oneshot::channel();

        let mloop1 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "127.0.0.1:0",
            "224.45.0.1:7777",
            10000,
            quit_rx1,
            200,
            Duration::from_secs_f32(1.0),
            1_000_000,
            false,
            ArcShift::new(EphemeralNodeId::new(1)),
            Box::new(|_| Duration::ZERO),
        )
        .await
        .unwrap();

        //let (sender_tx2, sender_rx2) = tokio::sync::mpsc::channel(1000);
        let (_quit_tx2, quit_rx2) = tokio::sync::oneshot::channel();
        //let (receiver_tx2, mut receiver_rx2) = tokio::sync::mpsc::channel(1000);

        let mloop2 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "127.0.0.1:0",
            "224.45.0.1:7777",
            10000,
            quit_rx2,
            200,
            Duration::from_secs_f32(1.0),
            1_000_000,
            false,
            ArcShift::new(EphemeralNodeId::new(2)),
            Box::new(|_| Duration::ZERO),
        )
        .await
        .unwrap();

        test_sending_various_payloads(mloop1, mloop2).await;
    }
    async fn test_sending_various_payloads(
        mut mloop1: MulticastSenderLoop<TokioUdpDriver>,
        mut mloop2: MulticastSenderLoop<TokioUdpDriver>,
    ) {
        let payloads = || {
            vec![
                vec![1u8; 1],
                vec![2u8; 10],
                vec![3u8; 250],
                vec![4u8; 1000],
                vec![5u8; 2000],
            ]
        };
        let task1 = async move {
            let mut to_send = payloads();
            let mut expect: HashSet<Vec<u8>> = payloads().into_iter().collect();
            let mut ctx = mloop1.create_context()?;
            let mut received = Vec::new();
            tokio::time::sleep(Duration::from_millis(100)).await;
            while mloop1
                .run(&mut ctx, &mut received, &mut to_send, &mut false, None)
                .await?
                == false
            {
                for (_addr, _srcnode, msg) in received.drain(..) {
                    println!("Task1 received {} byte msg", msg.len());
                    assert!(expect.remove(&msg));
                }
                if expect.is_empty() {
                    return Ok(());
                }
            }
            panic!("loop exited unexpectedly");
        };
        let task2 = async move {
            let mut to_send = payloads();
            let mut expect: HashSet<Vec<u8>> = payloads().into_iter().collect();
            let mut ctx = mloop2.create_context()?;
            let mut received = Vec::new();
            tokio::time::sleep(Duration::from_millis(100)).await;
            while mloop2
                .run(&mut ctx, &mut received, &mut to_send, &mut false, None)
                .await?
                == false
            {
                for (_addr, _srcnode, msg) in received.drain(..) {
                    println!("Task2 received {} byte msg", msg.len());
                    assert!(expect.remove(&msg));
                }
                if expect.is_empty() {
                    return Ok(());
                }
            }
            panic!("loop exited unexpectedly");
        };

        let jh1: tokio::task::JoinHandle<anyhow::Result<()>> = spawn(task1);
        let jh2: tokio::task::JoinHandle<anyhow::Result<()>> = spawn(task2);

        tokio::time::timeout(Duration::from_secs(10), jh1)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), jh2)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sender_ipv6() {
        setup_tracing();
        let (_quit_tx1, quit_rx1) = tokio::sync::oneshot::channel();

        let mloop1 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "[::]:0",
            "[ff02::42:41%2]:7775",
            //receiver_tx1,
            //sender_rx1,
            10000,
            quit_rx1,
            200,
            Duration::from_secs_f32(1.0),
            100,
            false,
            ArcShift::new(EphemeralNodeId::new(1)),
            Box::new(|_| Duration::ZERO),
        )
        .await
        .unwrap();

        let (_quit_tx2, quit_rx2) = tokio::sync::oneshot::channel();

        let mloop2 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "[::]:0",
            "[ff02::42:41%2]:7775",
            10000,
            quit_rx2,
            200,
            Duration::from_secs_f32(1.0),
            100,
            false,
            ArcShift::new(EphemeralNodeId::new(2)),
            Box::new(|_| Duration::ZERO),
        )
        .await
        .unwrap();

        test_sending_various_payloads(mloop1, mloop2).await;
    }
}
