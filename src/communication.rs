use crate::distributor::{Address, Distributor, DistributorMessage, EphemeralNodeId, SerializedMessage, Status};

use crate::colors::{rgb};
use crate::communication::size_limit_vec_deque::{MeasurableSize, SizeLimitVecDeque};
use crate::communication::udp::TokioUdpDriver;
use crate::{Application, Database, MessageId, NoatunTime};
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

use std::collections::{VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::{io, thread};
use std::cmp::Reverse;
use indexmap::map::Entry;
use smallvec::SmallVec;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::error::Elapsed;
use tokio::time::Instant;
use tokio::{select, spawn};
use tracing::{debug, error, info, instrument, trace, warn};

pub mod size_limit_vec_deque;
pub mod udp;

#[derive(Savefile, Debug)]
pub(crate) enum NetworkPacket<Endpoint> {
    Data(bool /*push*/, TransmittedEntitySortable),
    RetransmitRequest { who: Endpoint, what: Vec<u64> },
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
    ///  * In particular, no node on the network should appear to have our address.
    ///  * If loopback occurs (we see the packets we ourselves end), the source address
    ///    should always be `Self::own_address`.
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
    type Endpoint: Eq
        + Debug
        + Hash
        + Serialize
        + Deserialize
        + Packed
        + Send
        + Sync
        + Copy
        + Display;
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
    fn recv_buf_from<B: bytes::buf::BufMut + Send>(
        &mut self,
        buf: &mut B,
    ) -> impl std::future::Future<Output = std::io::Result<(usize, Endpoint)>> + Send;
}
#[allow(async_fn_in_trait)] //For now
pub trait CommunicationSendSocket<Endpoint: PartialEq + Debug + Send> {
    fn local_addr(&self) -> Result<Endpoint>;
    fn send_to(
        &mut self,
        buf: &[u8],
    ) -> impl std::future::Future<Output = io::Result<usize>> + Send;
}


#[derive(Debug, Clone)]
struct TransmittedEntitySortable {
    seq: u16,
    data: Vec<u8>,
    // Note, u16::MAX signifies "no boundary"
    first_boundary: u16,
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
        let datalen = deserializer.read_u16()?;
        let data = deserializer.read_bytes(datalen as usize)?;
        let first_boundary = deserializer.read_u16()?;
        Ok(Self {
            seq,
            data,
            first_boundary,
        })
    }
}

impl TransmittedEntitySortable {
    fn free(&self, max_payload: usize) -> usize {
        max_payload.saturating_sub(self.data.len())
    }
}

#[derive(Debug)]
struct SortableTransmittedEntity {
    reconstructed_seq: u64,
    entity: TransmittedEntitySortable,
}

impl MeasurableSize for SortableTransmittedEntity {
    fn size_bytes(&self) -> usize {
        self.entity.data.len() + size_of_val(&self.reconstructed_seq) + size_of_val(&self.entity)
    }
}

struct ReceiveTrack {
    have_valid_accum: bool,
    accum: VecDeque<u8>,
    expected_next: u64,
    sorted_packets: VecDeque<SortableTransmittedEntity>,
    retransmit_interval: Duration,
    disable_retransmit: bool,
    retransmit_counter: usize,
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
    pub const SENDER_RETRANSMIT_WINDOW: usize = ReceiveTrack::RECEIVER_RETRANSMIT_WINDOW/5;
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
            retransmit_counter: 0,
            //TODO: Clean away tracks if last success is long far ago
            compile_error!("Maybe low-level retransmits should also use the neighbor info...")
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

    async fn process<Endpoint: Hash + Eq + Debug + Copy + Display>(
        &mut self,
        packet: TransmittedEntitySortable,
        packet_source: Endpoint,
        tx_finished_received_frame: &mut Sender<(Address, Vec<u8>)>,
        retransmit_requests: &mut IndexMap<Endpoint, RetransmitInfo>,
        push: bool,
    ) -> Result<()> {
        let reconstructed_seq = self.reconstruct_seq(packet.seq);

        let packet = SortableTransmittedEntity {
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

            if first.reconstructed_seq != self.expected_next {
                println!("Mismatch: disabled retransmit: {:?}, packet count: {}, last prog: {:?}", self.disable_retransmit, self.expected_next, self.retransmit_counter);
                if self.disable_retransmit || packet_count > Self::SENDER_RETRANSMIT_WINDOW || self.retransmit_counter > 3 {
                    self.accum.clear();
                    self.have_valid_accum = false;
                    self.expected_next = first.reconstructed_seq;
                    self.retransmit_counter = 0;
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
                    if self.expected_next == 0 {
                        //Make sure that after a restart, we always pickup wherever the on-wire stream is
                        self.expected_next = first.reconstructed_seq;
                    } else {
                        trace!(
                        "Appending retransmit-request for {:?} {:?}",
                        packet_source,
                        self.expected_next
                    );
                        //let cur_missing = self.expected_next;
                        let accum_retransmits = retransmit_requests.entry(packet_source).or_default();
                        for x in self.expected_next..first.reconstructed_seq {
                            accum_retransmits.items.insert(x);
                        }
                        if !push {
                            println!("Enqueue retransmit in {:?}", self.retransmit_interval);
                            accum_retransmits.wait_until = Instant::now() + self.retransmit_interval;
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

                        self.retransmit_counter += 1;

                        return Ok(());
                    }
                }
            }

            self.retransmit_counter = 0;
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
                    tx_finished_received_frame
                        .try_send((
                            Address::from(packet_source),
                            self.accum.iter().copied().collect(),
                        ))
                        .unwrap();
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
                        tx_finished_received_frame
                            .try_send((Address::from(packet_source), temp))
                            .unwrap();
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
    wait_until: Instant,
    items: IndexSet<u64>,
}
impl Default for RetransmitInfo {
    fn default() -> Self {
        Self {
            wait_until: tokio::time::Instant::now(),
            items: Default::default(),
        }
    }
}

struct MulticastSenderLoop<Socket: CommunicationDriver> {
    send_socket: Socket::Sender,
    receive_socket: Socket::Receiver,
    bind_address: Socket::Endpoint,
    /// Transmitted messages kept in out queue, to allow retransmitting
    history: SizeLimitVecDeque<SortableTransmittedEntity>,
    queue: VecDeque<SortableTransmittedEntity>,
    outgoing_retransmit_requests: IndexMap<Socket::Endpoint, RetransmitInfo>,
    receive_track: IndexMap<Socket::Endpoint, ReceiveTrack>,
    quit_rx: tokio::sync::oneshot::Receiver<()>,
    /// Sent to net
    message_rx: Receiver<Vec<u8>>,
    /// Received from net
    message_tx: Sender<(Address /*src*/, Vec<u8>)>,
    last_send: Instant,
    last_send_size: usize,
    recvbuf: Vec<u8>,
    max_payload_per_packet: usize,
    next_send_seq: u64,
    limiter: BwLimiter,
    /// The timeout before a retransmit is initiated
    retransmit_interval: Duration,
    disable_retransmit: bool,
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

impl<Socket: CommunicationDriver> MulticastSenderLoop<Socket> {
    //TODO: Too many arguments
    pub(crate) async fn new(
        driver: &mut Socket,
        bind_address: &str,
        multicast_group: &str,
        message_tx: Sender<(Address, Vec<u8>)>,
        message_rx: Receiver<Vec<u8>>,
        bandwidth_bytes_per_second: u64,
        quit_rx: tokio::sync::oneshot::Receiver<()>,
        mtu: usize,
        retransmit_interval: Duration,
        retransmit_buffer_size_bytes: usize,
        disable_retransmit: bool,
    ) -> Result<MulticastSenderLoop<Socket>> {
        let (send_socket, receive_socket) = driver
            .initialize(bind_address, multicast_group, mtu)
            .await?;
        let max_payload_per_packet = mtu.saturating_sub(APPROX_HEADER_SIZE);
        debug!("Send socket bind-address: {:?}", send_socket.local_addr()?);

        if max_payload_per_packet < 100 {
            bail!("Unreasonably small MTU specified: {}", mtu);
        }
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
            message_rx,
            message_tx,
            last_send: Instant::now(),
            last_send_size: 0,
            recvbuf: Vec::with_capacity(mtu),
            max_payload_per_packet,
            next_send_seq: 0,
            retransmit_interval,
        })
    }
    pub(crate) fn send_buf(
        queue: &mut VecDeque<SortableTransmittedEntity>,
        max_payload_per_packet: usize,
        next_send_seq: &mut u64,
        buffer: Vec<u8>,
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
            queue.push_back(SortableTransmittedEntity {
                reconstructed_seq: *next_send_seq,
                entity: TransmittedEntitySortable {
                    seq: *next_send_seq as u16,
                    data,
                    first_boundary: if is_first { 0 } else { u16::MAX },
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
    #[instrument(skip(self),fields(local=?self.bind_address))]
    pub async fn run(mut self) -> Result<()> {
        let mut cursend: Option<Vec<u8>> = None;
        let send_local_addr = self.send_socket.local_addr()?;
        loop {
            self.recvbuf.clear();
            let receive = self.receive_socket.recv_buf_from(&mut self.recvbuf);

            if cursend.is_none() {
                if self.outgoing_retransmit_requests.is_empty() == false {
                    let mut temp = vec![];
                    if let Some((first_key, first_val)) =
                        self.outgoing_retransmit_requests.iter_mut().next()
                    {
                        let packet = NetworkPacket::<Socket::Endpoint>::RetransmitRequest {
                            who: *first_key,
                            what: first_val.items.iter().take(20).copied().collect(), //If more than this are missing, we'll request them too in due time
                        };
                        Serializer::bare_serialize(&mut temp, 0, &packet).unwrap();
                        trace!("Sending raw retransmit {:?} ({} bytes)", packet, temp.len());
                        cursend = Some(temp);
                        first_val.items = first_val.items.iter().skip(20).copied().collect();
                        if first_val.items.is_empty() {
                            let first_key = *first_key;
                            self.outgoing_retransmit_requests.swap_remove(&first_key);
                        }
                    }
                }
                if cursend.is_none() {
                    let mut temp = vec![];
                    cursend = self.queue.pop_front().map(|x| {
                        // Consider if savefile really is the best here. Some more space efficiency
                        // wouldn't hurt!
                        Serializer::bare_serialize(
                            &mut temp,
                            0,
                            &NetworkPacket::<Socket::Endpoint>::Data(
                                self.queue.is_empty(),
                                x.entity.clone(),
                            ),
                        )
                        .unwrap();
                        let (Ok(insert_point) | Err(insert_point)) = self
                            .history
                            .binary_search_by_key(&x.reconstructed_seq, |x| x.reconstructed_seq);
                        self.history.insert(insert_point, x);

                        temp
                    });
                }
            }

            let send_queue_empty = cursend.is_none();

            let send = async {
                if let Some(tosend) = cursend.as_mut() {
                    let send_size = tosend.len();
                    self.limiter.wait_debt_free().await;

                    match self.send_socket.send_to(tosend).await {
                        Ok(sent) => {
                            if sent != send_size {
                                error!(
                                    "Packet send failure. Expected to send {}, sent {}",
                                    send_size, sent
                                );
                            }
                            self.limiter.incur_debt(send_size as u64);

                            //trace!("Actually sent {} raw bytes", sent);
                            cursend.take();
                        }
                        Err(err) => {
                            // Apply rate limit even on failure
                            self.limiter.incur_debt(send_size as u64);
                            cursend.take();
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                            error!("Send error: {:?}", err);
                        }
                    }
                } else {
                    std::future::pending().await
                }
            };
            let get_cmd = self.message_rx.recv();
            select! {
                    biased;
                    _ = &mut self.quit_rx => {
                        info!("quit_rx signalled, background task shutting down");
                        return Ok(());
                    }
                    _ = send => {
                    }
                    buf = get_cmd, if send_queue_empty => {
                        if let Some(buf) = buf {
                            Self::send_buf(
                                &mut self.queue,
                                self.max_payload_per_packet,
                                &mut self.next_send_seq,
                                buf
                            )?;
                        } else {
                            info!("cmd-channel gone, background task shutting down");
                            return Ok(());
                        }
                    }
                    msg = receive => {
                        let (size, src_addr) = msg.expect("network should not fail");
                        if src_addr == send_local_addr {
                            continue;
                        }

                        assert_eq!(size, self.recvbuf.len());
                        let Ok(packet): Result<NetworkPacket<Socket::Endpoint>,_> = Deserializer::bare_deserialize(&mut Cursor::new(&self.recvbuf),0)  else {
                            error!("Invalid packet received");
                            continue;
                        };
                        //trace!("Received packet {:?}", packet);
                        match packet {
                            NetworkPacket::Data(push, entity) => {

                                match self.receive_track.entry(src_addr).or_insert_with(||ReceiveTrack::new(self.retransmit_interval, self.disable_retransmit))
                                    .process(entity, src_addr, &mut self.message_tx, &mut self.outgoing_retransmit_requests, push).await {
                                        Ok(()) => {}
                                        Err(err) => {
                                            error!("Receive error: {:?}", err);
                                    }
                                }
                            }
                            NetworkPacket::RetransmitRequest{who, what  } => {
                                if who == self.bind_address {
                                    self.queue_retransmits(&what);
                                }
                            }
                        }
                    }
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
            (DebugEventMsg::Send(s), DebugEventMsg::Receive(r)) => {
                s == r
            }
            (_s, _r) => {
                false
            }
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

enum Cmd<APP: Application> {
    AddMessage(
        Option<NoatunTime>,
        APP::Message,
        oneshot::Sender<Result<()>>,
    ),
    Quit(oneshot::Sender<()>),
    GetStatus(oneshot::Sender<Status>),
    InstallDebugEventLogger(
        Box<dyn FnMut(DebugEvent) + 'static + Send + Sync>,
        oneshot::Sender<()>,
    ),
}


struct FillInFrequency {
    who: Address,
}


struct SeenBy {
    who: Address,
    when: Instant
}


struct DecayingKnowledge {
    short_term: f32,
    long_term: f32,
    epic_term: f32,
    last_update: Instant,
    interval_seconds: f32,
}
impl Default for DecayingKnowledge {
    fn default() -> Self {
        Self {
            short_term: 0.0,
            long_term: 0.0,
            epic_term: 0.0,
            last_update: Instant::now(),
            interval_seconds: 0.0,
        }
    }
}

impl DecayingKnowledge {
    pub fn check(&mut self) -> bool {
        self.decay();
        self.short_term + self.long_term + self.epic_term > 0.0
    }
    fn decay(&mut self)  {
        let now = Instant::now();
        let elapsed_secs = now.duration_since(self.last_update).as_secs_f32();
        let short_k = elapsed_secs/self.interval_seconds;
        let long_k = 0.1 * short_k;
        let epic_k = 0.01 * short_k;
        self.last_update = now;
        self.short_term *= (-elapsed_secs*short_k).exp();
        self.long_term *= (-elapsed_secs*long_k).exp();
        self.epic_term *= (-elapsed_secs*epic_k).exp();
        self.short_term = self.short_term.clamp(-10.0,10.0);
        self.long_term = self.long_term.clamp(-10.0,10.0);
        self.epic_term = self.epic_term.clamp(-10.0,10.0);
    }
    pub fn observe_true(&mut self) {
        self.short_term += 1.0;
        self.long_term += 1.0;
        self.epic_term += 1.0;
    }
    pub fn observe_false(&mut self) {
        self.short_term -= 5.0;
        self.long_term -= 1.0;
        self.epic_term -= 1.0;
    }
}


/*
compile_error!("Do this:\
Each node has a randomized back-off interval of 10s (or periodic message interval) + 10s * [num nodes].
When it fires, the node observes for a while after if anyone else answered the same query.
If not, it reduces its delay to 0. It keeps looking for other nodes answering the same query.
If it sees that, it compares the node-id:s. If it's smaller, it resets its state to the regular
back-off interval.

This is all kept per "original source". So nodes keep track of which node they first observed
having each message-id. This means we can handle situations where different messages need be
treated differently.

We also introduce a squelch-message. A node that gets multiple answers to the same query sends
a squelch to one of them. This is for the case where the nodes cannot hear each other, and can't
themselves figure out that a squelch is needed.

The squelch is also "per source". The squelch has a lifetime. It's also possible to un-squelch,
which is done if duplicate partner is unavailable.

There is also force-un-squelch, which forces transmission of a channel
")*/

//TODO: Consider the responsibilities of 'communciation.rs' and 'distributor.rs'
// I think possibly this should be put in 'distributor.rs'. And the latter should perhaps
// be split into submodules.
pub(crate) struct PeerOriginInfo {
    /// The peer this information concerns
    peer: EphemeralNodeId,
    /// The origin of that peer this information concerns
    origin: EphemeralNodeId,
    /// True if 'peer' can normally hear 'origin'
    can_hear_source: DecayingKnowledge,

}
impl PeerOriginInfo {
    pub fn new(peer: EphemeralNodeId, source: EphemeralNodeId) -> PeerOriginInfo {
        PeerOriginInfo {
            peer,
            origin: source,
            can_hear_source: DecayingKnowledge::default(),
        }
    }
}

/// Information about a neighbor's neighbors
pub(crate) struct NeighborNeighborInfo {
    node_id: EphemeralNodeId,
    //squelched: DecayingKnowledge,
    is_neighbor: DecayingKnowledge,
}

//TODO: Clean up all unused stuff here and around
pub(crate) struct PeerInfo {
    pub(crate) peer: EphemeralNodeId,
    /// For messages we first observed from key (`Address`), this peer is usually filled in
    /// by messages from `SeenBy`.
    pub(crate) source_info: IndexMap<EphemeralNodeId, PeerOriginInfo>,
    pub(crate) peer_neighbors: Vec<EphemeralNodeId>,

    /// set to true whenever we decide to inhibit a request based on the idea that
    /// there's another node in our group that should be doing the request
    pub(crate) request_inhibited_based_on_node_numbers: NodeNumberBasedInhibit,
    pub(crate) send_msg_and_descendants_based_on_node_numbers: NodeNumberBasedInhibit, //TODO: Unused?
    pub(crate) resend_actual_message_based_on_node_numbers: NodeNumberBasedInhibit,

}
impl PeerInfo {
    pub fn new(peer: EphemeralNodeId) -> PeerInfo {
        PeerInfo {
            peer,
            source_info: Default::default(),
            peer_neighbors: vec![],
            request_inhibited_based_on_node_numbers: NodeNumberBasedInhibit::default(),
            send_msg_and_descendants_based_on_node_numbers: NodeNumberBasedInhibit::default(),
            resend_actual_message_based_on_node_numbers: Default::default(),
        }
    }
    pub fn we_should_answer_request(&self, our_id: EphemeralNodeId) -> bool {
        error!("Checking peer's (={:?}) neighborlist: {:?} looking for us: {:?}", self.peer,self.peer_neighbors, our_id);
        if let Some(our_index_in_their_neighbor_list) = self.peer_neighbors.iter().position(|x|*x == our_id) {
            println!("our index in neighborlist: {}", our_index_in_their_neighbor_list);
            if our_index_in_their_neighbor_list == 0 {
                return true;
            }
        }
        false
    }
    pub fn cant_hear(&mut self, source: EphemeralNodeId) {
        let source_info = self.source_info.entry(source).or_insert_with(|| PeerOriginInfo::new(self.peer, source));
        source_info.can_hear_source.observe_false();
    }
    pub fn can_hear(&mut self, source: EphemeralNodeId) {
        let source_info = self.source_info.entry(source).or_insert_with(|| PeerOriginInfo::new(self.peer, source));
        source_info.can_hear_source.observe_true();
    }
    pub fn record_squelch(&mut self, origin: EphemeralNodeId) {
        //TODO: Implement or remove this
        todo!()/*7let origin = self.source_info.entry(origin).or_insert_with(|| PeerOriginInfo {
            peer: self.peer,
            origin: origin,
            can_hear_source: Default::default(),
        });*/
    }
}


pub(crate) struct MessageSourceInfo {
    pub(crate) original_source: EphemeralNodeId,
    pub(crate) transmitter_seen: bool,
    pub(crate) other_transmitters: bool
}

#[derive(Default)]
pub(crate) struct RecentMessages {
    pub(crate) recent_messages: IndexMap<MessageId, MessageSourceInfo>,
}
impl RecentMessages {
    pub(crate) fn get_node_for(&mut self, message_id: &MessageId) -> Option<EphemeralNodeId> {
        self.recent_messages.get_mut(message_id).map(|x|x.original_source)
    }

    fn get(&mut self, message_id: &MessageId) -> Option<&mut MessageSourceInfo> {
        self.recent_messages.get_mut(message_id)
    }
    /// actual_transmission is to be set to true if message was transmitted in full, not just
    /// mentioned. This is used to help us understand if a node has a message because of a
    /// retransmission. I.e, if actual_transmission=false, this event itself can't be such a
    /// retransmission.
    fn record_message_source(&mut self, message: MessageId, source: EphemeralNodeId, actual_transmission: bool ) {
        match self.recent_messages.entry(message) {
            Entry::Vacant(e) => {
                e.insert( MessageSourceInfo {
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

#[derive(Default)]
pub(crate) struct Peers {
    peers: IndexMap<EphemeralNodeId, PeerInfo>,
}

impl Peers {
    /*pub(crate) fn request_upstream_inhibited(&self, peer: EphemeralNodeId) -> bool {
        if let Some(info) = self.peers.get(&peer) {
            info.request_inhibited_based_on_node_numbers
        } else {
            true
        }

    }*/
    pub fn get_insert_peer(&mut self, peer_id: EphemeralNodeId) -> &mut PeerInfo {
        self.peers.entry(peer_id).or_insert_with(||PeerInfo::new(peer_id))
    }
    pub fn get_insert_peer2(&mut self, peer_id: EphemeralNodeId) -> &PeerInfo {
        self.peers.entry(peer_id).or_insert_with(||PeerInfo::new(peer_id))
    }
    pub fn get_peer_mut(&mut self, peer_id: EphemeralNodeId) -> Option<&mut PeerInfo> {
        self.peers.get_mut(&peer_id)
    }
    pub fn get_peer(&self, peer_id: EphemeralNodeId) -> Option<&PeerInfo> {
        self.peers.get(&peer_id)
    }
    pub fn get_neighbors(&self) -> Vec<EphemeralNodeId> {
        let mut t: Vec<_> = self.peers.keys().copied().collect();
        t.sort_unstable();
        t
    }

    fn record_squelch(&mut self, destination: EphemeralNodeId, origin: EphemeralNodeId) {
        let peer = self.peers.entry(destination).or_insert_with(||PeerInfo::new(destination));
        peer.record_squelch(origin);
    }
}

pub(crate) struct QuarantinedMessage {
    /// The message we're talking about
    message_id: MessageId,
    /// Where we got the reference to the message
    immediate_source: EphemeralNodeId,
    /// An original origin we might be able to ask to get the message relayed from
    origin: EphemeralNodeId,
    /// The first time we noticed we lacked this message
    first_need: Instant,
}

#[derive(Default)]
pub(crate) struct NodeNumberBasedInhibit {
    was_inhibited: bool,
    inhibit_with_no_one_else_taking_up_the_slack_count: usize,
}

struct Patience(usize);

impl Patience {
    pub fn tax(&mut self) -> bool {
        if self.0 == 0 {
            true
        } else {
            self.0 -= 1;
            false
        }
    }
}


impl NodeNumberBasedInhibit {
    pub(crate) fn satisfied(&mut self) {
        self.was_inhibited = false;
        self.inhibit_with_no_one_else_taking_up_the_slack_count = 0;
    }
    pub(crate) fn time_passed(&mut self) {
        if self.was_inhibited {
            self.inhibit_with_no_one_else_taking_up_the_slack_count += 1;
        }
    }
    pub fn is_inhibited(&mut self,
                        self_node: EphemeralNodeId,
                        our_neighbor_list: &[EphemeralNodeId],
                        neighbors_of_requester: &[EphemeralNodeId]
    ) -> bool {
        let mut patience = Patience(self.inhibit_with_no_one_else_taking_up_the_slack_count);
        trace!("is_inhibited #{:?}: our: {:?} neigh: {:?}", self_node, our_neighbor_list, neighbors_of_requester);
        for our_neighbor in our_neighbor_list {
            if *our_neighbor >= self_node {
                continue;
            }

            if neighbors_of_requester.contains(&our_neighbor) {
                if patience.tax() {
                    self.was_inhibited = true;
                    trace!("#{:?}: was inhibited", self_node);
                    // This other neighbor should do the request instead
                    return true;
                }
            }

        }
        trace!("#{:?}: was not inhibited", self_node);
        false
    }

}

// TODO: Maybe nodes that notice that they have more neighbors than basically anybody,
// should try to change node-id to a small number, so they can be natural, efficient relays for everybody.
#[derive(Default)]
pub(crate) struct Neighborhood {
    pub(crate) peers: Peers,
    /// The source we've observed for recent messages.
    pub(crate) recent_messages: RecentMessages,

    pub(crate) quarantine: IndexMap<MessageId, QuarantinedMessage>,
}

impl Neighborhood {

    /// request_from = the origin of the request we're about to respond to
    /// self_node = ourselves
    pub(crate) fn is_inhibited(&mut self, request_from: EphemeralNodeId, self_node: EphemeralNodeId, kind :impl FnOnce(&mut PeerInfo) -> &mut NodeNumberBasedInhibit ) -> bool {
        let neighbors_list: SmallVec<[_;16]> = self.peers.peers.keys().copied().collect();
        let Some(requesting_node) = self.peers.get_peer_mut(request_from) else {
            return true;
        };
        if requesting_node.peer_neighbors.is_empty() {
            // Not fully up-and-running yet
            return true;
        }
        let requesting_node_neighbors: SmallVec<[_;16]> = requesting_node.peer_neighbors.iter().copied().collect();
        let inhibitor = kind(requesting_node);
        inhibitor.is_inhibited(self_node, &neighbors_list, &requesting_node_neighbors)
    }

    /// A previous tentatively missing path may actually not be a missing path, it might just
    /// be there's some jitter (which is normal and expected).
    ///
    /// The 'msg' is a message that was just received and had all parents resolved.
    pub(crate) fn check_if_tentative_missing_path_was_actually_ok(&mut self, msg: MessageId)  {
        self.quarantine.swap_remove(&msg);
    }

    /// Origin is where our immediate source got it from.
    pub(crate) fn record_tentative_missing_path(&mut self, parent_we_are_missing: MessageId, immediate_source: EphemeralNodeId, origin: EphemeralNodeId) {
        match self.quarantine.entry(parent_we_are_missing) {
            Entry::Occupied(mut e) => {
                let e = e.get_mut();
                if immediate_source < e.immediate_source {
                    e.immediate_source = immediate_source;
                    e.origin = origin;
                }
                if immediate_source == e.immediate_source {
                    e.origin = origin.min(e.immediate_source);
                }

            }
            Entry::Vacant(mut e) => {
                e.insert(QuarantinedMessage {
                    message_id: parent_we_are_missing,
                    immediate_source,
                    origin,
                    first_need: Instant::now(),
                });
            }
        }
    }

    pub fn record_message(&mut self, message: &DistributorMessage, our_node_id: EphemeralNodeId) {
        //TODO Finish epohemeralnodeid stuff, make sure to implement re-randomization on conflicts! And clean up old history
        match message {
            DistributorMessage::Squelch {
                source, transmitter, messages
            } => {
                if *transmitter == our_node_id {
                    let peer = self.peers.get_insert_peer(*source);
                    for message in messages {
                        if let Some(source) = self.recent_messages.get(message) {
                            peer.record_squelch(source.original_source);
                        }
                    }
                }
            }
            DistributorMessage::ReportHeads{heads, source,..}=> {
                let peer = self.peers.get_insert_peer(*source);
                peer.request_inhibited_based_on_node_numbers.time_passed();
                for head in heads {
                    if let Some(msg_source) = self.recent_messages.get(head) {
                        if msg_source.other_transmitters == false {
                            peer.can_hear(msg_source.original_source);
                        }
                    }
                    self.recent_messages.record_message_source(*head, *source, false);
                }
            }
            DistributorMessage::SyncAllQuery(_) => {}
            DistributorMessage::SyncAllRequest(_) => {}
            DistributorMessage::SyncAllAck(_) => {}
            DistributorMessage::RequestUpstream { source, query, .. } => {
                let peer = self.peers.get_insert_peer(*source);
                for (queried_msg, _count) in query {
                    if let Some(msg_source) = self.recent_messages.get(queried_msg) {
                        if msg_source.other_transmitters == false {
                            peer.cant_hear(msg_source.original_source);
                        }
                    }
                }


            }
            DistributorMessage::UpstreamResponse { source, messages, .. } => {
                if let Some(peer) = self.peers.get_peer_mut(*source) {
                    peer.request_inhibited_based_on_node_numbers.satisfied();
                }
                for msg in messages {
                    self.recent_messages.record_message_source(msg.id, *source, false);
                }
            }
            DistributorMessage::SendMessageAndAllDescendants { .. } => {}
            DistributorMessage::Message{source, message:msg, ..} => {
                self.recent_messages.record_message_source(msg.message_id(), *source, true);
            }
        }
    }
}

#[derive(Debug)]
pub struct DuplicationChecker<T> {
    pub(crate) memory: IndexMap<T, Instant>,
    pub(crate) gc_counter: usize,
    pub(crate) interval: Duration
}
impl<T:Eq+Hash> DuplicationChecker<T> {
    pub fn new(interval: Duration) -> DuplicationChecker<T> {
        DuplicationChecker {
            memory: Default::default(),
            gc_counter: 0,
            interval,
        }
    }
    /// Returns true if duplicate.
    pub fn is_duplicate(&mut self, id: T) -> bool {
        self.gc_counter += 1;
        if self.gc_counter > 10000 {
            self.gc_counter = 0;
            self.memory.retain(|k,v|v.elapsed() <= 2*self.interval);
        }
        match self.memory.entry(id) {
            Entry::Occupied(e) => {
                let e = e;
                if e.get().elapsed() > self.interval {
                    e.swap_remove();
                    false
                } else {
                    true
                }
            }
            Entry::Vacant(e) => {
                e.insert(Instant::now());
                false
            }
        }
    }
}

#[derive(Debug)]
pub struct QueryableOutbuffer {
    pub(crate) outbuf: VecDeque<DistributorMessage>,
    pub(crate) recent_sent: VecDeque<DistributorMessage>,

    pub(crate) request_upstream_message_inhibit: DuplicationChecker<MessageId>,
    pub(crate) periodic_message_interval: Duration,

    pub(crate) recently_sent_upstream_responses_for: DuplicationChecker<MessageId>,
    pub(crate) recently_sent_message_ids:  DuplicationChecker<MessageId>,
}

impl QueryableOutbuffer {
    pub fn new(periodic_message_interval: Duration) -> Self {
        Self {
            outbuf: Default::default(),
            recent_sent: Default::default(),
            request_upstream_message_inhibit: DuplicationChecker::new(2*periodic_message_interval),
            periodic_message_interval: periodic_message_interval,
            recently_sent_upstream_responses_for: DuplicationChecker::new(2*periodic_message_interval),
            recently_sent_message_ids: DuplicationChecker::new(2*periodic_message_interval),
        }
    }
    //TODO: Implement detection of node id duplicates
    pub(crate) fn upstream_response_blocked(&mut self, id: MessageId) -> bool {
        self.recently_sent_upstream_responses_for.is_duplicate(id)
    }
    pub(crate) fn message_already_sent(&mut self, id: MessageId) -> bool {
        self.recently_sent_message_ids.is_duplicate(id)
    }

    pub(crate) fn len(&self) -> usize {
        self.outbuf.len()
    }

    pub(crate) fn request_upstream(
        &mut self,
        messages_to_request: impl Iterator<Item = (MessageId, usize)>,
        self_node: EphemeralNodeId,
        request_from: EphemeralNodeId,
        neighbors: &mut Neighborhood
    ) {

        if neighbors.is_inhibited(request_from, self_node, |info|&mut info.request_inhibited_based_on_node_numbers) {
            return;
        }

        let query: Vec<_> = messages_to_request
            .filter(
                |msg| {
                    !self.request_upstream_message_inhibit.is_duplicate(msg.0)
                }
            )
            .collect();

        if !query.is_empty() {
            self.outbuf
                .push_back(DistributorMessage::RequestUpstream { query, source: self_node, destination: request_from });
        }
    }
}

struct DatabaseCommunicationLoop<APP: Application + Send>
where
    <APP as Application>::Params: Send,
    Self: Send,
{
    database: Arc<Mutex<Database<APP>>>,
    /// Join handle for MulticastSenderLoop
    jh: tokio::task::JoinHandle<Result<()>>,
    sender_tx: Sender<Vec<u8>>,
    receiver_rx: Receiver<(Address, Vec<u8>)>,
    distributor: Distributor,
    cmd_rx: Receiver<Cmd<APP>>,

    /// When the first item was put into the buffer
    buffer_life_start: Instant,
    next_periodic: tokio::time::Instant,
    buffered_incoming_messages: Vec<(Address /*src*/, DistributorMessage)>,
    outbuf: QueryableOutbuffer,
    neighborhood: Neighborhood,
    nextsend: Vec<u8>,
    nextsend_id: Option<MessageId>,
    node: String, //Address as string
    debug_event_logger: Option<Box<dyn FnMut(DebugEvent) + 'static + Send + Sync>>,
    report_head_interval: Duration,
}

impl<APP: Application + Send> Drop for DatabaseCommunicationLoop<APP>
where
    <APP as Application>::Params: Send,
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
        }
    }
}

impl<APP: Application + 'static + Send> DatabaseCommunicationLoop<APP>
where
    <APP as Application>::Params: Send,

    <APP as Application>::Message: Send,
{
    fn process_packet(&mut self, src: Address, packet: Vec<u8>) -> Result<()> {
        let msg: DistributorMessage = Deserializer::bare_deserialize(&mut Cursor::new(&packet), 0)?;
        trace!("Received DistrMsg {:?}", msg);
        self.buffered_incoming_messages.push((src, msg));
        Ok(())
    }
    fn process_messages(&mut self) -> Result<()> {
        if self.buffered_incoming_messages.is_empty() {
            return Ok(());
        }
        let mut database = self.database.lock().unwrap();
        if let Some(dbg) = &mut self.debug_event_logger {
            for msg in self.buffered_incoming_messages.iter() {
                dbg(DebugEvent {
                    node: ArrayString::from(&self.node).unwrap_or_else(|_| Default::default()),
                    time: Instant::now(),
                    msg: DebugEventMsg::Receive(msg.1.debug_format::<APP::Message>()?),
                });
            }
        }
        self.outbuf.recent_sent.clear();
        self.distributor.receive_message(
            &mut *database,
            self.buffered_incoming_messages.drain(..),
            &mut self.outbuf,
            &mut self.neighborhood
        )?;
        drop(database);
        Ok(())
    }

    fn debug_record(&mut self, msg: &DistributorMessage) -> Result<()> {
        if let Some(dbg) = &mut self.debug_event_logger {
            dbg(DebugEvent {
                node: ArrayString::from(&self.node).unwrap_or_else(|_| Default::default()),
                time: Instant::now(),
                msg: DebugEventMsg::Send(msg.debug_format::<APP::Message>()?),
            });
        }
        Ok(())
    }

    pub(crate) async fn run(self, senderloop_quit_tx: oneshot::Sender<()>) -> Result<()> {
        let result = self.run2().await;
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
    #[instrument(skip(self), fields(node=?self.node))]
    pub(crate) async fn run2(mut self) -> Result<Option<tokio::sync::oneshot::Sender<()>>> {
        self.nextsend.clear();
        let mut buffer_timer_instant = None;
        loop {
            // For buffered incoming messages
            let buffer_len = self.buffered_incoming_messages.len();
            let buffer_life_start = self.buffer_life_start;

            let buffering_timer = async move {
                if buffer_len > 0 {
                    if buffer_len > 1000 || buffer_life_start.elapsed() > Duration::from_secs(2) {
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

            if self.nextsend.is_empty() && !self.outbuf.is_empty() {
                let msg = self.outbuf.pop_front().unwrap();
                self.debug_record(&msg)?;
                self.nextsend_id = msg.message_id();
                Serializer::bare_serialize(&mut self.nextsend, 0, &msg)?;
                //info!("Sending {:?} as {} byte", msg, self.nextsend.len());
            }

            let sendtask = async {
                if !self.nextsend.is_empty() {
                    let permit = self.sender_tx.reserve().await?;
                    if let Some(nextsend_id) = self.nextsend_id {
                        let mut db = self
                            .database
                            .lock()
                            .map_err(|e| anyhow!("mutex lock failed: {:?}", e))?;
                        let mut sess = db.begin_session_mut()?;
                        if !sess.mark_transmitted(nextsend_id)? {
                            self.nextsend_id = None;
                            self.nextsend.clear();
                            return Ok::<(), anyhow::Error>(());
                        }
                        self.nextsend_id = None;
                    }
                    permit.send(std::mem::take(&mut self.nextsend));
                } else {
                    std::future::pending::<()>().await;
                }
                Ok::<(), anyhow::Error>(())
            };

            select!(
                biased;
                res = sendtask => {
                    res?;
                }
                _process_incoming = buffering_timer => {
                    buffer_timer_instant = None;
                    self.process_messages()?;
                }
                _periodic = tokio::time::sleep_until(self.next_periodic) => {
                    let database = self.database.lock().unwrap();
                    let session = database.begin_session()?;
                    let msgs = self.distributor.get_periodic_message(&session, &self.neighborhood)?;
                    trace!("Time for periodic messages: {:?}", msgs);
                    self.outbuf.extend(msgs);
                    self.next_periodic += self.report_head_interval;
                }
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        //println!("Cmd rx gone!");
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
                            let db = self.database.lock().unwrap();
                            let now = db.noatun_now();
                            sender.send(self.distributor.get_status(now)).map_err(|_|anyhow!("oneshot sender failed"))?;
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
                                    self.outbuf.push_back(
                                        DistributorMessage::Message{
                                            source: self.distributor.node_id(),
                                            message:SerializedMessage::new(msg)?,
                                            demand_ack: false,
                                            origin: self.distributor.node_id()
                                    });
                                    _ = result.send(Ok(()));
                                }
                                Err(err) => {
                                    _ = result.send(Err(err));
                                }
                            }
                        }
                    }
                }
                recv_pkt = self.receiver_rx.recv() => {
                    let Some(recv_pkt) = recv_pkt else {
                        bail!("sender loop quit");
                    };
                    self.process_packet(recv_pkt.0, recv_pkt.1)?;
                }

            );
        }
    }
}

struct NoatunRuntime {}

pub struct DatabaseCommunication<APP: Application> {
    database: Arc<Mutex<Database<APP>>>,
    cmd_tx: Sender<Cmd<APP>>,
    node: String,
}

impl<APP: Application + 'static + Send> DatabaseCommunication<APP>
where
    <APP as Application>::Params: Send,
    <APP as Application>::Message: Send,
{
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
    pub fn inner_database(&self) -> MutexGuard<Database<APP>> {
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

    pub async fn add_message(&self, msg: APP::Message) -> Result<()> {
        self.add_message_impl(None, msg).await
    }

    pub async fn add_message_at(&self, time: NoatunTime, msg: APP::Message) -> Result<()> {
        self.add_message_impl(Some(time), msg).await
    }
    async fn add_message_impl(&self, time: Option<NoatunTime>, msg: APP::Message) -> Result<()> {
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
    pub fn blocking_add_message_at(&self, time: NoatunTime, msg: APP::Message) -> Result<()> {
        self.blocking_add_message(Some(time), msg)
    }
    /// Must *not* be called from within a tokio runtime.
    pub fn blocking_add_message(&self, time: Option<NoatunTime>, msg: APP::Message) -> Result<()> {
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
    pub fn with_root<R>(&self, f: impl FnOnce(&APP) -> R) -> R {
        let db = self.database.lock().unwrap();
        db.with_root(f)
    }

    #[instrument(skip(self),fields(node=?self.node))]
    pub fn set_mock_time(&mut self, time: NoatunTime) -> Result<()> {
        let mut db = self.database.lock().unwrap();
        let mut sess = db.begin_session_mut()?;

        sess.set_mock_time(time).unwrap();
        sess.maybe_advance_cutoff().unwrap();
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
        preview: impl Iterator<Item = APP::Message>,
        f: impl FnOnce(&APP) -> R,
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

    /// Spawns the communication system as a future on the current tokio runtime.
    pub async fn async_tokio_new<Driver: CommunicationDriver + 'static>(
        driver: &mut Driver,
        database: Database<APP>,
        config: DatabaseCommunicationConfig,
    ) -> Result<DatabaseCommunication<APP>> {
        let (sender_tx, sender_rx) = tokio::sync::mpsc::channel(1);
        let (quit_tx, quit_rx) = tokio::sync::oneshot::channel();
        let (receiver_tx, receiver_rx) = tokio::sync::mpsc::channel(1000);
        let sender_loop = MulticastSenderLoop::new(
            driver,
            &config.listen_address,
            &config.multicast_address,
            receiver_tx,
            sender_rx,
            config.bandwidth_limit_bytes_per_second,
            quit_rx,
            config.mtu,
            Duration::from_secs_f32(config.retransmit_interval_seconds),
            config.retransmit_buffer_size_bytes,
            config.disable_retransmit
        )
        .await?;
        let node = sender_loop.bind_address.to_string();
        let jh = spawn(sender_loop.run());

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(100);

        let database = Arc::new(Mutex::new(database));

        let main = DatabaseCommunicationLoop {
            distributor: Distributor::new(config.periodic_message_interval, config.initial_ephemeral_node_id.unwrap_or_else(EphemeralNodeId::random)),
            node: node.clone(),
            database: database.clone(),
            jh,
            sender_tx,
            receiver_rx,
            cmd_rx,
            buffer_life_start: Instant::now(),
            next_periodic: tokio::time::Instant::now(),
            buffered_incoming_messages: vec![],
            outbuf: QueryableOutbuffer::new(config.periodic_message_interval),
            neighborhood: Neighborhood::default(),
            nextsend: vec![],
            nextsend_id: None,
            debug_event_logger: config.debug_logger,
            report_head_interval: config.periodic_message_interval,
        };

        spawn(main.run(quit_tx));

        Ok(DatabaseCommunication {
            database,
            cmd_tx,
            node,
        })
    }

    fn start_async_runtime<Driver: CommunicationDriver + Sync + Send + 'static>(
        driver: &mut Driver,
        database: Database<APP>,
        config: DatabaseCommunicationConfig,
    ) -> Result<(Runtime, DatabaseCommunication<APP>)> {
        let runtime = Runtime::new()?;
        let com: DatabaseCommunication<APP> =
            runtime.block_on(Self::async_tokio_new(driver, database, config))?;
        Ok((runtime, com))
    }

    pub fn new(
        database: Database<APP>,
        config: DatabaseCommunicationConfig,
    ) -> Result<DatabaseCommunication<APP>> {
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
    use crate::tests::setup_tracing;
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

    #[tokio::test(start_paused = true)]
    async fn test_sender_ipv4() {
        let (sender_tx1, sender_rx1) = tokio::sync::mpsc::channel(1000);
        let (_quit_tx1, quit_rx1) = tokio::sync::oneshot::channel();
        let (receiver_tx1, _receiver_rx1) = tokio::sync::mpsc::channel(1000);

        let mloop1 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "192.168.1.112:0",
            "224.1.2.3:7776",
            receiver_tx1,
            sender_rx1,
            10000,
            quit_rx1,
            200,
            Duration::from_secs_f32(1.0),
            1_000_000,
            false
        )
        .await
        .unwrap();

        let (sender_tx2, sender_rx2) = tokio::sync::mpsc::channel(1000);
        let (_quit_tx2, quit_rx2) = tokio::sync::oneshot::channel();
        let (receiver_tx2, mut receiver_rx2) = tokio::sync::mpsc::channel(1000);

        let mloop2 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "192.168.1.112:0",
            "224.1.2.3:7776",
            receiver_tx2,
            sender_rx2,
            10000,
            quit_rx2,
            200,
            Duration::from_secs_f32(1.0),
            1_000_000,
            false
        )
        .await
        .unwrap();

        let jh1 = spawn(mloop1.run());
        let jh2 = spawn(mloop2.run());

        for packet in [
            vec![1u8; 1],
            vec![2u8; 10],
            vec![3u8; 250],
            vec![4u8; 1000],
            vec![5u8; 10000],
        ] {
            sender_tx1.send(packet.clone()).await.unwrap();

            let (_src, got) = receiver_rx2.recv().await.unwrap();
            assert_eq!(got, packet);
        }
        std::thread::sleep(Duration::from_secs(1000));
        drop(sender_tx1);
        drop(sender_tx2);
        jh1.await.unwrap().unwrap();
        jh2.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_sender_ipv6() {
        setup_tracing();
        let (sender_tx1, sender_rx1) = tokio::sync::mpsc::channel(1000);
        let (_quit_tx1, quit_rx1) = tokio::sync::oneshot::channel();
        let (receiver_tx1, _receiver_rx1) = tokio::sync::mpsc::channel(1000);

        let mloop1 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "[::]:0",
            "[ff02::42:41%2]:7775",
            receiver_tx1,
            sender_rx1,
            10000,
            quit_rx1,
            200,
            Duration::from_secs_f32(1.0),
            100,
            false
        )
        .await
        .unwrap();

        let (sender_tx2, sender_rx2) = tokio::sync::mpsc::channel(1000);
        let (_quit_tx2, quit_rx2) = tokio::sync::oneshot::channel();
        let (receiver_tx2, mut receiver_rx2) = tokio::sync::mpsc::channel(1000);

        let mloop2 = MulticastSenderLoop::new(
            &mut TokioUdpDriver,
            "[::]:0",
            "[ff02::42:41%2]:7775",
            receiver_tx2,
            sender_rx2,
            10000,
            quit_rx2,
            200,
            Duration::from_secs_f32(1.0),
            100,
            false
        )
        .await
        .unwrap();

        let jh1 = spawn(mloop1.run());
        let jh2 = spawn(mloop2.run());

        for packet in [vec![1u8; 1], vec![2u8; 10]] {
            sender_tx1.send(packet.clone()).await.unwrap();

            let (_src, got) = receiver_rx2.recv().await.unwrap();
            assert_eq!(got, packet);
        }
        drop(sender_tx1);
        drop(sender_tx2);
        jh1.await.unwrap().unwrap();
        jh2.await.unwrap().unwrap();
    }
}
