use crate::distributor::{Distributor, DistributorMessage, SerializedMessage, Status};

use crate::communication::udp::TokioUdpDriver;
use crate::{Application, Database, Message, MessageId, MessagePayload, NoatunTime};
use anyhow::{anyhow, bail, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Utc};
use indexmap::{IndexMap, IndexSet};
use savefile::{
    Deserialize, Deserializer, Introspect, IntrospectItem, Packed, SavefileError, Schema,
    Serialize, Serializer, WithSchema, WithSchemaContext,
};
use savefile_derive::Savefile;
use std::collections::VecDeque;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::error::Elapsed;
use tokio::time::Instant;
use tokio::{select, spawn};
use tracing::{debug, error, info, instrument, trace, warn};

pub mod udp {

    use crate::communication::{
        CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket,
    };
    use anyhow::{bail, Context};
    use socket2::{Domain, Protocol, Type};
    use std::net::{IpAddr, SocketAddr};
    use tokio::net::UdpSocket;
    use tracing::info;

    impl CommunicationDriver for TokioUdpDriver {
        type Receiver = tokio::net::UdpSocket;
        type Sender = CommunicationUdpSendSocket;
        type Endpoint = SocketAddr;

        async fn initialize(
            &mut self,
            bind_address: &str,
            multicast_group: &str,
            mtu: usize,
        ) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
            let multicast_group: SocketAddr = multicast_group
                .parse()
                .context(format!("parsing multicast group {}", multicast_group))?;
            let bind_address: SocketAddr = bind_address
                .parse()
                .context(format!("parsing listening/bind address {}", bind_address))?;
            let send_socket = UdpSocket::bind(bind_address).await?;
            let udp = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
            if mtu >= u16::MAX as usize {
                bail!("Maximum MTU supported by noatun is 65534");
            }
            udp.set_reuse_address(true)?;
            udp.set_multicast_loop_v4(true)?;
            info!("Binding to group {:?}", multicast_group);
            udp.bind(&multicast_group.into())?;
            udp.set_nonblocking(true)?;
            let receive_socket = UdpSocket::from_std(udp.into())?;

            match (multicast_group.ip(), bind_address.ip()) {
                (IpAddr::V4(multicast_ipv4), IpAddr::V4(bind_ipv4)) => {
                    info!(
                        "Joining multicast group {} on if {}",
                        multicast_ipv4, bind_ipv4
                    );
                    receive_socket.join_multicast_v4(multicast_ipv4, bind_ipv4)?;
                    receive_socket.set_multicast_loop_v4(true)?;
                }
                (IpAddr::V6(multicast_ipv6), IpAddr::V6(_bind_ipv6)) => {
                    //TODO: Fix ipv6 multicast?
                    receive_socket.join_multicast_v6(&multicast_ipv6, 0)?;
                    receive_socket.set_multicast_loop_v6(true)?;
                }
                _ => {
                    panic!(
                        "Bind address and multicast group used different address family. They must both be ipv4 or both ipv6."
                    );
                }
            }

            Ok((
                CommunicationUdpSendSocket {
                    socket: send_socket,
                    multicast_addr: multicast_group,
                },
                receive_socket,
            ))
        }

        fn parse_endpoint(s: &str) -> anyhow::Result<Self::Endpoint> {
            Ok(s.parse().context(format!("couldn't parse {:?}", s))?)
        }
    }
    pub struct TokioUdpDriver;
    pub struct CommunicationUdpSendSocket {
        multicast_addr: SocketAddr,
        socket: UdpSocket,
    }
    impl CommunicationSendSocket<SocketAddr> for CommunicationUdpSendSocket {
        fn local_addr(&self) -> anyhow::Result<SocketAddr> {
            Ok(self.socket.local_addr()?)
        }

        async fn send_to(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            UdpSocket::send_to(&self.socket, buf, self.multicast_addr).await
        }
    }

    impl CommunicationReceiveSocket<SocketAddr> for tokio::net::UdpSocket {
        async fn recv_buf_from<B: bytes::BufMut + Send>(
            &mut self,
            buf: &mut B,
        ) -> std::io::Result<(usize, SocketAddr)> {
            UdpSocket::recv_buf_from(self, buf).await
        }
    }
}

#[derive(Savefile, Debug)]
enum NetworkPacket<Endpoint> {
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

#[derive(Debug)]
enum TransmittedEntity<Endpoint> {
    Sortable(TransmittedEntitySortable),
    Retransmit(u64, Endpoint),
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

#[derive(Default)]
struct ReceiveTrack {
    accum: VecDeque<u8>,
    expected_next: u64,
    sorted_packets: VecDeque<SortableTransmittedEntity>,
}

impl ReceiveTrack {
    /// How long are packets kept in the retransmit window.
    /// I.e, after this has passed, they're lost forever.
    pub const RETRANSMIT_WINDOW: usize = 1000;
    pub const RETRANSMIT_WINDOW_U16: u16 = if Self::RETRANSMIT_WINDOW > u16::MAX as usize {
        panic!("RETRANSMIT_WINDOW constant value too large")
    } else {
        Self::RETRANSMIT_WINDOW as u16
    };

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

    async fn process<Endpoint: Hash + Eq + Debug>(
        &mut self,
        packet: TransmittedEntitySortable,
        packet_source: Endpoint,
        tx_finished_received_frame: &mut Sender<Vec<u8>>,
        retransmit_requests: &mut IndexMap<Endpoint, RetransmitInfo>,
        push: bool,
    ) -> Result<()> {
        let reconstructed_seq = self.reconstruct_seq(packet.seq);
        /*if let Some(info) = retransmit_requests.get_mut(&packet_source) {
            info.items.swap_remove(&reconstructed_seq);
            if info.items.is_empty() {
                retransmit_requests.swap_remove(&packet_source);
            }
        };
         */
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
        while let Some(first) = self.sorted_packets.front() {
            if first.reconstructed_seq != self.expected_next {
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
                    let accum = retransmit_requests.entry(packet_source).or_default();
                    for x in self.expected_next..first.reconstructed_seq {
                        accum.items.insert(x);
                    }
                    if !push {
                        accum.wait_until = Instant::now() + Duration::from_secs(1);
                        //TODO: Dynamic? Or configurable?
                    }
                    let mut cur = first.reconstructed_seq;
                    'loop1: for key in &self.sorted_packets {
                        while cur < key.reconstructed_seq {
                            accum.items.insert(cur);
                            cur += 1;
                            if accum.items.len() > 100 {
                                break 'loop1;
                            }
                        }
                        cur = key.reconstructed_seq + 1;
                    }
                    assert!(accum.items.is_empty() == false);

                    return Ok(());
                }
            }
            if first.entity.first_boundary == u16::MAX {
                self.accum.extend(&first.entity.data);
            } else {
                self.accum
                    .extend(&first.entity.data[0..first.entity.first_boundary as usize]);
                if !self.accum.is_empty() {
                    tx_finished_received_frame
                        .try_send(self.accum.iter().copied().collect())
                        .unwrap();
                    self.accum.clear();
                }
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
                        tx_finished_received_frame.try_send(temp).unwrap();
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

struct MulticasterSenderLoop<Socket: CommunicationDriver> {
    send_socket: Socket::Sender,
    receive_socket: Socket::Receiver,
    bind_address: Socket::Endpoint,
    history: VecDeque<SortableTransmittedEntity>,
    queue: VecDeque<SortableTransmittedEntity>,
    outgoing_retransmit_requests: IndexMap<Socket::Endpoint, RetransmitInfo>,
    receive_track: IndexMap<Socket::Endpoint, ReceiveTrack>,
    quit_rx: tokio::sync::oneshot::Receiver<()>,
    /// Sent to net
    message_rx: Receiver<Vec<u8>>,
    /// Received from net
    message_tx: Sender<Vec<u8>>,
    last_send: Instant,
    last_send_size: usize,
    recvbuf: Vec<u8>,
    max_payload_per_packet: usize,
    next_send_seq: u64,
    limiter: BwLimiter,
}
impl<Socket: CommunicationDriver> Drop for MulticasterSenderLoop<Socket> {
    fn drop(&mut self) {
        info!("Dropping MulticasterSenderLoop loop");
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

impl<Socket: CommunicationDriver> MulticasterSenderLoop<Socket> {
    pub async fn new(
        driver: &mut Socket,
        bind_address: &str,
        multicast_group: &str,
        message_tx: Sender<Vec<u8>>,
        message_rx: Receiver<Vec<u8>>,
        bandwidth_bytes_per_second: u64,
        quit_rx: tokio::sync::oneshot::Receiver<()>,
        mtu: usize,
    ) -> Result<MulticasterSenderLoop<Socket>> {
        let (send_socket, receive_socket) = driver
            .initialize(bind_address, multicast_group, mtu)
            .await?;
        let max_payload_per_packet = mtu.saturating_sub(APPROX_HEADER_SIZE);
        info!("Send socket bind-address: {:?}", send_socket.local_addr()?);

        if max_payload_per_packet < 100 {
            bail!("Unreasonably small MTU specified: {}", mtu);
        }
        Ok(Self {
            quit_rx,
            bind_address: send_socket.local_addr()?,
            send_socket,
            receive_socket,
            history: Default::default(),
            queue: Default::default(),
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
        })
    }
    pub fn send_buf(
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
    pub fn queue_retransmits(&mut self, what: &[u64]) {
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
            info!("enqueued {:?} from history", what);
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
                        debug!("Sending raw retransmit {:?} ({} bytes)", packet, temp.len());
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
                        if self.history.len() > 100 {
                            //TODO: Is this default always enough?
                            self.history.pop_front();
                        }
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

                                match self.receive_track.entry(src_addr).or_default()
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

enum Cmd<APP: Application> {
    AddMessage(
        Option<NoatunTime>,
        APP::Message,
        oneshot::Sender<Result<()>>,
    ),
    Quit(oneshot::Sender<()>),
    GetStatus(oneshot::Sender<Status>),
}

struct DatabaseCommunicationLoop<APP: Application + Send>
where
    <APP as Application>::Params: Send,
    Self: Send,
{
    database: Arc<Mutex<Database<APP>>>,
    jh: tokio::task::JoinHandle<Result<()>>,
    sender_tx: Sender<Vec<u8>>,
    receiver_rx: Receiver<Vec<u8>>,
    distributor: Distributor,
    cmd_rx: Receiver<Cmd<APP>>,

    /// When the first item was put into the buffer
    buffer_life_start: Instant,
    next_periodic: tokio::time::Instant,
    buffered_incoming_messages: Vec<DistributorMessage>,
    outbuf: VecDeque<DistributorMessage>,
    nextsend: Vec<u8>,
    nextsend_id: Option<MessageId>,
    node: String, //Address as string
}

impl<APP: Application + Send> Drop for DatabaseCommunicationLoop<APP>
where
    <APP as Application>::Params: Send,
    Self: Send,
{
    fn drop(&mut self) {
        info!("Dropping com loop");
    }
}

pub struct DatabaseCommunicationConfig {
    pub listen_address: String,
    pub multicast_address: String,
    pub mtu: usize,
    pub bandwidth_limit_bytes_per_second: u64,
}

impl Default for DatabaseCommunicationConfig {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:0".to_string(),
            multicast_address: "230.230.230.230:7777".to_string(),
            mtu: 1000,
            bandwidth_limit_bytes_per_second: 1000,
        }
    }
}

impl<APP: Application + 'static + Send> DatabaseCommunicationLoop<APP>
where
    <APP as Application>::Params: Send,

    <APP as Application>::Message: Send,
{
    const PERIODIC_MSG_INTERVAL: Duration = Duration::from_secs(5);

    fn process_packet(&mut self, packet: Vec<u8>) -> Result<()> {
        let msg: DistributorMessage = Deserializer::bare_deserialize(&mut Cursor::new(&packet), 0)?;
        trace!("Received DistrMsg {:?}", msg);
        self.buffered_incoming_messages.push(msg);
        Ok(())
    }
    fn process_messages(&mut self) -> Result<()> {
        let mut database = self.database.lock().unwrap();
        let new_msgs = self
            .distributor
            .receive_message(&mut *database, self.buffered_incoming_messages.drain(..))?;
        drop(database);
        self.outbuf.extend(new_msgs);
        Ok(())
    }

    pub async fn run(self, senderloop_quit_tx: oneshot::Sender<()>) -> Result<()> {
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
    pub async fn run2(mut self) -> Result<Option<tokio::sync::oneshot::Sender<()>>> {
        self.nextsend.clear();
        loop {
            // For buffered incoming messages
            let buffer_len = self.buffered_incoming_messages.len();
            let buffer_life_start = self.buffer_life_start;
            let buffering_timer = async move {
                if buffer_len > 0 {
                    if buffer_len > 1000 || buffer_life_start.elapsed() > Duration::from_secs(2) {
                    } else {
                        info!("Sleeping 10ms, waiting for buffer to fill");
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        //TODO: Longer sleep, maybe configurable?
                    }
                } else {
                    std::future::pending().await
                }
            };

            if self.nextsend.is_empty() && !self.outbuf.is_empty() {
                let msg = self.outbuf.pop_front().unwrap();
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
                        if !db.mark_transmitted(nextsend_id)? {
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
                    self.process_messages()?;
                }
                _periodic = tokio::time::sleep_until(self.next_periodic) => {
                    let database = self.database.lock().unwrap();
                    let msgs = self.distributor.get_periodic_message(&*database)?;
                    info!("Time for periodic messages: {:?}", msgs);
                    self.outbuf.extend(msgs);
                    self.next_periodic += Self::PERIODIC_MSG_INTERVAL;
                }
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        //println!("Cmd rx gone!");
                        info!("cmd rx, sender is gone");
                        return Ok(None); //Done
                    };
                    match cmd {
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
                            let mut temp = vec![];
                            msg.serialize(&mut temp)?; //TODO: get rid of this serialization
                            let res = database.append_local_opt(time, msg);

                            match res {

                                Ok(res) => {
                                    let msg: APP::Message = APP::Message::deserialize(&temp).unwrap();
                                    self.outbuf.push_back(
                                        DistributorMessage::Message(SerializedMessage::new(
                                            Message {
                                                header: res.clone(),
                                                payload: msg
                                            }
                                        )?, false)
                                    );
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
                    self.process_packet(recv_pkt)?;
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
    pub fn set_mock_time(&mut self, time: NoatunTime) {
        let mut db = self.database.lock().unwrap();
        db.set_mock_time(time).unwrap();
        db.maybe_advance_cutoff().unwrap();
    }
    pub fn get_all_messages(&self) -> Result<Vec<Message<APP::Message>>> {
        self.database.lock().unwrap().get_all_messages()
    }
    pub fn get_update_heads(&self) -> Vec<crate::MessageId> {
        self.database.lock().unwrap().get_update_heads().to_vec()
    }
    pub fn get_cutoff_time(&self) -> Result<NoatunTime> {
        Ok(self
            .database
            .lock()
            .unwrap()
            .current_cutoff_state()?
            .before_time
            .to_noatun_time())
    }
    /// TODO: Probably remove this before release, it should never be necessary
    pub fn reproject(&mut self) -> Result<()> {
        self.database.lock().unwrap().reproject()?;
        Ok(())
    }
    pub fn with_root_preview<R>(
        &self,
        time: DateTime<Utc>,
        preview: impl Iterator<Item = APP::Message>,
        f: impl FnOnce(&APP) -> R,
    ) -> Result<R> {
        let mut db = self.database.lock().unwrap();

        db.with_root_preview(time, preview, f)
    }

    pub fn set_projection_time_limit(&mut self, limit: NoatunTime) -> Result<()> {
        let mut db = self.database.lock().unwrap();
        db.set_projection_time_limit(limit)
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
        let sender_loop = MulticasterSenderLoop::new(
            driver,
            &config.listen_address,
            &config.multicast_address,
            receiver_tx,
            sender_rx,
            config.bandwidth_limit_bytes_per_second,
            quit_rx,
            config.mtu
        )
        .await?;
        let node = sender_loop.bind_address.to_string();
        let jh = spawn(sender_loop.run());

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(100);

        let database = Arc::new(Mutex::new(database));

        let main = DatabaseCommunicationLoop {
            distributor: Distributor::new(&node),
            node: node.clone(),
            database: database.clone(),
            jh,
            sender_tx,
            receiver_rx,
            cmd_rx,
            buffer_life_start: Instant::now(),
            next_periodic: tokio::time::Instant::now(),
            buffered_incoming_messages: vec![],
            outbuf: Default::default(),
            nextsend: vec![],
            nextsend_id: None,
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
    use crate::communication::{MulticasterSenderLoop, ReceiveTrack};
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

    #[tokio::test(start_paused=true)]
    async fn test_sender() {
        let (sender_tx1, sender_rx1) = tokio::sync::mpsc::channel(1000);
        let (_quit_tx1, quit_rx1) = tokio::sync::oneshot::channel();
        let (receiver_tx1, _receiver_rx1) = tokio::sync::mpsc::channel(1000);

        let mloop1 = MulticasterSenderLoop::new(
            &mut TokioUdpDriver,
            "127.0.0.1:0",
            "230.230.230.230:7777",
            receiver_tx1,
            sender_rx1,
            10000,
            quit_rx1,
            200,
        )
        .await
        .unwrap();

        let (sender_tx2, sender_rx2) = tokio::sync::mpsc::channel(1000);
        let (_quit_tx2, quit_rx2) = tokio::sync::oneshot::channel();
        let (receiver_tx2, mut receiver_rx2) = tokio::sync::mpsc::channel(1000);

        let mloop2 = MulticasterSenderLoop::new(
            &mut TokioUdpDriver,
            "127.0.0.1:0",
            "230.230.230.230:7777",
            receiver_tx2,
            sender_rx2,
            10000,
            quit_rx2,
            200,
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

            let got = receiver_rx2.recv().await.unwrap();
            assert_eq!(got, packet);
        }
        drop(sender_tx1);
        drop(sender_tx2);
        jh1.await.unwrap().unwrap();
        jh2.await.unwrap().unwrap();
    }
}
