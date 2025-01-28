
use crate::distributor::{Distributor, DistributorMessage, SerializedMessage};

use crate::{Application, ContextGuard, Database, DatabaseContextData, Message, MessageHeader, MessagePayload};
use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use libc::newlocale;
use savefile::{
    Deserialize, Deserializer, Field, Introspect, IntrospectItem, Packed, SavefileError, Schema,
    SchemaPrimitive, SchemaStruct, Serialize, Serializer, WithSchema, WithSchemaContext,
};
use savefile_derive::Savefile;
use socket2::{Domain, Protocol, SockRef, Type};
use std::collections::VecDeque;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, SocketAddr};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::spawn_local;
use tokio::{select, spawn};
use tokio::runtime::{Builder, Runtime};

#[derive(Savefile, Debug)]
enum NetworkPacket {
    Data(TransmittedEntity),
    Retransmit { who: IpAddr, what: Vec<u64> },
}

const IP_HEADER_SIZE: usize = 20;
const UDP_HEADER_SIZE: usize = 8;
const NOATUN_NETWORK_PACKET_OVERHEAD: usize = 1;
const NOATUN_TRANSMITTED_ENTITY_OVERHEAD: usize = 6;
const APPROX_HEADER_SIZE: usize = IP_HEADER_SIZE
    + UDP_HEADER_SIZE
    + NOATUN_NETWORK_PACKET_OVERHEAD
    + NOATUN_TRANSMITTED_ENTITY_OVERHEAD;

#[derive(Debug)]
struct TransmittedEntity {
    seq: u16,
    data: Vec<u8>,
    // Note, u16::MAX signifies "no boundary"
    first_boundary: u16,
}

impl Introspect for TransmittedEntity {
    fn introspect_value(&self) -> String {
        format!(
            "TransmittedEntity(#{},first_boundary={},len={})",
            self.seq,
            self.first_boundary,
            self.data.len()
        )
    }

    fn introspect_child<'a>(&'a self, index: usize) -> Option<Box<dyn IntrospectItem<'a> + 'a>> {
        None
    }
}

impl WithSchema for TransmittedEntity {
    fn schema(version: u32, context: &mut WithSchemaContext) -> Schema {
        Schema::Custom("TransmittedEntity".into())
    }
}
impl Packed for TransmittedEntity {}
impl Serialize for TransmittedEntity {
    fn serialize(
        &self,
        serializer: &mut Serializer<impl Write>,
    ) -> std::result::Result<(), SavefileError> {
        serializer.write_u16(self.seq)?;
        assert!(self.data.len() <= u16::MAX as usize);
        serializer.write_u16(self.data.len() as u16)?;
        serializer.write_bytes(&self.data);
        serializer.write_u16(self.first_boundary)?;
        Ok(())
    }
}
impl Deserialize for TransmittedEntity {
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

impl TransmittedEntity {
    fn free(&self, max_payload: usize) -> usize {
        max_payload.saturating_sub(self.data.len())
    }
}

#[derive(Debug)]
struct SortableTransmittedEntity {
    reconstructed_seq: u64,
    entity: TransmittedEntity,
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

    async fn process(&mut self, packet: TransmittedEntity, tx: &mut Sender<Vec<u8>>) -> Result<()> {
        let packet = SortableTransmittedEntity {
            reconstructed_seq: self.reconstruct_seq(packet.seq),
            entity: packet,
        };
        let Err(insert_point) = self
            .sorted_packets
            .binary_search_by_key(&packet.reconstructed_seq, |x| x.reconstructed_seq)
        else {
            // Already existed
            println!("Already had packet: {:?}", &packet);
            return Ok(());
        };

        self.sorted_packets.insert(insert_point, packet);
        while let Some(first) = self.sorted_packets.front() {
            if first.reconstructed_seq != self.expected_next {
                if self.expected_next == 0 {
                    self.expected_next = first.reconstructed_seq;
                } else {
                    return Ok(());
                }
            }
            if first.entity.first_boundary == u16::MAX {
                self.accum.extend(&first.entity.data);
            } else {
                self.accum
                    .extend(&first.entity.data[0..first.entity.first_boundary as usize]);
                if !self.accum.is_empty() {
                    //println!("Sending out {:?}", self.accum);
                    tx.try_send(self.accum.iter().copied().collect()).unwrap();
                    //println!("Send done");
                    self.accum.clear();
                }
                let mut cur_boundary = first.entity.first_boundary as usize;
                let mut reader = Cursor::new(&first.entity.data);
                reader.seek(SeekFrom::Start(cur_boundary as u64));

                while cur_boundary < first.entity.data.len() {
                    let next_size = reader.read_u16::<LittleEndian>()? as usize;
                    cur_boundary += 2;
                    if next_size == u16::MAX as usize {
                        break;
                    }
                    let mut temp = vec![0; next_size];
                    reader.read_exact(&mut temp)?;
                    if !temp.is_empty() {
                        //println!("Sending out {:?}", temp);
                        tx.try_send(temp).unwrap();
                        //println!("Send done");
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

struct MulticasterSenderLoop {
    send_socket: UdpSocket,
    receive_socket: UdpSocket,
    bind_address: IpAddr,
    multicast_group: SocketAddr,
    history: VecDeque<SortableTransmittedEntity>,
    queue: VecDeque<SortableTransmittedEntity>,
    receive_track: IndexMap<SocketAddr, ReceiveTrack>,
    /// Sent to net
    message_rx: Receiver<Vec<u8>>,
    /// Received from net
    message_tx: Sender<Vec<u8>>,
    last_send: Instant,
    last_send_size: usize,
    recvbuf: Vec<u8>,
    max_payload_per_packet: usize,
    next_send_seq: u64,
}
impl Drop for MulticasterSenderLoop {
    fn drop(&mut self) {
        println!("Dropping MulticasterSenderLoop loop");
    }
}

impl MulticasterSenderLoop {
    pub async fn new(
        bind_address: SocketAddr,
        multicast_group: SocketAddr,
        message_tx: Sender<Vec<u8>>,
        message_rx: Receiver<Vec<u8>>,
        bandwidth_bytes_per_second: u64,
        mtu: usize,
    ) -> Result<MulticasterSenderLoop> {
        let send_socket = UdpSocket::bind(bind_address).await?;
        let udp = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        if mtu >= u16::MAX as usize {
            bail!("Maximum MTU supported by noatun is 65534");
        }
        udp.set_reuse_address(true)?;
        udp.set_multicast_loop_v4(true);
        println!("Binding to group {:?}", multicast_group);
        udp.bind(&multicast_group.into())?;
        udp.set_nonblocking(true);
        let receive_socket = UdpSocket::from_std(udp.into())?;

        let max_payload_per_packet = mtu.saturating_sub(APPROX_HEADER_SIZE);
        println!("Send socket bind-address: {:?}", send_socket.local_addr()?);
        match (multicast_group.ip(), bind_address.ip()) {
            (IpAddr::V4(multicast_ipv4), IpAddr::V4(bind_ipv4)) => {
                println!("Joining multicast group {} on if {}", multicast_ipv4, bind_ipv4);
                receive_socket.join_multicast_v4(multicast_ipv4, bind_ipv4);
                receive_socket.set_multicast_loop_v4(true);
            }
            (IpAddr::V6(multicast_ipv6), IpAddr::V6(bind_ipv6)) => {
                receive_socket.join_multicast_v6(&multicast_ipv6, 0);
                receive_socket.set_multicast_loop_v6(true);
            }
            _ => {
                panic!(
                    "Bind address and multicast group used different address family. They must both be ipv4 or both ipv6."
                );
            }
        }
        if max_payload_per_packet < 100 {
            bail!("Unreasonably small MTU specified: {}", mtu);
        }
        Ok(Self {
            send_socket,
            bind_address: bind_address.ip(),
            receive_socket,
            history: Default::default(),
            queue: Default::default(),
            receive_track: Default::default(),
            message_rx,
            message_tx,
            last_send: Instant::now(),
            last_send_size: 0,
            recvbuf: Vec::with_capacity(mtu),
            max_payload_per_packet,
            next_send_seq: 0,
            multicast_group,
        })
    }
    pub fn send_buf(
        queue: &mut VecDeque<SortableTransmittedEntity>,
        max_payload_per_packet: usize,
        next_send_seq: &mut u64,
        buffer: Vec<u8>,
    ) {
        let mut is_first;
        let buffer: &[u8] = if let Some(last) = queue.back_mut() {
            if last.entity.first_boundary != u16::MAX {
                if last.entity.free(max_payload_per_packet) >= 2 + buffer.len() {
                    last.entity
                        .data
                        .write_u16::<LittleEndian>(buffer.len().try_into().unwrap());
                    last.entity.data.extend(buffer);
                    return;
                }
                last.entity.data.write_u16::<LittleEndian>(u16::MAX);
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
            data.write_all(&buffer[reader_pos..reader_pos + chunk]);
            reader_pos += chunk;
            queue.push_back(SortableTransmittedEntity {
                reconstructed_seq: *next_send_seq,
                entity: TransmittedEntity {
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
                return;
            };
            self.queue.push_front(history_item);
        }
    }
    pub async fn run(mut self) {
        let mut cursend: Option<Vec<u8>> = None;
        loop {
            self.recvbuf.clear();
            let receive = self.receive_socket.recv_buf_from(&mut self.recvbuf);

            if cursend.is_none() {
                cursend = self.queue.pop_front().map(|x| {
                    let mut temp = vec![];
                    // Consider if savefile really is the best here. Some more efficiency
                    // wouldn't hurt!
                    Serializer::bare_serialize(&mut temp, 0, &NetworkPacket::Data(x.entity))
                        .unwrap();
                    temp
                });
            }

            let mut send = async {
                if let Some(tosend) = cursend.as_mut() {
                    //println!("Sending {} bytes on wire", tosend.len());
                    match self
                        .send_socket
                        .send_to(tosend, &self.multicast_group)
                        .await
                    {
                        Ok(sent) => {
                            //println!("Sent {} byte packet:     {:?}", sent, tosend);
                            cursend.take();
                        }
                        Err(err) => {
                            eprintln!("Send error: {:?}", err);
                        }
                    }
                } else {
                    std::future::pending().await
                }
            };
            let get_cmd = self.message_rx.recv();
            select! {
                    buf = get_cmd => {
                        if let Some(buf) = buf {
                            Self::send_buf(
                                &mut self.queue,
                                self.max_payload_per_packet,
                                &mut self.next_send_seq,
                                buf
                            );
                        } else {
                            eprintln!("Has ended");
                            return;
                        }
                    }
                    _ = send => {
                    }
                    msg = receive => {
                        let (size, addr) = msg.expect("network should not fail");
                        if addr == self.send_socket.local_addr().unwrap() { //TODO: maybe remember local_addr, don't get it each time
                            continue;
                        }
                        println!("Received {} byte packet: {:?}", size, self.recvbuf);
                        assert_eq!(size, self.recvbuf.len());
                        let Ok(packet): Result<NetworkPacket,_> = Deserializer::bare_deserialize(&mut Cursor::new(&self.recvbuf),0)  else {
                            eprintln!("Invalid packet received");
                            continue;
                        };
                        //println!("Deserialized into {:?}", packet);
                        match packet {
                            NetworkPacket::Data(entity) => {
                                //println!("TransmittedEntity");
                                match self.receive_track.entry(addr).or_default()
                                    .process(entity, &mut self.message_tx).await {
                                        Ok(()) => {}
                                        Err(err) => {
                                            eprintln!("Receive error: {:?}", err);
                                    }
                                }
                            }
                            NetworkPacket::Retransmit{who, what  } => {
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
    AddMessage(Option<DateTime<Utc>>, APP::Message, oneshot::Sender<Result<()>>),
}

struct DatabaseCommunicationLoop<APP: Application+Send> where <APP as Application>::Params: Send, Self:Send{
    database: Arc<Mutex<Database<APP>>>,
    jh: tokio::task::JoinHandle<()>,
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
}

impl<APP:Application+Send> Drop for DatabaseCommunicationLoop<APP> where <APP as Application>::Params: Send, Self:Send {
    fn drop(&mut self) {
        println!("Dropping com loop");
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


impl<APP: Application + 'static+Send> DatabaseCommunicationLoop<APP> where
    <APP as Application>::Params: Send,

    <APP as Application>::Message: Send{
    const PERIODIC_MSG_INTERVAL: Duration = Duration::from_secs(5);

    fn process_packet(&mut self, packet: Vec<u8>) -> Result<()> {
        let msg: DistributorMessage = Deserializer::bare_deserialize(&mut Cursor::new(&packet), 0)?;
        println!("Received {:?}", msg);
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

    pub async fn run(mut self) -> Result<()> {
        let result = self.run2().await;
        println!("run result {:?}", result);
        result
    }
    pub async fn run2(mut self) -> Result<()> {
        self.nextsend.clear();
        loop {

            // For buffered incoming messages
            let buffer_len = self.buffered_incoming_messages.len();
            let buffer_life_start = self.buffer_life_start;
            let mut buffering_timer = async move {
                if buffer_len > 0 {
                    if buffer_len > 1000 || buffer_life_start.elapsed() > Duration::from_secs(2) {
                    } else {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                } else {
                    std::future::pending().await
                }
            };

            if self.nextsend.is_empty() && !self.outbuf.is_empty() {
                let msg = self.outbuf.pop_front().unwrap();
                println!("Sending {:?}", msg);
                Serializer::bare_serialize(&mut self.nextsend, 0, &msg)?;
            }
            let sendtask = async {
                if !self.nextsend.is_empty() {
                    let permit = self.sender_tx.reserve().await?;
                    permit.send(std::mem::take(&mut self.nextsend));
                } else {
                    std::future::pending().await
                }
                Ok::<(), SendError<()>>(())
            };

            select!(
                res = sendtask => {
                    res?;
                }
                periodic = tokio::time::sleep_until(self.next_periodic) => {
                    let database = self.database.lock().unwrap();
                    self.outbuf.extend(self.distributor.get_periodic_message(&*database)?);
                    self.next_periodic += Self::PERIODIC_MSG_INTERVAL;
                }
                cmd = self.cmd_rx.recv() => {
                    println!("Cmd received");
                    let Some(cmd) = cmd else {
                        eprintln!("Done"); //TODO
                        return Ok(()); //Done
                    };
                    match cmd {
                        Cmd::AddMessage(time, msg,result) => {
                            let mut database = self.database.lock().unwrap();
                            let mut temp = vec![];
                            msg.serialize(&mut temp); //TODO: get rid of this serialization
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
                process_incoming = buffering_timer => {
                    self.process_messages()?;
                }

            )
        }
    }
}

struct NoatunRuntime {

}

pub struct DatabaseCommunication<APP: Application> {
    database: Arc<Mutex<Database<APP>>>,
    cmd_tx: Sender<Cmd<APP>>,

}

impl<APP: Application + 'static+Send> DatabaseCommunication<APP>
where <APP as Application>::Params: Send,
    <APP as Application>::Message: Send,

{
    pub async fn add_message(&self, msg: APP::Message) -> Result<()> {
        self.add_message_impl(None, msg).await
    }

    pub async fn add_message_at(&self, time: DateTime<Utc>, msg: APP::Message) -> Result<()> {
        self.add_message_impl(Some(time), msg).await
    }
    async fn add_message_impl(&self, time: Option<DateTime<Utc>>, msg: APP::Message) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        match self.cmd_tx.send(Cmd::AddMessage(time, msg, response_tx)).await {
            Ok(()) => {}
            Err(err) => {
                bail!("Failed to AddMessage - background thread no longer running");
            }
        }
        response_rx.await??;
        Ok(())
    }
    pub fn blocking_add_message_at(&self, time: DateTime<Utc>, msg: APP::Message) -> Result<()> {
        self.blocking_add_message(Some(time), msg)
    }
    /// Must *not* be called from within a tokio runtime.
    pub fn blocking_add_message(&self, time: Option<DateTime<Utc>>, msg: APP::Message) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        match self.cmd_tx.blocking_send(Cmd::AddMessage(time, msg, response_tx)) {
            Ok(()) => {}
            Err(err) => {
                bail!("Failed to AddMessage - background thread no longer running");
            }
        }
        response_rx.blocking_recv()??;
        Ok(())
    }
    pub fn with_root<R>(&self, f: impl FnOnce(&APP) -> R) -> R {
        let db = self.database.lock().unwrap();
        db.with_root(f)
    }
    pub fn get_all_messages(&self) -> Result<Vec<Message<APP::Message>>> {
        self.database.lock().unwrap().get_all_messages()
    }
    /// TODO: Probably remove this, it should never be necessary
    pub fn reproject(&mut self) {
        self.database.lock().unwrap().reproject();
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

    pub fn set_projection_time_limit(&mut self, limit: DateTime<Utc>) -> Result<()> {
        let mut db = self.database.lock().unwrap();
        db.set_projection_time_limit(limit)
    }

    /// Spawns the communication system as a future on the current tokio runtime.
    pub async fn async_tokio_new(
        database: Database<APP>,
        config: DatabaseCommunicationConfig,
    ) -> Result<DatabaseCommunication<APP>> {
        let (sender_tx, mut sender_rx) = tokio::sync::mpsc::channel(1000);
        let (receiver_tx, mut receiver_rx) = tokio::sync::mpsc::channel(1000);
        let sender_loop = MulticasterSenderLoop::new(
            config.listen_address.parse().context(format!("parsing listen address {}", config.listen_address))?,
            config.multicast_address.parse().context(format!("parsing multicast address {}", config.multicast_address))?,
            receiver_tx,
            sender_rx,
            config.bandwidth_limit_bytes_per_second,
            config.mtu,
        )
        .await?;
        let jh = spawn(sender_loop.run());

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(100);

        let database = Arc::new(Mutex::new(database));
        let main = DatabaseCommunicationLoop {
            database: database.clone(),
            jh,
            sender_tx,
            receiver_rx,
            distributor: Default::default(),
            cmd_rx,
            buffer_life_start: Instant::now(),
            next_periodic: tokio::time::Instant::now(),
            buffered_incoming_messages: vec![],
            outbuf: Default::default(),
            nextsend: vec![],
        };

        spawn(main.run());

        Ok(DatabaseCommunication { database, cmd_tx })
    }

    fn start_async_runtime(database: Database<APP>, config: DatabaseCommunicationConfig) -> Result<(Runtime, DatabaseCommunication<APP>)> {
        let mut runtime = Runtime::new()?;
        let com: DatabaseCommunication<APP> = runtime.block_on(Self::async_tokio_new(database, config))?;
        Ok((runtime, com))
    }

    pub fn new(
        database: Database<APP>,
        config: DatabaseCommunicationConfig,
    ) -> Result<DatabaseCommunication<APP>> {
        let (res_tx,res_rx) = tokio::sync::oneshot::channel();
        thread::spawn(move||{
            match Self::start_async_runtime(database, config) {
                Ok((runtime,app)) => {
                    res_tx.send(Ok(app));
                    runtime.block_on(std::future::pending::<()>());
                    println!("Dropping runtime");
                }
                Err(err) => {
                    res_tx.send(Err(err));
                }
            }
        });

        let app = res_rx.blocking_recv()??;
        Ok(app)
    }



}

#[cfg(test)]
mod tests {

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

    #[tokio::test]
    async fn test_sender() {
        let (sender_tx, mut sender_rx) = tokio::sync::mpsc::channel(1000);
        let (receiver_tx, mut receiver_rx) = tokio::sync::mpsc::channel(1000);

        let mloop = MulticasterSenderLoop::new(
            "127.0.0.1:0".parse().unwrap(),
            "230.230.230.230:7777".parse().unwrap(),
            receiver_tx,
            sender_rx,
            1000,
            200,
        )
        .await
        .unwrap();

        let jh = spawn(mloop.run());

        println!("About to send");
        for packet in [
            vec![1u8; 1],
            vec![2u8; 10],
            vec![3u8; 250],
            vec![4u8; 1000],
            vec![5u8; 10000],
        ] {
            sender_tx.send(packet.clone()).await.unwrap();
            println!("About to recv");
            let got = receiver_rx.recv().await.unwrap();
            assert_eq!(got, packet);
        }
        println!("quitting");
        drop(sender_tx);
        jh.await.unwrap();
    }
}
