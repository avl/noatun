#![feature(test)]
#![allow(unused)]
#![allow(dead_code)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::comparison_chain)]

extern crate test;

use crate::data_types::DatabaseCell;
use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
use crate::message_store::OnDiskMessageStore;
use crate::platform_specific::{FileMapping, get_boot_time};
use crate::projector::Projector;
use crate::sha2_helper::sha2;
use anyhow::{Context, Result, bail};
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Utc};
pub use database::Database;
use fs2::FileExt;
use indexmap::IndexMap;
use memmap2::MmapMut;
pub use projection_store::DatabaseContext;
use rand::RngCore;
use savefile_derive::Savefile;
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::cell::{Cell, OnceCell};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::{transmute, transmute_copy};
use std::ops::{Add, Deref, Range};
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::slice::SliceIndex;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

mod disk_abstraction;
mod message_store;
mod projection_store;
mod undo_store;

struct MessageComponent<const ID: u32, T> {
    value: Option<T>,
}

pub(crate) mod platform_specific;

mod boot_checksum;
pub(crate) mod disk_access;
mod sha2_helper;

#[cfg(feature = "tokio")]
pub mod communication {
    use anyhow::{Result, bail};
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use indexmap::IndexMap;
    use libc::newlocale;
    use savefile::{Deserialize, Deserializer, Field, Introspect, IntrospectItem, Packed, SavefileError, Schema, SchemaPrimitive, SchemaStruct, Serialize, Serializer, WithSchema, WithSchemaContext};
    use savefile_derive::Savefile;
    use socket2::{Domain, Protocol, SockRef, Type};
    use std::collections::VecDeque;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};
    use std::net::{IpAddr, SocketAddr};
    use std::ops::Deref;
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};
    use tokio::net::UdpSocket;
    use tokio::{select, spawn};
    use tokio::sync::mpsc::{Receiver, Sender};
    use tokio::sync::mpsc::error::SendError;
    use tokio::sync::oneshot;
    use tokio::task::spawn_local;
    use crate::{Application, Database};
    use crate::distributor::{Distributor, DistributorMessage};

    #[derive(Savefile, Debug)]
    enum NetworkPacket {
        Data(TransmittedEntity),
        Retransmit { who: IpAddr, what: Vec<u64> },
    }

    const IP_HEADER_SIZE: usize = 20;
    const UDP_HEADER_SIZE: usize = 8;
    const NOATUN_NETWORK_PACKET_OVERHEAD: usize = 1;
    const NOATUN_TRANSMITTED_ENTITY_OVERHEAD: usize = 6;
    const APPROX_HEADER_SIZE: usize =
        IP_HEADER_SIZE + UDP_HEADER_SIZE+NOATUN_NETWORK_PACKET_OVERHEAD+NOATUN_TRANSMITTED_ENTITY_OVERHEAD;

    #[derive(Debug)]
    struct TransmittedEntity {
        seq: u16,
        data: Vec<u8>,
        // Note, u16::MAX signifies "no boundary"
        first_boundary: u16,
    }

    impl Introspect for TransmittedEntity {
        fn introspect_value(&self) -> String {
            format!("TransmittedEntity(#{},first_boundary={},len={})",
                self.seq, self.first_boundary, self.data.len()
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
        fn serialize(&self, serializer: &mut Serializer<impl Write>) -> std::result::Result<(), SavefileError> {
            serializer.write_u16(self.seq)?;
            assert!(self.data.len() <= u16::MAX as usize);
            serializer.write_u16(self.data.len() as u16)?;
            serializer.write_bytes(&self.data);
            serializer.write_u16(self.first_boundary)?;
            Ok(())
        }
    }
    impl Deserialize for TransmittedEntity {
        fn deserialize(deserializer: &mut Deserializer<impl Read>) -> std::result::Result<Self, SavefileError> {
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
        pub const RETRANSMIT_WINDOW_U16: u16 =
            if Self::RETRANSMIT_WINDOW > u16::MAX as usize {
                panic!("RETRANSMIT_WINDOW constant value too large")
            } else {
                Self::RETRANSMIT_WINDOW as u16
            };

        pub(crate) fn reconstruct_seq(&self, seq:u16) -> u64 {
            Self::reconstruct_seq_impl(self.expected_next, seq)
        }
        pub(crate) fn reconstruct_seq_impl(expected_next: u64, seq:u16) -> u64 {
            let short_delta = seq - (expected_next as u16);

            if short_delta < 65535 - Self::RETRANSMIT_WINDOW_U16 {
                // Interpret as future value
                expected_next + short_delta as u64
            } else {
                // A retransmission, that we don't actually need
                expected_next + short_delta as u64
            }
        }

        async fn process(
            &mut self,
            packet: TransmittedEntity,
            tx: &mut Sender<Vec<u8>>,
        ) -> Result<()> {
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
                    return Ok(());
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
                        let mut temp = Vec::with_capacity(next_size);
                        temp.resize(next_size, 0);
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
            udp.bind(&multicast_group.into())?;
            udp.set_nonblocking(true);
            let receive_socket = UdpSocket::from_std(udp.into())?;

            let max_payload_per_packet = mtu.saturating_sub(APPROX_HEADER_SIZE);
            match (multicast_group.ip(), bind_address.ip()) {
                (IpAddr::V4(multicast_ipv4), IpAddr::V4(bind_ipv4)) => {
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
                multicast_group: multicast_group,
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
                        last.entity.data
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
                queue.push_back(
                    SortableTransmittedEntity {
                        reconstructed_seq: *next_send_seq,
                        entity: TransmittedEntity {
                            seq: *next_send_seq as u16,
                            data,
                            first_boundary: if is_first { 0 } else { u16::MAX },
                        },
                    }
                    );
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
                let Ok(index) = self.history.binary_search_by_key(what, |x| x.reconstructed_seq) else {
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
                        Serializer::bare_serialize(&mut temp, 0, &NetworkPacket::Data(x.entity)).unwrap();
                        temp
                    });
                }

                let mut send = async {
                    if let Some(tosend) = cursend.as_mut() {
                        //println!("Sending {} bytes on wire", tosend.len());
                        match self
                            .send_socket
                            .send_to(&tosend, &self.multicast_group)
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
                            //println!("Received {} byte packet: {:?}", size, self.recvbuf);
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


    enum Cmd<APP:Application> {
        AddMessage(APP::Message, oneshot::Sender<Result<()>>)
    }

    struct DatabaseCommunicationLoop<APP:Application> {
        database: Arc<Mutex<Database<APP>>>,
        jh: tokio::task::JoinHandle<()>,
        sender_tx: Sender<Vec<u8>>,
        receiver_rx: Receiver<Vec<u8>>,
        distributor: Distributor,
        cmd_rx: Receiver<Cmd<APP>>,

        /// When the first item was put into the buffer
        buffer_life_start: Instant,
        next_periodic: tokio::time::Instant,
        bufferd_incoming_messages: Vec<DistributorMessage>,
        outbuf: VecDeque<DistributorMessage>,
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

    pub struct DatabaseCommunication<APP:Application> {
        database: Arc<Mutex<Database<APP>>>,
        cmd_tx: Sender<Cmd<APP>>
    }

    pub struct RootRef<'a, APP:Application> {
        guard : MutexGuard<'a, Database<APP>>
    }

    impl<'a, APP:Application> Deref for RootRef<'a, APP> {
        type Target = APP::Root;

        fn deref(&self) -> &Self::Target {
            self.guard.get_root().0
        }
    }

    impl<APP:Application+'static> DatabaseCommunicationLoop<APP> {
        const PERIODIC_MSG_INTERVAL: Duration = Duration::from_secs(5);



        pub fn process_packet(&mut self, packet: Vec<u8>) -> Result<()> {
            let msg: DistributorMessage = Deserializer::bare_deserialize(&mut Cursor::new(&packet), 0)?;
            println!("Received {:?}", msg);
            self.bufferd_incoming_messages.push(msg);
            Ok(())
        }
        pub fn process_messages(&mut self) -> Result<()> {
            {}
            let mut database = self.database.lock().unwrap();
            let new_msgs = self.distributor.receive_message(
                &mut *database,
                self.bufferd_incoming_messages.drain(..))?;
            drop(database);
            self.outbuf.extend(new_msgs);
            Ok(())
        }

        pub async fn run(mut self) -> Result<()> {
            let mut nextsend = vec![];
            loop {
                // For buffered incoming messages
                let buffer_len = self.bufferd_incoming_messages.len();
                let buffer_life_start = self.buffer_life_start;
                let mut buffering_timer = async move {
                    if buffer_len > 0 {
                        if buffer_len > 1000 || buffer_life_start.elapsed() > Duration::from_secs(2) {} else {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    } else {
                        std::future::pending().await
                    }
                };

                if nextsend.is_empty() && !self.outbuf.is_empty() {
                    let msg = self.outbuf.pop_front().unwrap();
                    println!("Sending {:?}", msg);
                    Serializer::bare_serialize(&mut nextsend, 0, &msg)?;
                }
                let sendtask = async {
                    if !nextsend.is_empty() {
                        let permit = self.sender_tx.reserve().await?;
                        permit.send(std::mem::take(&mut nextsend));
                    } else {
                        std::future::pending().await
                    }
                    Ok::<(),SendError<()>>(())
                };

                select!(
                    res = sendtask => {
                        res?;
                    }
                    periodic = tokio::time::sleep_until(self.next_periodic) => {
                        let database = self.database.lock().unwrap();
                        self.outbuf.extend(self.distributor.get_periodic_message(&*database)?);
                        self.next_periodic = self.next_periodic + Self::PERIODIC_MSG_INTERVAL;
                    }
                    cmd = self.cmd_rx.recv() => {
                        println!("Cmd received");
                        let Some(cmd) = cmd else {
                            eprintln!("Done"); //TODO
                            return Ok(()); //Done
                        };
                        match cmd {
                            Cmd::AddMessage(msg,result) => {
                                let mut database = self.database.lock().unwrap();
                                _ = result.send(database.append_local(msg));
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
    impl<APP:Application+'static> DatabaseCommunication<APP> {

        pub async fn add_message(&self, msg: APP::Message) -> Result<()> {
            let (response_tx, response_rx) = oneshot::channel();
            match self.cmd_tx.send(Cmd::AddMessage(msg, response_tx)).await {
                Ok(()) => {}
                Err(err) => {
                    bail!("Failed to AddMessage");
                }
            }
            response_rx.await??;
            Ok(())
        }
        pub fn get_root(&self) -> RootRef<APP> {
            RootRef {
                guard: self.database.lock().unwrap()
            }
        }
        pub async fn new(database: Database<APP>, config: DatabaseCommunicationConfig) -> DatabaseCommunication<APP> {
            let (sender_tx, mut sender_rx) = tokio::sync::mpsc::channel(1000);
            let (receiver_tx, mut receiver_rx) = tokio::sync::mpsc::channel(1000);
            let sender_loop = MulticasterSenderLoop::new(
                config.listen_address.parse().unwrap(),
                config.multicast_address.parse().unwrap(),
                receiver_tx,
                sender_rx,
                config.bandwidth_limit_bytes_per_second,
                config.mtu,
            )
                .await
                .unwrap();
            let jh = spawn(sender_loop.run());

            let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(100);

            let database = Arc::new(Mutex::new(database));
            let main = DatabaseCommunicationLoop {
                database:database.clone(),
                jh,
                sender_tx,
                receiver_rx,
                distributor: Default::default(),
                cmd_rx,
                buffer_life_start: Instant::now(),
                next_periodic: tokio::time::Instant::now(),
                bufferd_incoming_messages: vec![],
                outbuf: Default::default(),
            };
            spawn_local(main.run());

            DatabaseCommunication {
                database,
                cmd_tx
            }
        }

    }

    #[cfg(test)]
    mod tests {

        use tokio::spawn;
        use crate::communication::{MulticasterSenderLoop,ReceiveTrack};

        #[test]
        fn reconstruct_seq_logic() {
            assert_eq!(
                ReceiveTrack::reconstruct_seq_impl(10, 9),
                9);
            assert_eq!(
                ReceiveTrack::reconstruct_seq_impl(10, 7),
                7);
            assert_eq!(
                ReceiveTrack::reconstruct_seq_impl(65536, 0),
                65536);
            assert_eq!(
                ReceiveTrack::reconstruct_seq_impl(65535, 1),
                65537);
            assert_eq!(
                ReceiveTrack::reconstruct_seq_impl(65536+65535, 1),
                65536+65537);

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
                vec![1u8;1],
                vec![2u8;10],
                vec![3u8;250],
                vec![4u8;1000],
                vec![5u8;10000],
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
}




#[derive(
    Pod,
    Zeroable,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Savefile,
)]
#[repr(transparent)]
pub struct MessageId {
    data: [u32; 4],
}

const ASSURE_SUPPORTED_USIZE: () = const {
    if size_of::<usize>() != 8 {
        panic!("noatun currently only supports 64 bit platforms with 64 bit usize");
    }
};

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let time_ms = self.timestamp();

        let time = chrono::DateTime::from_timestamp_millis(time_ms as i64)
            .unwrap();

        let time_str = time.to_rfc3339();
        write!(
            f,
            "{:?}-{:x}-{:x}-{:x}",
            time_str,
            (self.data[1] & 0xffff0000) >> 16,
            self.data[2],
            self.data[3]
        )
    }
}

impl Debug for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if cfg!(test) && self.data[0] == 0 {
            write!(
                f,
                "#{:x}_{:x}_{:x}",
                self.data[1], self.data[2], self.data[3]
            )
        } else {
            write!(f, "{}", self)
        }
    }
}

impl MessageId {
    pub const ZERO: MessageId = MessageId { data: [0u32; 4] };
    pub fn min(self, other: MessageId) -> MessageId {
        if self < other { self } else { other }
    }
    pub fn is_zero(&self) -> bool {
        self.data[0] == 0 && self.data[1] == 0
    }
    pub fn zero() -> MessageId {
        MessageId { data: [0, 0, 0, 0] }
    }

    /// Create an artificial MessageId, mostly useful for tests and possibly debugging.
    pub fn new_debug(nr: u32) -> Self {
        Self {
            data: [0, 0, 0, nr],
        }
    }

    pub fn generate_for_time(time: DateTime<Utc>) -> Result<MessageId> {
        let mut random_part = [0u8; 10];
        rand::thread_rng().fill_bytes(&mut random_part);
        Self::from_parts(time, random_part)
    }
    pub fn from_parts_for_test(time: DateTime<Utc>, random: u64) -> MessageId {
        let mut data = [0u8; 10];
        data[2..10].copy_from_slice(&random.to_le_bytes());

        Self::from_parts(time, data).unwrap()
    }
    pub fn timestamp(&self) -> u64 {
        let restes = (self.data[0] as u64) + (((self.data[1] & 0xffff) as u64) << 32) as u64;
        restes
    }
    pub fn from_parts(time: DateTime<Utc>, random: [u8; 10]) -> Result<MessageId> {
        let t: u64 = time
            .timestamp_millis()
            .try_into()
            .context("Time value is out of range. Value must be ")?;
        if t >= 1 << 48 {
            bail!("Time value is too large");
        }
        let mut data = [0u8; 16];
        data[0..6].copy_from_slice(&t.to_le_bytes()[0..6]);
        data[6..16].copy_from_slice(&random);

        Ok(MessageId {
            data: bytemuck::cast(data),
        })
    }
    pub fn from_parts_raw(time: u64, random: [u8; 10]) -> Result<MessageId> {
        if time >= 1 << 48 {
            bail!("Time value is too large");
        }
        let mut data = [0u8; 16];
        data[0..6].copy_from_slice(&time.to_le_bytes()[0..6]);
        data[6..16].copy_from_slice(&random);

        Ok(MessageId {
            data: bytemuck::cast(data),
        })
    }
}

pub mod sequence_nr {
    use bytemuck::{Pod, Zeroable};
    use std::fmt::{Debug, Display, Formatter};

    #[derive(Pod, Zeroable, Copy, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    #[repr(transparent)]
    // 0 is an invalid sequence number, used to represent 'not a number'
    pub struct SequenceNr(u32);
    impl Display for SequenceNr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            if self.0 == 0 {
                write!(f, "#INVALID")
            } else {
                write!(f, "#{}", self.0 - 1)
            }
        }
    }
    impl Debug for SequenceNr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            if self.0 == 0 {
                write!(f, "#INVALID")
            } else {
                write!(f, "#{}", self.0 - 1)
            }
        }
    }

    impl SequenceNr {
        pub const INVALID: SequenceNr = SequenceNr(0);
        pub fn is_invalid(self) -> bool {
            self.0 == 0
        }
        pub fn is_valid(self) -> bool {
            self.0 != 0
        }
        pub fn successor(self) -> SequenceNr {
            SequenceNr(self.0 + 1)
        }
        pub fn from_index(index: usize) -> SequenceNr {
            if index >= (u32::MAX - 1) as usize {
                panic!("More than 2^32 elements created. Not supported by noatun");
            }
            SequenceNr(index as u32 + 1)
        }
        pub fn index(self) -> usize {
            if self.0 == 0 {
                panic!("0 SequenceNr does not have an index")
            }
            self.0 as usize - 1
        }
        pub fn try_index(self) -> Option<usize> {
            if self.0 == 0 {
                return None;
            }
            Some(self.0 as usize - 1)
        }
    }
}

pub trait MessagePayload: Debug {
    type Root: Object;
    fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root);

    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;
    fn serialize<W: Write>(&self, writer: W) -> Result<()>;
}

#[derive(Debug)]
pub struct MessageHeader {
    pub id: MessageId,
    pub parents: Vec<MessageId>,
}

#[derive(Debug)]
pub struct Message<M: MessagePayload> {
    pub header: MessageHeader,
    pub payload: M,
}

impl<M: MessagePayload> Message<M> {
    pub fn new(id: MessageId, parents: Vec<MessageId>, payload: M) -> Self {
        Self {
            header: MessageHeader { id, parents },
            payload,
        }
    }
    pub fn id(&self) -> MessageId {
        self.header.id
    }
}

/// A state-less object, mostly useful for testing
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct DummyUnitObject;

impl Object for DummyUnitObject {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        &DummyUnitObject
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        // # SAFETY
        // Any dangling pointer is a valid pointer to a zero-sized type
        unsafe { &mut *(std::ptr::dangling_mut()) }
    }
}

mod update_head_tracker {
    use crate::MessageId;
    use crate::disk_abstraction::Disk;
    use crate::disk_access::FileAccessor;
    use anyhow::Result;

    pub(crate) struct UpdateHeadTracker {
        file: FileAccessor,
    }

    impl UpdateHeadTracker {
        pub(crate) fn add_new_update_head(
            &mut self,
            new_message_id: MessageId,
            subsumed: &[MessageId],
        ) -> anyhow::Result<()> {
            let mapping = self.file.map_mut();
            let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);
            let mut i = 0;
            let mut file_len = id_mapping.len();
            let mut maplen = id_mapping.len();
            while i < maplen {
                if subsumed.contains(&id_mapping[i]) {
                    if i != maplen - 1 {
                        id_mapping.swap(i, maplen - 1);
                    }
                    maplen -= 1;
                } else {
                    i += 1;
                }
            }
            if maplen == file_len {
                self.file.grow((file_len + 1) * size_of::<MessageId>())?;
                file_len = file_len + 1;
            }

            let mapping = self.file.map_mut();
            let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);

            id_mapping[maplen] = new_message_id.clone();
            maplen += 1;

            if maplen < file_len {
                self.file.fast_truncate(maplen);
            }
            Ok(())
        }

        pub(crate) fn get_update_heads(&self) -> &[MessageId] {
            bytemuck::cast_slice(self.file.map())
        }
        pub(crate) fn new<D: Disk>(
            disk: &mut D,
            target: &crate::Target,
        ) -> Result<UpdateHeadTracker> {
            Ok(Self {
                file: disk.open_file(target, "update_head", 0, 10 * 1024 * 1024)?,
            })
        }
    }
}

// TODO: Do we need this?
/*mod update_tail_tracker {
    use crate::disk_abstraction::Disk;
    use crate::disk_access::FileAccessor;
    use crate::message_store::OnDiskMessageStore;
    use crate::{MessagePayload, MessageId, Target, Message};
    use anyhow::Result;

    pub(crate) struct UpdateTailTracker {
        file: FileAccessor,
    }

    /// Contains messages that cannot be applied to main store because we don't know
    /// their ancestors
    pub(crate) struct Quarantine<M: MessagePayload> {
        quarantine: OnDiskMessageStore<Message<M>>,
    }
    impl<M: MessagePayload> Quarantine<M> {
        pub(crate) fn add_new_update_tail(
            &mut self,
            message: &M,
            existing: &OnDiskMessageStore<M>,
        ) -> Result<()> {
            for item in message.parents() {
                if !existing.contains_message(item)? {}
            }
            todo!()
        }

        pub(crate) fn new<D: Disk>(
            disk: &mut D,
            target: &Target,
            max_file_size: usize,
        ) -> anyhow::Result<Self> {
            let mut sub = target.append("tail");
            Ok(Self {
                quarantine: OnDiskMessageStore::new(disk, &sub, max_file_size)?,
            })
        }
    }
}*/

pub(crate) mod cutoff {
    use crate::MessageId;
    use crate::message_store::IndexEntry;
    use bytemuck::{Pod, Zeroable};
    use chrono::{DateTime, Utc};
    use savefile_derive::Savefile;

    pub(crate) struct CutOffConfig {
        /// The approximate time in history at which all nodes must have been in sync.
        /// I.e, all nodes are expected to eventually sync up. I.e, all nodes are expected
        /// to have all messages created prior to (now - interval_ms).next_multiple_of(grace_period_ms)
        age: u64,
        stride: u64,
    }

    impl Default for CutOffConfig {
        fn default() -> Self {
            Self {
                age: 86400_000,
                stride: 3600_000,
            }
        }
    }
    impl CutOffConfig {
        pub fn nominal_cutoff(&self, time_now: DateTime<Utc>) -> u64 {
            CutOffState::nominal_now(time_now.timestamp_millis() as u64, self) //TODO: Try_into
        }
    }

    #[derive(Savefile, Clone, Copy, Debug, Pod, Zeroable, PartialEq, Eq, Default)]
    #[repr(C)]
    pub struct CutoffHash {
        values: [u64; 2],
    }

    impl CutoffHash {
        pub(crate) fn from_all(msg: &[MessageId]) -> CutoffHash {
            let mut temp = CutoffHash::default();
            for m in msg {
                temp.xor_with_msg(*m);
            }
            temp
        }
        fn from(msg: MessageId) -> CutoffHash {
            bytemuck::cast(msg)
        }
        fn xor_with(&mut self, other: CutoffHash) {
            self.values[0] ^= other.values[0];
            self.values[1] ^= other.values[1];
        }
        fn xor_with_msg(&mut self, other: MessageId) {
            self.xor_with(CutoffHash::from(other));
        }
    }

    #[derive(Clone, Copy, Debug, Pod, Zeroable)]
    #[repr(C)]
    struct CutOffHashPos {
        hash: CutoffHash,
        before_time: u64,
    }

    impl CutOffHashPos {
        fn adjust_forward_to(&mut self, time: u64, messages: &[IndexEntry]) {
            assert!(self.before_time <= time);
            if self.before_time == time {
                return; //Nothing to do
            }
            let prior = MessageId::from_parts_raw(self.before_time, [0u8; 10]).unwrap();
            let mut cur_index = match messages.binary_search_by_key(&prior, |x| x.message) {
                Ok(hit) => hit,
                Err(insloc) => insloc,
            };
            while cur_index < messages.len() {
                let cur = &messages[cur_index];
                if cur.message.timestamp() >= time {
                    // Done
                    return;
                }
                self.hash.xor_with_msg(cur.message);
                cur_index += 1;
            }
            self.before_time = time;
        }
    }

    #[derive(Clone, Copy, Debug, Pod, Zeroable)]
    #[repr(C)]
    pub(crate) struct CutOffState {
        /// The prior, the current, and the upcoming,
        stamps: [CutOffHashPos; 3],
    }

    impl CutOffState {
        pub fn is_acceptable_cutoff_hash(&self, hash: CutoffHash) -> bool {
            self.stamps.iter().any(|x| x.hash == hash)
        }
        pub fn nominal_hash(&self) -> CutoffHash {
            self.stamps[1].hash
        }
        /// Now rounded to the nearest multiple of stride
        fn nominal_now(now: u64, config: &CutOffConfig) -> u64 {
            now.saturating_sub(config.stride / 2)
                .next_multiple_of(config.stride)
        }
        pub fn advance_time(&mut self, now: u64, config: &CutOffConfig, messages: &[IndexEntry]) {
            let nominal_now = Self::nominal_now(now, config);
            let prior = nominal_now.saturating_sub(config.stride);
            let next = nominal_now.saturating_add(config.stride);

            self.stamps[0].adjust_forward_to(prior, messages);
            self.stamps[1].adjust_forward_to(nominal_now, messages);
            self.stamps[2].adjust_forward_to(next, messages);
        }

        pub fn report_add(&mut self, message_id: MessageId) {
            self.apply(message_id);
        }
        pub fn report_delete(&mut self, message_id: MessageId) {
            self.apply(message_id);
        }

        /// Add and delete are logically identical ops (because xor)
        fn apply(&mut self, message_id: MessageId) {
            let t = message_id.timestamp();
            for pos in &mut self.stamps {
                if t < pos.before_time {
                    pos.hash.xor_with_msg(message_id);
                }
            }
        }
    }
    #[cfg(test)]
    mod tests {
        use super::{CutOffHashPos, CutoffHash};
        use crate::MessageId;
        use crate::message_store::IndexEntry;

        #[test]
        fn test_advance_pos() {
            let mut pos = CutOffHashPos {
                hash: CutoffHash::from(MessageId::new_debug(0)),
                before_time: 100,
            };

            pos.adjust_forward_to(201, &[IndexEntry {
                message: MessageId::from_parts_raw(200, [0u8; 10]).unwrap(),
                file_offset: crate::message_store::FileOffset::deleted(),
                file_total_size: 0,
            }]);

            assert_eq!(
                pos.hash,
                CutoffHash::from(MessageId::from_parts_raw(200, [0u8; 10]).unwrap())
            );
        }
    }
}
mod projector {
    use crate::cutoff::{CutOffConfig, CutoffHash};
    use crate::disk_abstraction::Disk;
    use crate::disk_access::FileAccessor;
    use crate::message_store::{IndexEntry, OnDiskMessageStore};
    use crate::sequence_nr::SequenceNr;
    use crate::update_head_tracker::UpdateHeadTracker;
    use crate::{
        Application, Database, DatabaseContext, Message, MessageHeader, MessageId, MessagePayload,
        Target,
    };
    use anyhow::Result;
    use bytemuck::{Pod, Zeroable};
    use chrono::{DateTime, Utc};
    use std::marker::PhantomData;
    use std::time::{Duration, SystemTime};

    pub(crate) struct Projector<APP: Application> {
        messages: OnDiskMessageStore<APP::Message>,
        head_tracker: UpdateHeadTracker,
        phantom_data: PhantomData<(*const APP::Root)>,
        cut_off_config: CutOffConfig,
    }

    impl<APP: Application> Projector<APP> {
        pub fn get_upstream_of(
            &self,
            message_id: impl DoubleEndedIterator<Item = (MessageId, usize)>,
        ) -> Result<impl Iterator<Item = (MessageHeader, /*count*/ usize)>> {
            self.messages.get_upstream_of(message_id)
        }

        pub(crate) fn get_update_heads(&self) -> &[MessageId] {
            self.head_tracker.get_update_heads()
        }
        pub(crate) fn get_messages_after(
            &self,
            message: MessageId,
            count: usize,
        ) -> Result<Vec<MessageId>> {
            self.messages.get_messages_at_or_after(message, count)
        }

        pub fn nominal_cutoffhash(&self) -> Result<CutoffHash> {
            self.messages.nominal_cutoffhash()
        }

        pub fn is_acceptable_cutoff_hash(&self, hash: CutoffHash) -> Result<bool> {
            self.messages.is_acceptable_cutoff_hash(hash)
        }

        pub(crate) fn contains_message(&self, id: MessageId) -> Result<bool> {
            self.messages.contains_message(id)
        }

        pub(crate) fn load_message(&self, id: MessageId) -> Result<Message<APP::Message>> {
            Ok(self
                .messages
                .read_message(id)?
                .ok_or_else(|| anyhow::anyhow!("Message not found"))?)
        }

        pub fn recover(&mut self) -> Result<()> {
            self.messages
                .recover(|id, parents| self.head_tracker.add_new_update_head(id, parents))
        }
        pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
            self.messages.get_all_message_ids()
        }
        pub fn get_all_messages(&self) -> Result<Vec<Message<APP::Message>>> {
            self.messages.get_all_messages()
        }
        pub fn get_all_messages_with_children(
            &self,
        ) -> Result<Vec<(Message<APP::Message>, Vec<MessageId>)>> {
            self.messages.get_all_messages_with_children()
        }

        pub(crate) fn new<D: Disk>(
            s: &mut D,
            target: &Target,
            max_size: usize,
            cutoff_interval: Duration,
        ) -> Result<Projector<APP>> {
            Ok(Projector {
                messages: OnDiskMessageStore::new(s, target, max_size)?,
                head_tracker: UpdateHeadTracker::new(s, target)?,
                phantom_data: PhantomData,
                cut_off_config: CutOffConfig::default(),
            })
        }

        pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<()> {
            self.messages.mark_transmitted(message_id)
        }

        /// Returns true if the message did not exist and was inserted
        fn push_message(
            &mut self,
            context: &mut DatabaseContext,
            message: Message<APP::Message>,
            local: bool,
        ) -> Result<bool> {
            self.push_sorted_messages(context, std::iter::once(message), local)
        }

        /// Returns true if any of the messages were not previously present
        pub(crate) fn push_messages(
            &mut self,
            context: &mut DatabaseContext,
            message: impl Iterator<Item = Message<APP::Message>>,
            local: bool,
        ) -> Result<bool> {
            let mut messages: Vec<Message<APP::Message>> = message.collect();
            messages.sort_unstable_by_key(|x| x.id());

            self.push_sorted_messages(context, messages.into_iter(), local)
        }
        pub(crate) fn push_sorted_messages(
            &mut self,
            context: &mut DatabaseContext,
            messages: impl ExactSizeIterator<Item = Message<APP::Message>>,
            local: bool,
        ) -> Result<bool> {
            if let Some(insert_point) = self.messages.append_many_sorted(
                messages,
                |id, parents| self.head_tracker.add_new_update_head(id, parents),
                local,
            )? {
                if let Some(cur_main_db_next_index) = context.next_seqnr().try_index() {
                    if insert_point < cur_main_db_next_index {
                        self.rewind(context, insert_point)?;
                    }
                }

                Ok(true)
            } else {
                Ok(false)
            }
        }
        pub(crate) fn rewind(&mut self, context: &mut DatabaseContext, point: usize) -> Result<()> {
            context.rewind(SequenceNr::from_index(point));
            Ok(())
        }

        fn apply_single_message(
            context: &mut DatabaseContext,
            root: &mut APP::Root,
            msg: &Message<APP::Message>,
            seqnr: SequenceNr,
        ) {
            msg.payload.apply(context, root); //TODO: Handle panics in apply gracefully
            context.set_next_seqnr(seqnr.successor()); //TODO: Don't record a snapshot for _every_ message.
            context.finalize_message(seqnr);
        }

        pub(crate) fn apply_missing_messages(
            &mut self,
            root: &mut APP::Root,
            context: &mut DatabaseContext,
            time_now: DateTime<Utc>,
        ) -> Result<()> {
            let cutoff = self.cut_off_config.nominal_cutoff(time_now);

            let cur_seqnr = context.next_seqnr();

            context.clear_unused_tracking();

            let first_run = self
                .messages
                .query_by_index(context.next_seqnr().try_index().unwrap())?;

            match do_run::<APP>(context, root, first_run, cutoff)? {
                RunResult::NeedRunAfterCutoff(next_run_start) => {
                    remove_stale_messages(self, context, true);
                    let second_run = self.messages.query_by_index(next_run_start)?;
                    let RunResult::Finished(before_cutoff) =
                        do_run::<APP>(context, root, second_run, cutoff)?
                    else {
                        unreachable!(
                            "Second run _also_ encountered both before and after cutoff elements!"
                        )
                    };
                    remove_stale_messages(self, context, before_cutoff);
                }
                RunResult::Finished(before_cutoff) => {
                    remove_stale_messages(self, context, before_cutoff);
                }
            }

            enum RunResult {
                NeedRunAfterCutoff(usize),
                Finished(bool /*before cutoff*/),
            }

            /// If returns true, need to finalize before-cutoff-part, then continue at given index
            fn do_run<APP: Application>(
                context: &mut DatabaseContext,
                root: &mut APP::Root,
                items: impl Iterator<Item = (usize, Message<APP::Message>)>,
                cutoff: u64,
            ) -> Result<RunResult> {
                let mut seen_before_cutoff = false;
                let mut last_element_was_before_cutoff = false;
                for (seq, msg) in items {
                    let seqnr = SequenceNr::from_index(seq);
                    let is_before_cutoff = msg.id().timestamp() < cutoff;
                    if is_before_cutoff {
                        seen_before_cutoff = true;
                    }
                    if !is_before_cutoff && seen_before_cutoff {
                        return Ok(RunResult::NeedRunAfterCutoff(seqnr.index()));
                    }
                    Projector::<APP>::apply_single_message(context, root, &msg, seqnr);
                    last_element_was_before_cutoff = is_before_cutoff;
                }
                Ok(RunResult::Finished(last_element_was_before_cutoff))
            }

            fn remove_stale_messages<APP: Application>(
                tself: &mut Projector<APP>,
                context: &mut DatabaseContext,
                before_cutoff: bool,
            ) -> Result<()> {
                let must_remove =
                    context.calculate_stale_messages(&mut tself.messages, before_cutoff)?;
                for index in must_remove {
                    tself.messages.mark_deleted_by_index(index);
                    //*self.messages.get_index_mut(index.index()).unwrap().1 = None;
                }
                Ok(())
            }
            //let next_index = self.messages.next_index()?;
            //context.set_next_seqnr(SequenceNr::from_index(next_index));
            Ok(())
        }
    }
}

pub enum GenPtr {
    Thin(ThinPtr),
    Fat(FatPtr),
}

pub trait Pointer: Copy + Debug + 'static {
    fn start(self) -> usize;
    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self;
    fn as_generic(&self) -> GenPtr;
}

pub trait Object {
    /// This is meant to be either ThinPtr for sized objects, or
    /// FatPtr for dynamically sized objects. Other types are likely to not make sense.
    type Ptr: Pointer;
    /// Access a shared instance of Self at the given pointer address.
    ///
    /// # Safety
    /// The caller must ensure that the accessed object is not aliased with a mutable
    /// reference to the same object.
    // TODO: Don't expose these methods. Too hard to use correctly!
    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self;
    /// Access a mutable instance of Self at the given pointer address.
    /// NOTE!
    /// Self must not allow direct access to any of its fields. Self must
    /// provide methods that can be used to mutate the fields, and those methods
    /// must report all writes to the DatabaseContext.
    ///
    /// NOTE!
    /// The above holds for all places that may be reachable through Self, not only direct
    /// fields on Self. It also holds for collections or any other data. I.e, if Self
    /// is a collection type, it cannot give direct mutable access to a u8 element, or similar.
    ///
    /// # Safety
    /// The caller must ensure that the accessed object is not aliased with any other
    /// reference to the same object.
    // TODO: Don't expose these methods. Too hard to use correctly!
    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self;
}


// TODO: Remove this! It's not safe.
// You can't provide noatun-features for any POD, since POD can be updated
// without tracking the updates in the noatun database!
#[derive(Clone, Debug, Copy, Pod, Zeroable)]
#[repr(transparent)]
pub struct PodObject<T: Pod> {
    pub pod: T,
}

impl<T: Pod> Object for PodObject<T> {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

pub trait FixedSizeObject: Object<Ptr = ThinPtr> + Sized + Pod {}

impl<T: Object<Ptr = ThinPtr> + Sized + Copy + Pod> FixedSizeObject for T {}

pub trait Application {
    type Root: Object + ?Sized;
    type Message: MessagePayload<Root = Self::Root>;

    fn initialize_root(ctx: &mut DatabaseContext) -> &mut Self::Root;
}

#[derive(Copy, Clone, Debug)]
pub struct FatPtr {
    start: usize,
    /// Size in bytes
    len: usize,
}
impl FatPtr {
    /// Start index, and size in bytes
    pub fn from(start: usize, len: usize) -> FatPtr {
        FatPtr { start, len }
    }
}
// TODO: We should probably have a generic ThinPtr type, like ThinPtr<T>,
// that allows type-safe access to &mut T
#[derive(Copy, Clone, Debug)]
pub struct ThinPtr(pub usize);

unsafe impl Zeroable for FatPtr {}

unsafe impl Pod for FatPtr {}
unsafe impl Zeroable for ThinPtr {}

unsafe impl Pod for ThinPtr {}

impl Pointer for ThinPtr {
    fn start(self) -> usize {
        self.0
    }

    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self {
        let index = (addr as *const T as *const u8 as usize) - (buffer_start as usize);
        ThinPtr(index)
    }

    fn as_generic(&self) -> GenPtr {
        GenPtr::Thin(*self)
    }
}

impl ThinPtr {
    pub fn null() -> ThinPtr {
        ThinPtr(0)
    }
}

impl Pointer for FatPtr {
    fn start(self) -> usize {
        self.start
    }
    fn create<T: ?Sized>(addr: &T, buffer_start: *const u8) -> Self {
        assert_eq!(
            std::mem::size_of::<*const T>(),
            2 * std::mem::size_of::<usize>()
        );

        FatPtr {
            start: ((addr as *const T as *const u8 as usize) - (buffer_start as usize)),
            len: size_of_val(addr),
        }
    }
    fn as_generic(&self) -> GenPtr {
        GenPtr::Fat(*self)
    }
}

impl<T: FixedSizeObject> Object for [T] {
    type Ptr = FatPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_slice(index) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_slice_mut(index) }
    }
}

pub mod data_types {
    use crate::sequence_nr::SequenceNr;
    use crate::{Database, DatabaseContext, FatPtr, FixedSizeObject, Object, Pointer, ThinPtr};
    use bytemuck::{Pod, Zeroable};
    use sha2::digest::typenum::Zero;
    use std::fmt::{Debug, Formatter};
    use std::marker::PhantomData;
    use std::mem::transmute_copy;
    use std::ops::{Deref, Index, Range};

    #[derive(Copy, Clone)]
    #[repr(C)]
    pub struct DatabaseCell<T: Copy> {
        value: T,
        registrar: SequenceNr,
    }

    impl<T:Copy+Debug> Debug for DatabaseCell<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.value.fmt(f)
        }
    }

    //TODO: Document. Also rename this or DatabaseCell, the names should harmonize.
    #[derive(Copy, Clone)]
    #[repr(C)]
    pub struct OpaqueCell<T: Copy> {
        value: T,
        registrar: SequenceNr,
    }

    // TODO: The below (and same for DatabaseCell) are probably not actually sound.
    // There could be padding needed. We could avoid this by making sure SequenceNr
    // has alignment 1.
    unsafe impl<T: Pod> Zeroable for OpaqueCell<T> {}
    unsafe impl<T: Pod> Pod for OpaqueCell<T> {}

    pub trait DatabaseCellArrayExt<T: Pod> {
        fn observe(&self, context: &DatabaseContext) -> Vec<T>;
    }
    impl<T: Pod> DatabaseCellArrayExt<T> for &[DatabaseCell<T>] {
        fn observe(&self, context: &DatabaseContext) -> Vec<T> {
            self.iter().map(|x| x.get(context)).collect()
        }
    }

    impl<T: Copy> Deref for DatabaseCell<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.value
        }
    }

    unsafe impl<T> Zeroable for DatabaseCell<T> where T: Pod {}

    unsafe impl<T> Pod for DatabaseCell<T> where T: Pod {}
    impl<T: Pod> DatabaseCell<T> {
        pub fn get(&self, context: &DatabaseContext) -> T {
            context.observe_registrar(self.registrar);
            self.value
        }
        pub fn get_ref(&self, context: &DatabaseContext) -> &T {
            context.observe_registrar(self.registrar);
            &self.value
        }
        pub fn set<'a>(&'a mut self, context: &'a DatabaseContext, new_value: T) {
            let index = context.index_of(self);
            //context.write(index, bytes_of(&new_value));
            context.write_pod(new_value, &mut self.value);
            context.update_registrar(&mut self.registrar, false);
        }
    }

    impl<T: Pod> Object for OpaqueCell<T> {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl<T: Pod> OpaqueCell<T> {
        pub fn set<'a>(&'a mut self, context: &'a DatabaseContext, new_value: T) {
            let index = context.index_of(self);
            //context.write(index, bytes_of(&new_value));
            context.write_pod(new_value, &mut self.value);
            context.update_registrar(&mut self.registrar, true);
        }
    }

    impl<T: Pod> DatabaseCell<T> {
        #[allow(clippy::mut_from_ref)]
        pub fn allocate(context: &DatabaseContext) -> &mut Self {
            let memory = unsafe { context.allocate_pod::<DatabaseCell<T>>() };
            unsafe { &mut *(memory as *mut _ as *mut DatabaseCell<T>) }
        }
        pub fn new(value: T) -> DatabaseCell<T> {
            DatabaseCell {
                value,
                registrar: Default::default(),
            }
        }
    }

    impl<T: Pod> Object for DatabaseCell<T> {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }
    #[repr(C)]

    pub struct RawDatabaseVec<T> {
        length: usize,
        capacity: usize,
        data: usize,
        phantom_data: PhantomData<T>,
    }
    unsafe impl<T> Zeroable for RawDatabaseVec<T> {}

    impl<T> Copy for RawDatabaseVec<T> {}

    impl<T> Default for RawDatabaseVec<T> {
        fn default() -> Self {
            Self {
                length: 0,
                capacity: 0,
                data: 0,
                phantom_data: Default::default(),
            }
        }
    }

    impl<T> Clone for RawDatabaseVec<T> {
        fn clone(&self) -> Self {
            *self
        }
    }

    impl<T> Debug for RawDatabaseVec<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RawDatabaseVec({})", self.length)
        }
    }
    unsafe impl<T> Pod for RawDatabaseVec<T> where T: 'static {}

    impl<T: 'static> RawDatabaseVec<T> {
        fn realloc_add(&mut self, ctx: &DatabaseContext, new_capacity: usize, new_len: usize) {
            debug_assert!(new_capacity >= new_len);
            debug_assert!(new_capacity >= self.capacity);
            debug_assert!(new_len >= self.length);

            let dest = ctx.allocate_raw(new_capacity * size_of::<T>(), align_of::<T>());
            let dest_index = ctx.index_of_ptr(dest);

            if self.length > 0 {
                let old_ptr = FatPtr::from(self.data, size_of::<T>() * self.length);
                ctx.copy(old_ptr, dest_index.0);
            }

            let new_len = new_len;

            ctx.write_pod(
                RawDatabaseVec {
                    length: new_len,
                    capacity: new_capacity,
                    data: dest_index.0,
                    phantom_data: Default::default(),
                },
                self,
            )
        }
        pub fn len(&self) -> usize {
            self.length
        }
        pub fn grow(&mut self, ctx: &DatabaseContext, new_length: usize) {
            if new_length <= self.length {
                return;
            }
            if self.capacity < new_length {
                self.realloc_add(ctx, 2 * new_length, new_length);
            } else {
                ctx.write_pod(new_length, &mut self.length);
            }
        }
        #[allow(clippy::mut_from_ref)]
        pub fn new(ctx: &DatabaseContext) -> &mut DatabaseVec<T> {
            unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
        }
    }
    impl<T: Pod + 'static> RawDatabaseVec<T> {
        pub(crate) fn get_slice(&self, context: &DatabaseContext, range: Range<usize>) -> &[T] {
            let offset = self.data + range.start * size_of::<T>();
            let len = range.end - range.start;

            unsafe { context.access_slice_at(offset, len) }
        }
        pub(crate) fn get_full_slice_mut(&self, context: &DatabaseContext) -> &mut [T] {
            let offset = self.data;
            unsafe { context.access_slice_at_mut(offset, self.length) }
        }
        pub(crate) fn get_full_slice(&self, context: &DatabaseContext) -> &[T] {
            let offset = self.data;
            unsafe { context.access_slice_at(offset, self.length) }
        }
        pub(crate) fn get_slice_mut(
            &self,
            context: &DatabaseContext,
            range: Range<usize>,
        ) -> &mut [T] {
            let offset = self.data + range.start * size_of::<T>();
            let len = range.end - range.start;

            unsafe { context.access_slice_at_mut(offset, len) }
        }
        pub(crate) fn get(&self, ctx: &DatabaseContext, index: usize) -> &T {
            assert!(index < self.length);
            let offset = self.data + index * size_of::<T>();
            unsafe { ctx.access_pod(ThinPtr(offset)) }
        }
        pub(crate) fn get_mut(&self, ctx: &DatabaseContext, index: usize) -> &mut T {
            assert!(index < self.length);
            let offset = self.data + index * size_of::<T>();
            let t = unsafe { ctx.access_pod_mut(ThinPtr(offset)) };
            t
        }
        pub(crate) fn write_untracked(&mut self, ctx: &DatabaseContext, index: usize, val: T) {
            let offset = self.data + index * size_of::<T>();
            unsafe {
                ctx.write_pod(val, ctx.access_pod_mut(ThinPtr(offset)));
            };
        }
        pub(crate) fn push_untracked<'a>(&'a mut self, ctx: &DatabaseContext, t: T) -> ThinPtr
        where
            T: Pod,
        {
            if self.length >= self.capacity {
                self.realloc_add(ctx, (self.capacity + 1) * 2, self.length + 1);
            } else {
                ctx.write_pod(self.length + 1, &mut self.length);
            }

            self.write_untracked(ctx, self.length - 1, t);
            let offset = self.data + (self.length - 1) * size_of::<T>();
            ThinPtr(offset)
        }
    }
    impl<T> Object for RawDatabaseVec<T>
    where
        T: FixedSizeObject + 'static,
    {
        type Ptr = ThinPtr;
        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }
        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    #[repr(C)]
    pub struct DatabaseVec<T> {
        length: usize,
        capacity: usize,
        data: usize,
        length_registrar: SequenceNr,
        phantom_data: PhantomData<T>,
    }

    unsafe impl<T> Zeroable for DatabaseVec<T> {}

    impl<T> Copy for DatabaseVec<T> {}

    impl<T> Clone for DatabaseVec<T> {
        fn clone(&self) -> Self {
            *self
        }
    }

    unsafe impl<T> Pod for DatabaseVec<T> where T: 'static {}

    impl<T: 'static> DatabaseVec<T> {
        fn realloc_add(&mut self, ctx: &mut DatabaseContext, new_capacity: usize, new_len: usize) {
            debug_assert!(new_capacity >= new_len);
            debug_assert!(new_capacity >= self.capacity);
            debug_assert!(new_len >= self.length);
            let dest = ctx.allocate_raw(new_capacity * size_of::<T>(), align_of::<T>());
            let dest_index = ctx.index_of_ptr(dest);

            if self.length > 0 {
                let old_ptr = FatPtr::from(self.data, size_of::<T>() * self.length);
                ctx.copy(old_ptr, dest_index.0);
            }

            ctx.write_pod(
                DatabaseVec {
                    length: new_len,
                    capacity: new_capacity,
                    data: dest_index.0,
                    length_registrar: SequenceNr::default(),
                    phantom_data: Default::default(),
                },
                self,
            )
        }
        #[allow(clippy::mut_from_ref)]
        pub fn new(ctx: &DatabaseContext) -> &mut DatabaseVec<T> {
            unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
        }
    }

    impl<T> DatabaseVec<T>
    where
        T: FixedSizeObject + 'static,
    {
        pub fn len(&self, ctx: &DatabaseContext) -> usize {
            ctx.observe_registrar(self.length_registrar);
            self.length
        }
        pub fn get(&self, ctx: &DatabaseContext, index: usize) -> &T {
            assert!(index < self.length);
            let offset = self.data + index * size_of::<T>();
            unsafe { T::access(ctx, ThinPtr(offset)) }
        }
        pub fn get_mut(&mut self, ctx: &mut DatabaseContext, index: usize) -> &mut T {
            assert!(index < self.length);
            let offset = self.data + index * size_of::<T>();
            let t = unsafe { T::access_mut(ctx, ThinPtr(offset)) };
            t
        }
        pub(crate) fn write(&mut self, ctx: &mut DatabaseContext, index: usize, val: T) {
            let offset = self.data + index * size_of::<T>();
            unsafe {
                let dest = T::access_mut(ctx, ThinPtr(offset));
                ctx.write_pod(val, dest);
            };
        }

        pub fn push_zeroed(&mut self, context: &mut DatabaseContext) -> &mut T {
            self.push(context, T::zeroed());
            self.get_mut(context, self.length - 1)
        }
        pub fn push<'a>(&'a mut self, ctx: &mut DatabaseContext, t: T) {
            if self.length >= self.capacity {
                self.realloc_add(ctx, (self.capacity + 1) * 2, self.length + 1);
            } else {
                ctx.write_pod(self.length + 1, &mut self.length);
            }
            ctx.update_registrar(&mut self.length_registrar, false);

            self.write(ctx, self.length - 1, t)
        }
    }

    impl<T> Object for DatabaseVec<T>
    where
        T: FixedSizeObject + 'static,
    {
        type Ptr = ThinPtr;
        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }
        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    #[repr(transparent)]
    pub struct DatabaseObjectHandle<T: Object + ?Sized> {
        object_index: T::Ptr,
        phantom: PhantomData<T>,
    }

    unsafe impl<T: Object + ?Sized> Zeroable for DatabaseObjectHandle<T> {}

    impl<T: Object + ?Sized> Copy for DatabaseObjectHandle<T> {}

    impl<T: Object + ?Sized> Clone for DatabaseObjectHandle<T> {
        fn clone(&self) -> Self {
            *self
        }
    }

    unsafe impl<T: Object + ?Sized + 'static> Pod for DatabaseObjectHandle<T> {}
    impl<T: Object + ?Sized + 'static> Object for DatabaseObjectHandle<T> {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }
        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl<T: Object + ?Sized> DatabaseObjectHandle<T> {
        pub fn get(&self, context: &DatabaseContext) -> &T {
            unsafe { T::access(context, self.object_index) }
        }
        pub fn get_mut(&mut self, context: &mut DatabaseContext) -> &mut T {
            unsafe { T::access_mut(context, self.object_index) }
        }

        pub fn new(value: T::Ptr) -> Self {
            Self {
                object_index: value,
                phantom: Default::default(),
            }
        }

        #[allow(clippy::mut_from_ref)]
        pub fn allocate(context: &DatabaseContext, value: T) -> &mut Self
        where
            T: Object<Ptr = ThinPtr>,
            T: Pod,
        {
            let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
            let target = unsafe { context.allocate_pod::<T>() };
            *target = value;
            this.object_index = context.index_of(target);
            this
        }

        #[allow(clippy::mut_from_ref)]
        pub fn allocate_unsized<'a>(context: &'a DatabaseContext, value: &T) -> &'a mut Self
        where
            T: Object<Ptr = FatPtr> + 'static,
        {
            let size_bytes = std::mem::size_of_val(value);
            let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
            let target_dst_ptr =
                unsafe { context.allocate_raw(size_bytes, std::mem::align_of_val(value)) };

            let target_src_ptr = value as *const T as *const u8;

            let (src_ptr, src_metadata): (*const u8, usize) = unsafe { transmute_copy(&value) };

            unsafe { std::ptr::copy(target_src_ptr, target_dst_ptr, size_bytes) };
            let thin_index = context.index_of_ptr(target_dst_ptr);

            this.object_index = FatPtr::from(thin_index.start(), size_bytes);
            this
        }
    }
}

#[derive(Clone,Copy)]
enum MultiInstanceThreadBlocker {
    Idle,
    InstanceActive,
    Disabled
}


thread_local! {
    pub(crate) static MULTI_INSTANCE_BLOCKER: Cell<MultiInstanceThreadBlocker> = const { Cell::new(MultiInstanceThreadBlocker::Idle) };
}

pub unsafe  fn disable_multi_instance_blocker() {
    MULTI_INSTANCE_BLOCKER.set(MultiInstanceThreadBlocker::Disabled);
}

#[derive(Clone)]
pub enum Target {
    OpenExisting(PathBuf),
    CreateNewOrOverwrite(PathBuf),
    CreateNew(PathBuf),
}
impl Target {
    fn path_buf(&mut self) -> &mut PathBuf {
        let (Target::CreateNew(x) | Target::CreateNewOrOverwrite(x) | Target::OpenExisting(x)) =
            self;
        x
    }
    #[must_use]
    pub fn append(&self, path: &str) -> Target {
        let mut temp = self.clone();
        *temp.path_buf() = self.path().join(path);
        temp
    }
    pub fn path(&self) -> &Path {
        let (Target::CreateNew(x) | Target::CreateNewOrOverwrite(x) | Target::OpenExisting(x)) =
            self;
        x
    }
    pub fn create(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_) | Target::CreateNew(_))
    }
    pub fn overwrite(&self) -> bool {
        matches!(self, Target::CreateNewOrOverwrite(_))
    }
}

pub mod database {
    use crate::cutoff::CutoffHash;
    use crate::disk_abstraction::{Disk, InMemoryDisk, StandardDisk};
    use crate::disk_access::FileAccessor;
    use crate::message_store::IndexEntry;
    use crate::projector::Projector;
    use crate::sequence_nr::SequenceNr;
    use crate::update_head_tracker::UpdateHeadTracker;
    use crate::{Application, DatabaseContext, MULTI_INSTANCE_BLOCKER, Message, MessageComponent, MessageHeader, MessageId, MessagePayload, Object, Pointer, Target, MultiInstanceThreadBlocker};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime};

    pub struct Database<Base: Application> {
        context: DatabaseContext,
        message_store: Projector<Base>,
        time_override: Option<DateTime<Utc>>,
        app: Base,
    }

    //TODO: Make the modules in this file be distinct files

    impl<APP: Application> Database<APP> {
        pub(crate) fn force_rewind(&mut self, index: SequenceNr) {
            self.context.rewind(index)
        }

        pub fn contains_message(&self, message_id: MessageId) -> Result<bool> {
            self.message_store.contains_message(message_id)
        }

        pub fn get_upstream_of(
            &self,
            message_id: impl DoubleEndedIterator<Item = (MessageId, /*query count*/ usize)>,
        ) -> Result<impl Iterator<Item = (MessageHeader, /*query count*/ usize)>> {
            self.message_store.get_upstream_of(message_id)
        }

        pub fn load_message(&self, message_id: MessageId) -> Result<Message<APP::Message>> {
            self.message_store.load_message(message_id)
        }

        pub(crate) fn get_update_heads(&self) -> &[MessageId] {
            self.message_store.get_update_heads()
        }

        pub(crate) fn get_messages_at_or_after(
            &self,
            message: MessageId,
            count: usize,
        ) -> Result<Vec<MessageId>> {
            self.message_store.get_messages_after(message, count)
        }

        pub fn is_acceptable_cutoff_hash(&self, hash: CutoffHash) -> Result<bool> {
            self.message_store.is_acceptable_cutoff_hash(hash)
        }
        pub fn nominal_cutoffhash(&self) -> Result<CutoffHash> {
            self.message_store.nominal_cutoffhash()
        }

        pub fn get_all_message_ids(&self) -> Result<Vec<MessageId>> {
            self.message_store.get_all_message_ids()
        }
        pub fn get_all_messages(&self) -> Result<Vec<Message<APP::Message>>> {
            self.message_store.get_all_messages()
        }
        pub fn get_all_messages_with_children(
            &self,
        ) -> Result<Vec<(Message<APP::Message>, Vec<MessageId>)>> {
            self.message_store.get_all_messages_with_children()
        }

        pub fn get_root(&self) -> (&APP::Root, &DatabaseContext) {
            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access(&self.context, root_ptr) };
            //let root = self.context.access_pod(root_ptr);
            (root, &self.context)
        }

        pub(crate) fn now(&self) -> chrono::DateTime<Utc> {
            self.time_override.unwrap_or_else(|| Utc::now())
        }

        pub(crate) fn with_root_mut<R>(
            &mut self,
            f: impl FnOnce(&mut APP::Root, &mut DatabaseContext) -> R,
        ) -> Result<R> {
            let now = self.now();
            if !self.context.mark_dirty()? {
                // Recovery needed
                Self::recover(
                    &mut self.app,
                    &mut self.context,
                    &mut self.message_store,
                    now,
                )?;
            }

            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access_mut(&mut self.context, root_ptr) };

            let t = f(root, &mut self.context);

            self.context.mark_clean()?;
            Ok(t)
        }

        pub fn reproject(&mut self) -> Result<()> {
            let now = self.now();
            // TODO: Reduce code duplication - mark_dirty etc exists in many methods
            if !self.context.mark_dirty()? {
                // Recovery needed
                Self::recover(
                    &mut self.app,
                    &mut self.context,
                    &mut self.message_store,
                    now,
                )?;
            }

            self.message_store.rewind(&mut self.context, 0)?;

            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access_mut(&mut self.context, root_ptr) };

            self.message_store
                .apply_missing_messages(root, &mut self.context, now)?;

            self.context.mark_clean()?;
            Ok(())
        }

        fn recover(
            app: &mut APP,
            context: &mut DatabaseContext,
            message_store: &mut Projector<APP>,
            time_now: chrono::DateTime<Utc>,
        ) -> Result<()> {
            context.clear()?;

            message_store.recover();
            let mmap_ptr = context.start_ptr();
            let root_obj_ref = APP::initialize_root(context);
            let root_ptr = DatabaseContext::index_of_rel(mmap_ptr, root_obj_ref);
            context.set_root_ptr(root_ptr.as_generic());

            context.set_next_seqnr(SequenceNr::from_index(0));

            // Safety:
            // Recover is only called when the db is not used
            let root = unsafe { <APP::Root as Object>::access_mut(context, root_ptr) };
            //let root = context.access_pod(root_ptr);
            message_store.apply_missing_messages(root, context, time_now)?;

            Ok(())
        }

        /// Note: You can set max_file_size to something very large, like 100_000_000_000
        pub fn create_new(
            path: impl AsRef<Path>,
            app: APP,
            overwrite_existing: bool,
            max_file_size: usize,
            cutoff_interval: Duration,
        ) -> Result<Database<APP>> {
            Self::create(
                app,
                if overwrite_existing {
                    Target::CreateNewOrOverwrite(path.as_ref().to_path_buf())
                } else {
                    Target::CreateNew(path.as_ref().to_path_buf())
                },
                max_file_size,
                cutoff_interval,
            )
        }
        pub fn open(
            path: impl AsRef<Path>,
            app: APP,
            max_file_size: usize,
            cutoff_interval: Duration,
        ) -> Result<Database<APP>> {
            Self::create(
                app,
                Target::OpenExisting(path.as_ref().to_path_buf()),
                max_file_size,
                cutoff_interval,
            )
        }

        // TODO: We should separate the message_id from the Message-type, and let
        // Noatun provide message_id. It should be provided when adding the message, so that
        // we can be sure that all local messages are such that they haven't been observed
        // previously
        // TODO: Maybe change the signature of this, and some other public methods, to accept
        // a raw message payload, and hide messageId-generation from user!
        pub fn append_single(&mut self, message: Message<APP::Message>, local: bool) -> Result<()> {
            self.append_many(std::iter::once(message), local)
        }

        /// Set the current time to the given value.
        /// This does not update the system time, it only affects the time for Noatun.
        pub fn set_mock_time(&mut self, time: DateTime<Utc>) {
            self.time_override = Some(time);
        }

        pub fn mark_transmitted(&mut self, message_id: MessageId) -> Result<()> {
            self.message_store.mark_transmitted(message_id)
        }

        pub fn append_local(&mut self, message: APP::Message) -> Result<()> {
            let now = self.now();
            println!("Now: {:?}", now);
            let new_id = MessageId::generate_for_time(now)?;
            println!("New id: {:?}", new_id);
            let t = Message::new(
                new_id,self.get_update_heads().to_vec(),
                message
            );
            self.append_single(t, true)?;
            Ok(())
        }

        pub fn append_many(
            &mut self,
            messages: impl Iterator<Item = Message<APP::Message>>,
            local: bool
        ) -> Result<()> {
            let now = self.now();
            if !self.context.mark_dirty()? {
                // Recovery needed
                Self::recover(
                    &mut self.app,
                    &mut self.context,
                    &mut self.message_store,
                    now,
                )?;
            }


            self.message_store
                .push_messages(&mut self.context, messages, local);


            let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
            let root = unsafe { <APP::Root as Object>::access_mut(&mut self.context, root_ptr) };

            self.message_store
                .apply_missing_messages(root, &mut self.context, now)?;

            self.context.mark_clean();
            Ok(())
        }

        fn set_multi_instance_block() {
            #[cfg(not(test))]
            {
                match MULTI_INSTANCE_BLOCKER.get() {
                    MultiInstanceThreadBlocker::Idle => {
                        MULTI_INSTANCE_BLOCKER.set(MultiInstanceThreadBlocker::InstanceActive);
                    }
                    MultiInstanceThreadBlocker::InstanceActive => {
                        panic!(
                            "Noatun: Multiple active DB-roots in the same thread are not allowed.\n\
                        You can disable this diagnostic by calling the unsafe method disable_multi_instance_blocker().\n\
                        Note, unsoundness can then occur if DatabaseContext from one instance is used by data \
                        for other."
                        );
                    }
                    MultiInstanceThreadBlocker::Disabled => {}
                }
            }
        }

        /// Create a database residing entirely in memory.
        /// This is mostly useful for tests
        // TODO: Use builder pattern?
        pub fn create_in_memory(
            mut app: APP,
            max_size: usize,
            cutoff_interval: Duration,
            mock_time: Option<chrono::DateTime<Utc>>,
        ) -> Result<Database<APP>> {
            Self::set_multi_instance_block();
            let mut disk = InMemoryDisk::default();
            let target = Target::CreateNew(PathBuf::default());
            let mut ctx = DatabaseContext::new(&mut disk, &target, max_size)
                .context("creating database in memory")?;
            let mut message_store = Projector::new(&mut disk, &target, max_size, cutoff_interval)?;

            Self::recover(
                &mut app,
                &mut ctx,
                &mut message_store,
                mock_time.unwrap_or_else(|| Utc::now()),
            )?;
            ctx.mark_clean()?;

            Ok(Database {
                context: ctx,
                app,
                message_store,
                time_override: mock_time,
            })
        }

        fn create(
            mut app: APP,
            target: Target,
            max_file_size: usize,
            cutoff_interval: Duration,
        ) -> Result<Database<APP>> {
            Self::set_multi_instance_block();
            let mut disk = StandardDisk;

            let mut ctx = DatabaseContext::new(&mut disk, &target, max_file_size)
                .context("opening database")?;

            let is_dirty = ctx.is_dirty();

            let mut message_store =
                Projector::new(&mut disk, &target, max_file_size, cutoff_interval)?;
            let mut update_heads = disk.open_file(&target, "update_heads", 0, 128 * 1024 * 1024)?;
            if is_dirty {
                Self::recover(&mut app, &mut ctx, &mut message_store, Utc::now())?;
                ctx.mark_clean()?;
            }
            Ok(Database {
                context: ctx,
                app,
                message_store,
                time_override: None,
            })
        }
    }
    impl<APP: Application> Drop for Database<APP> {
        fn drop(&mut self) {
            match MULTI_INSTANCE_BLOCKER.get() {
                MultiInstanceThreadBlocker::InstanceActive => {
                    MULTI_INSTANCE_BLOCKER.set(MultiInstanceThreadBlocker::Idle);
                }
                MultiInstanceThreadBlocker::Idle => {
                    eprintln!("Unexpected condition: MultiInstanceThreadBlocker was Idle, though we were running.");
                }
                MultiInstanceThreadBlocker::Disabled => {}
            }
        }
    }
}

pub mod distributor {
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
                parents: m.header.parents,
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
                database.get_update_heads().into_iter().copied().collect(),
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
                            let accum_count =
                                accumulated_upstream_queries.entry(msg).or_insert(0usize);
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
                    println!(
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
                    if child_msg
                        .header
                        .parents
                        .iter()
                        .find(|x| **x == msg_id)
                        .is_some()
                    {
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
                        eprintln!("Message could not be deserialized: {:?}", x);
                        None
                    }
                })
                .inspect(|x| {
                    println!(
                        "==========================================\nAppend message: {:?}",
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DatabaseCellArrayExt;
    use crate::disk_access::FileAccessor;
    use crate::distributor::DistributorMessage;
    use crate::projection_store::{MainDbAuxHeader, MainDbHeader};
    use crate::sequence_nr::SequenceNr;
    use byteorder::{LittleEndian, WriteBytesExt};
    use chrono::{NaiveDate, Utc};
    use data_types::DatabaseCell;
    use data_types::DatabaseObjectHandle;
    use data_types::DatabaseVec;
    use database::Database;
    use datetime_literal::datetime;
    use savefile::{load_noschema, save_noschema};
    use savefile_derive::Savefile;
    use sha2::{Digest, Sha256};
    use std::io::{Cursor, SeekFrom};
    use std::iter::once;
    use test::Bencher;

    #[test]
    fn test_mmap_big() {
        let mut mmap = FileAccessor::new(
            &Target::CreateNewOrOverwrite("test/mmap_test_big".into()),
            "mmap",
            0,
            1024 * 1024 * 1024,
        );
        //use std::io::Read;
        //let _ =  std::io::stdin().read(&mut [0u8]).unwrap();
    }

    #[test]
    fn test_mmap_helper() {
        let mut mmap = FileAccessor::new(
            &Target::CreateNewOrOverwrite("test/mmap_test1".into()),
            "mmap",
            0,
            16 * 1024 * 1024,
        )
        .unwrap();
        mmap.write_u32::<LittleEndian>(0x2b).unwrap();
        use byteorder::ReadBytesExt;
        use std::io::Read;
        use std::io::Seek;
        mmap.seek(SeekFrom::Start(12)).unwrap();
        mmap.write_u64::<LittleEndian>(0x2c).unwrap();
        mmap.seek(SeekFrom::Start(12)).unwrap();
        let initial_ptr = mmap.map_mut_ptr();
        assert_eq!(mmap.read_u64::<LittleEndian>().unwrap(), 0x2c);

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        mmap.write_u8(1).unwrap();
        assert_eq!(initial_ptr, mmap.map_mut_ptr());

        mmap.seek(SeekFrom::Start(3000_000)).unwrap();
        assert_eq!(mmap.read_u8().unwrap(), 1);

        mmap.flush_all().unwrap();

        mmap.truncate(0).unwrap();
        mmap.seek(SeekFrom::Start(0)).unwrap();

        let mut buf = [0];
        let got = mmap.read(&mut buf).unwrap();
        assert_eq!(got, 0);
        mmap.write_u8(42).unwrap();
        mmap.write_u8(42).unwrap();
        mmap.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(mmap.read_u8().unwrap(), 42);
    }

    struct DummyMessage<T> {
        phantom_data: PhantomData<T>,
    }
    impl<T> Debug for DummyMessage<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "DummyMessage")
        }
    }

    impl<T: Object> MessagePayload for DummyMessage<T> {
        type Root = T;


        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            unimplemented!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            unimplemented!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            unimplemented!()
        }
    }

    #[derive(Clone, Copy, Zeroable, Pod)]
    #[repr(C)]
    struct CounterObject {
        counter: DatabaseCell<u32>,
        counter2: DatabaseCell<u32>,
    }

    impl Object for CounterObject {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index) }
        }

        unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl CounterObject {
        fn set_counter(&mut self, ctx: &mut DatabaseContext, value1: u32, value2: u32) {
            self.counter.set(ctx, value1);
            self.counter2.set(ctx, value2);
        }
        fn new(ctx: &DatabaseContext) -> &mut CounterObject {
            ctx.allocate_pod()
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root = CounterObject;
        type Message = CounterMessage;

        fn initialize_root(mut ctx: &mut DatabaseContext) -> &mut Self::Root {
            let new_obj = CounterObject::new(ctx);
            new_obj
        }
    }

    #[derive(Debug)]
    struct IncrementMessage {
        increment_by: u32,
    }

    impl MessagePayload for IncrementMessage {
        type Root = CounterObject;


        fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
            unimplemented!()
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            unimplemented!()
        }

        fn serialize<W: Write>(&self, writer: W) -> Result<()> {
            unimplemented!()
        }
    }

    #[test]
    fn test1() {
        let mut db: Database<CounterApplication> = Database::create_new(
            "test/test1.bin",
            CounterApplication,
            true,
            1000,
            Duration::from_secs(1000),
        )
        .unwrap();

        db.with_root_mut(|counter, context| {
            assert_eq!(counter.counter.get(context), 0);
            counter.counter.set(context, 42);
            counter.counter2.set(context, 43);
            counter.counter.set(context, 44);

            assert_eq!(*counter.counter, 44);
            assert_eq!(counter.counter.get(context), 44);
            assert_eq!(counter.counter2.get(context), 43);
        });
    }

    #[derive(Debug, Clone, Savefile)]
    struct CounterMessage {
        id: MessageId,
        parent: Vec<MessageId>,
        inc1: i32,
        set1: u32,
    }
    impl CounterMessage {
        fn wrap(&self) -> Message<CounterMessage> {
            Message::new(self.id, self.parent.clone(), self.clone())
        }
    }
    impl MessagePayload for CounterMessage {
        type Root = CounterObject;

        fn apply(&self, context: &mut DatabaseContext, root: &mut CounterObject) {
            if self.inc1 != 0 {
                root.counter.set(
                    context,
                    root.counter.get(context).saturating_add_signed(self.inc1),
                );
            } else {
                root.counter.set(context, self.set1);
            }
        }

        fn deserialize(buf: &[u8]) -> Result<Self>
        where
            Self: Sized,
        {
            Ok(load_noschema(&mut Cursor::new(buf), 1)?)
        }

        fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
            Ok(save_noschema(&mut writer, 1, self)?)
        }
    }

    #[test]
    fn test_msg_store_real() {
        let mut db: Database<CounterApplication> = Database::create_new(
            "test/msg_store.bin",
            CounterApplication,
            true,
            10000,
            Duration::from_secs(1000),
        )
        .unwrap();

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::new_debug(0x100),
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        db.mark_transmitted(MessageId::new_debug(0x100));

        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: MessageId::new_debug(0x101),
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: MessageId::new_debug(0x102),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        println!("Update heads: {:?}", db.get_update_heads());
        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_msg_store_inmem_miri() {
        let mut db: Database<CounterApplication> = Database::create_in_memory(
            CounterApplication,
            10000,
            Duration::from_secs(1000),
            Some(datetime!(2021-01-01 Z)),
        )
        .unwrap();

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::new_debug(0x100),
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: MessageId::new_debug(0x101),
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: MessageId::new_debug(0x102),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);
        assert!(!db.contains_message(MessageId::new_debug(0x100)).unwrap());
        assert!(db.contains_message(MessageId::new_debug(0x101)).unwrap());
        assert!(db.contains_message(MessageId::new_debug(0x102)).unwrap());

        db.with_root_mut(|root, context| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_msg_store_after_cutoff_inmem_miri() {
        let mut db: Database<CounterApplication> = Database::create_in_memory(
            CounterApplication,
            10000,
            Duration::from_secs(1000),
            Some(datetime!(2024-01-01 Z)),
        )
        .unwrap();

        let m1 = MessageId::from_parts(datetime!(2024-01-01 Z), [0u8; 10]).unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![],
                id: m1,
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.mark_transmitted(m1).unwrap();
        let m2 = MessageId::from_parts(datetime!(2024-01-01 Z), [1u8; 10]).unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: m2,
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.set_mock_time(datetime!(2024-01-10 Z));
        db.reproject().unwrap();
        println!("Appending 2nd");
        let m3 = MessageId::from_parts(datetime!(2024-01-10 Z), [2u8; 10]).unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: m3,
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        assert!(!db.contains_message(m1).unwrap());
        assert!(db.contains_message(m2).unwrap());
        assert!(db.contains_message(m3).unwrap());

        db.with_root_mut(|root, context| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_cutoff_handling() {
        let mut db: Database<CounterApplication> =
            Database::create_in_memory(CounterApplication, 10000, Duration::from_secs(1000), None)
                .unwrap();

        db.append_single(
            CounterMessage {
                parent: vec![],
                id: MessageId::new_debug(0x100),
                inc1: 2,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x100)],
                id: MessageId::new_debug(0x101),
                inc1: 0,
                set1: 42,
            }
            .wrap(),
            true,
        )
        .unwrap();
        db.append_single(
            CounterMessage {
                parent: vec![MessageId::new_debug(0x101)],
                id: MessageId::new_debug(0x102),
                inc1: 1,
                set1: 0,
            }
            .wrap(),
            true,
        )
        .unwrap();

        let mut d = distributor::Distributor::new();

        println!("Heads: {:?}", d.get_periodic_message(&db));

        let r = d
            .receive_message(
                &mut db,
                std::iter::once(DistributorMessage::RequestUpstream {
                    query: vec![(MessageId::new_debug(0x102), 2)],
                }),
            )
            .unwrap();
        println!("Clarify: {:?}", r);

        // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

        db.with_root_mut(|root, context| {
            assert_eq!(root.counter.get(context), 43);
        });
    }

    #[test]
    fn test_handle() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<DatabaseCell<u32>>;
            type Message = DummyMessage<DatabaseObjectHandle<DatabaseCell<u32>>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> &mut Self::Root {
                let obj = DatabaseObjectHandle::allocate(ctx, DatabaseCell::new(43u32));
                obj
            }
        }

        let mut db: Database<HandleApplication> = Database::create_new(
            "test/test_handle.bin",
            HandleApplication,
            true,
            1000,
            Duration::from_secs(1000),
        )
        .unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(handle.get(context).get(context), 43);
    }

    #[test]
    fn test_handle_to_unsized_miri() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<[DatabaseCell<u8>]>;
            type Message = DummyMessage<DatabaseObjectHandle<[DatabaseCell<u8>]>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> &mut Self::Root {
                let obj = DatabaseObjectHandle::allocate_unsized(
                    ctx,
                    [43u8, 45].map(|x| DatabaseCell::new(x)).as_slice(),
                );
                obj
            }
        }

        let mut db: Database<HandleApplication> = Database::create_in_memory(
            HandleApplication,
            1000,
            Duration::from_secs(1000),
            Some(datetime!(2021-01-01 Z)),
        )
        .unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(handle.get(context).observe(context), &[43, 45]);
    }

    #[test]
    fn test_handle_miri() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<DatabaseCell<u32>>;
            type Message = DummyMessage<DatabaseObjectHandle<DatabaseCell<u32>>>;

            fn initialize_root(mut ctx: &mut DatabaseContext) -> &mut Self::Root {
                let obj = DatabaseObjectHandle::allocate(ctx, DatabaseCell::new(43u32));
                obj
            }
        }

        let mut db: Database<HandleApplication> = Database::create_in_memory(
            HandleApplication,
            1000,
            Duration::from_secs(1000),
            Some(datetime!(2021-01-01 Z)),
        )
        .unwrap();

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(handle.get(context).get(context), 43);

        db.with_root_mut(|root, context| {
            let a1 = root.get_mut(context);
            assert_eq!(a1.get(context), 43);
        });
    }

    #[test]
    fn test_vec0() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut Self::Root {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                obj
            }

            type Message = DummyMessage<DatabaseVec<CounterObject>>;
        }

        let mut db: Database<CounterVecApplication> = Database::create_new(
            "test/test_vec0",
            CounterVecApplication,
            true,
            10000,
            Duration::from_secs(1000),
        )
        .unwrap();
        db.with_root_mut(|counter_vec, context| {
            assert_eq!(counter_vec.len(context), 0);

            let new_element = counter_vec.push_zeroed(context);
            let new_element = counter_vec.get_mut(context, 0);

            new_element.counter.set(context, 47);
            let new_element = counter_vec.push_zeroed(context);
            new_element.counter.set(context, 48);

            assert_eq!(counter_vec.len(context), 2);

            let item = counter_vec.get_mut(context, 1);
            //let item2 = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
            //assert_eq!(*item2.counter, 48);

            for _ in 0..10 {
                let new_element = counter_vec.push_zeroed(context);
            }

            let item = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
        });
    }

    #[test]
    fn test_vec_miri0() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut Self::Root {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                obj
            }

            type Message = DummyMessage<DatabaseVec<CounterObject>>;
        }

        let mut db: Database<CounterVecApplication> = Database::create_in_memory(
            CounterVecApplication,
            10000,
            Duration::from_secs(1000),
            Some(datetime!(2021-01-01 Z)),
        )
        .unwrap();
        db.with_root_mut(|counter_vec, context| {
            assert_eq!(counter_vec.len(context), 0);

            let new_element = counter_vec.push_zeroed(context);
            let new_element = counter_vec.get_mut(context, 0);

            new_element.counter.set(context, 47);
            let new_element = counter_vec.push_zeroed(context);
            new_element.counter.set(context, 48);

            assert_eq!(counter_vec.len(context), 2);

            let item = counter_vec.get_mut(context, 1);
            //let item2 = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
            //assert_eq!(*item2.counter, 48);

            for _ in 0..10 {
                let new_element = counter_vec.push_zeroed(context);
            }

            let item = counter_vec.get_mut(context, 1);
            assert_eq!(*item.counter, 48);
        });
    }
    #[test]
    fn test_vec_undo() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut Self::Root {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                obj
            }

            type Message = DummyMessage<DatabaseVec<CounterObject>>;
        }

        let mut db: Database<CounterVecApplication> = Database::create_new(
            "test/vec_undo",
            CounterVecApplication,
            true,
            10000,
            Duration::from_secs(1000),
        )
        .unwrap();

        {
            db.with_root_mut(|counter_vec, context| {
                context.set_next_seqnr(SequenceNr::from_index(1));
                assert_eq!(counter_vec.len(context), 0);

                let new_element = counter_vec.push_zeroed(context);
                new_element.counter.set(context, 47);
                new_element.counter2.set(context, 48);

                context.set_next_seqnr(SequenceNr::from_index(2));
                assert_eq!(counter_vec.len(context), 1);
                context.set_next_seqnr(SequenceNr::from_index(3));
            });
        }

        {
            db.with_root_mut(|counter_vec, context| {
                let counter = counter_vec.get_mut(context, 0);
                counter.counter.set(context, 50);
                context.rewind(SequenceNr::from_index(2));
                assert_eq!(counter.counter.get(context), 47);
            });
        }

        db.force_rewind(SequenceNr::from_index(1));

        {
            db.with_root_mut(|counter_vec, context| {
                assert_eq!(counter_vec.len(context), 0);
            });
        }
    }

    #[bench]
    fn bench_sha256(b: &mut Bencher) {
        // write input message

        // read hash digest and consume hasher

        b.iter(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"hello world");
            hasher.finalize()
        });
    }

    mod distributor_tests {
        use crate::distributor::DistributorMessage::Message;
        use crate::distributor::{Distributor, DistributorMessage};
        use crate::tests::{CounterApplication, CounterMessage};
        use crate::{Database, MessageId};
        use chrono::DateTime;
        use chrono::Utc;
        use datetime_literal::datetime;
        use insta::assert_debug_snapshot;
        use std::iter::once;
        use std::mem::swap;
        use std::time::Duration;

        fn create_app(
            id: u64,
            msgs: impl IntoIterator<
                Item = (
                    DateTime<Utc>,
                    &'static [DateTime<Utc>],
                    i32,
                    u32,
                    bool, /*local*/
                ),
            >,
        ) -> Database<CounterApplication> {
            let mut db: Database<CounterApplication> = Database::create_in_memory(
                CounterApplication,
                10000,
                Duration::from_secs(1000),
                Some(datetime!(2021-01-01 Z)),
            )
            .unwrap();
            for (id, parents, inc1, set1, local) in msgs {
                db.append_single(
                    CounterMessage {
                        id: MessageId::from_parts_for_test(id, 0),
                        parent: parents
                            .iter()
                            .copied()
                            .map(|x| MessageId::from_parts_for_test(x, 0))
                            .collect(),
                        inc1,
                        set1,
                    }
                    .wrap(),
                    local,
                );
            }
            //println!("Messages present: {:?}", db.get_all_message_ids());
            db
        }

        #[derive(Debug)]
        struct SyncReport {
            num_messages: usize,
        }

        fn sync(dbs: Vec<Database<CounterApplication>>) -> SyncReport {
            let mut report = SyncReport { num_messages: 0 };
            let mut dbs: Vec<(Distributor, Database<_>)> =
                dbs.into_iter().map(|x| (Distributor::new(), x)).collect();
            let mut ether = vec![];
            for (db_id, (distr, db)) in dbs.iter_mut().enumerate() {
                let mut sent = distr.get_periodic_message(db).unwrap();
                assert_eq!(sent.len(), 1, "no resync is active");
                let sent = sent.pop().unwrap();

                println!("db: {:?} sent initial {:?}", db_id, sent);
                report.num_messages += 1;
                ether.push((db_id, sent));
            }
            let mut next_ether = vec![];
            loop {
                for (db_id, (distr, db)) in dbs.iter_mut().enumerate() {
                    let sent = distr
                        .receive_message(
                            db,
                            ether
                                .iter()
                                .filter(|(x_src_id, msg)| *x_src_id != db_id)
                                .map(|(_src, x)| x.clone()),
                        )
                        .unwrap();
                    report.num_messages += sent.len();
                    println!("db: {:?} sent {:?}", db_id, sent);
                    next_ether.extend(sent.into_iter().map(|x| (db_id, x)));
                }
                if next_ether.is_empty() {
                    break;
                }
                swap(&mut ether, &mut next_ether);
                next_ether.clear();
            }

            let first_set: Vec<_> = dbs[0].1.get_all_message_ids().unwrap();
            for (distr, db) in dbs.iter().skip(1) {
                assert_eq!(first_set, db.get_all_message_ids().unwrap());
            }
            report
        }
        #[test]
        #[rustfmt::skip]
        fn distributor_simple_deleted() {
            let dbs = vec![
                create_app(1,
                    [
                        (datetime!(2021-01-02 00:00:00 Z),[].as_slice(),1,0,true),
                        (datetime!(2021-01-03 00:00:00 Z),[datetime!(2021-01-02 00:00:00 Z)].as_slice(),0,7,true),
                    ]),
                create_app(2,
                        [
                        (datetime!(2021-01-04 00:00:00 Z),[].as_slice(),3,0,true),
                    ]),
            ];
            let report = sync(dbs);
            assert_eq!(report.num_messages, 10);
        }

        #[test]
        #[rustfmt::skip]
        fn distributor_simple_unsync() {
            let dbs = vec![
                create_app(1,
                    [
                        (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
                        (datetime!(2021-01-02 00:00:00 Z),[datetime!(2021-01-01 00:00:00 Z)].as_slice(),2,0,true),
                    ]),
                create_app(2,
                    [
                        (datetime!(2021-01-03 00:00:00 Z),[].as_slice(),3,0,true),
                    ]),
            ];
            let report = sync(dbs);
            assert_eq!(report.num_messages, 11);
        }

        #[test]
        #[rustfmt::skip]
        fn distributor_simple_in_sync() {
            let dbs = vec![
                create_app(1,
                    [
                        (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
                    ]),
                create_app(2,
                    [
                        (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
                    ]),
            ];
            let report = sync(dbs);
            assert_eq!(report.num_messages, 2);
        }

        #[test]
        #[rustfmt::skip]
        fn distributor_simple_almost_sync() {
            let dbs = vec![
                create_app(1,
                    [
                        (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
                        (datetime!(2021-01-02 00:00:00 Z),[datetime!(2021-01-01 00:00:00 Z)].as_slice(),2,0,true),
                    ]),
                create_app(2,
                    [
                        (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
                    ]),
            ];
            sync(dbs);
        }
        #[test]
        fn test_distributor() {
            let mut app1 = create_app(1, [(datetime!(2021-01-01 Z), [].as_slice(), 1, 0, true)]);
            let mut app2 = create_app(2, [(datetime!(2021-01-02 Z), [].as_slice(), 1, 0, true)]);

            let mut dist1 = crate::distributor::Distributor::new();
            let mut dist2 = crate::distributor::Distributor::new();

            let mut msg1 = dist1.get_periodic_message(&app1).unwrap();
            assert_eq!(msg1.len(), 1, "no resync is in progress");
            let msg1 = msg1.pop().unwrap();

            println!("dist1 sent: {:?}", msg1);
            let mut result = dist2.receive_message(&mut app2, once(msg1)).unwrap();

            println!("dist2 sent: {:?}", result);

            insta::assert_debug_snapshot!(result);
            assert_eq!(result.len(), 1);

            let mut result = dist1
                .receive_message(&mut app1, once(result.pop().unwrap()))
                .unwrap();
            println!("dist1 sent: {:?}", result);
            insta::assert_debug_snapshot!(result);
            assert_eq!(result.len(), 1);

            let mut result = dist2
                .receive_message(&mut app2, once(result.pop().unwrap()))
                .unwrap();
            println!("dist2 sent: {:?}", result);
            insta::assert_debug_snapshot!(result);

            let mut result = dist1
                .receive_message(&mut app1, once(result.pop().unwrap()))
                .unwrap();
            println!("dist1 sent: {:?}", result);
            assert!(matches!(&result[0], DistributorMessage::Message(_, false)));
            assert_eq!(result.len(), 1);

            let mut result = dist2
                .receive_message(&mut app2, once(result.pop().unwrap()))
                .unwrap();
            println!("App2 all msgs: {:?}", app2.get_all_message_ids().unwrap());
            println!("App2 update heads: {:?}", app2.get_update_heads());

            insta::assert_debug_snapshot!(app2.get_all_message_ids().unwrap());
            insta::assert_debug_snapshot!(app2.get_update_heads());
        }
    }
}
