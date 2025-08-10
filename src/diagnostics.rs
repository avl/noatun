use std::collections::VecDeque;
use tokio::time::Instant;
use crate::distributor::{Address, EphemeralNodeId};
use crate::NoatunTime;

#[derive(Clone)]
pub struct PacketRow {
    pub time: Instant,
    pub packet: String,
    pub size: usize,
}

#[derive(Clone)]
pub struct MessageRow {
    pub time: Instant,
    pub message: String,
    pub from: EphemeralNodeId,
    pub src_addr: Option<Address>,
}


#[derive(Default, Clone)]
pub struct DiagnosticsData {
    pub sent_packets: VecDeque<PacketRow>,
    pub received_packets: VecDeque<PacketRow>,
    pub sent_messages: VecDeque<MessageRow>,
    pub received_messages: VecDeque<MessageRow>,
}
