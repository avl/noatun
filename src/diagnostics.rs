use crate::distributor::{Address, EphemeralNodeId};
use std::collections::VecDeque;
use tokio::time::Instant;

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

#[derive(Clone)]
pub struct DiagnosticsData {
    pub sent_packets: VecDeque<PacketRow>,
    pub received_packets: VecDeque<PacketRow>,
    pub sent_messages: VecDeque<MessageRow>,
    pub received_messages: VecDeque<MessageRow>,
    pub packet_limit: usize,
}

impl Default for DiagnosticsData {
    fn default() -> Self {
        Self {
            sent_packets: Default::default(),
            received_packets: Default::default(),
            sent_messages: Default::default(),
            received_messages: Default::default(),
            packet_limit: 20,
        }
    }
}
