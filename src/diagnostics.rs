//! Module containing data types used by the diagnostics feature of noatun.
//!
//! See [`crate::communication::DatabaseCommunication::diagnostics_data`]
use crate::distributor::{Address, EphemeralNodeId};
use crate::noatun_instant::Instant;
use std::collections::VecDeque;

/// Information about a sent or received packet.
///
/// This is a low-level network protocol packet. For example, when UDP is
/// used as network backend, each instance of `PacketRow` will correspond
/// to a UDP packet.
#[derive(Clone)]
pub struct PacketRow {
    /// The time of transmission or reception
    pub time: Instant,
    /// A string representation of the data packet
    pub packet: String,
    /// The size of the packet in bytes
    pub size: usize,
}

/// A communication message. This is on a protocol level higher than [`PacketRow`].
///
/// For example, if a user message contains 100kB of data, and thus needs multiple
/// UDP packets for transmission on a UDP network, that message is still carried by a single
/// communication message.
#[derive(Clone)]
pub struct MessageRow {
    /// The time of transmission or reception
    pub time: Instant,
    /// A string representation of the message
    pub message: String,
    /// The ephemeral node id of the transmitting node
    pub from: EphemeralNodeId,
    /// The src address of the node that transmitted this message, if known
    pub src_addr: Option<Address>,
}

/// Helper type containing various diagnostics data
#[derive(Clone)]
pub struct DiagnosticsData {
    /// Sent network packets
    pub sent_packets: VecDeque<PacketRow>,
    /// Received network packets
    pub received_packets: VecDeque<PacketRow>,
    /// Sent communication messages
    pub sent_messages: VecDeque<MessageRow>,
    /// Received communication messages
    pub received_messages: VecDeque<MessageRow>,
    /// The max number of retained message in any of the other fields in this type.
    pub packet_limit: usize,
}

impl Default for DiagnosticsData {
    fn default() -> Self {
        Self {
            sent_packets: Default::default(),
            received_packets: Default::default(),
            sent_messages: Default::default(),
            received_messages: Default::default(),
            packet_limit: 100,
        }
    }
}
