//! CommunicationDriver for UDP over tokio
use crate::communication::{
    CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket,
};
use crate::{cur_node, test_elapsed};
use anyhow::{bail, Context};
use socket2::{Domain, Protocol, Type};
use std::net::{IpAddr, SocketAddr};
use tokio::net::UdpSocket;
use tracing::{info, trace};

impl CommunicationDriver for TokioUdpDriver {
    type Receiver = UdpSocket;
    type Sender = CommunicationUdpSendSocket;
    type Endpoint = SocketAddr;

    async fn initialize(
        &mut self,
        bind_address_str: &str,
        multicast_group_str: &str,
        mtu: usize,
    ) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
        let multicast_group: SocketAddr = multicast_group_str
            .parse()
            .context(format!("parsing multicast group {multicast_group_str}. Remember to include a port number: '230.1.0.1:9876'."))?;
        let bind_address_addr: IpAddr = bind_address_str
            .parse()
            .with_context(||{
                let without_last_digit = bind_address_str.trim_end_matches(char::is_numeric);
                if without_last_digit.len() != bind_address_str.len() && without_last_digit.ends_with(":") {
                    format!("parsing listening/bind address {bind_address_str}. Note, this should be a pure ip-address, not an address + port.")
                } else {
                    format!("parsing listening/bind address {bind_address_str}")
                }
            })?;
        let bind_address = SocketAddr::new(bind_address_addr, 0);
        let send_socket = UdpSocket::bind(bind_address).await?;
        let domain;
        match (multicast_group.ip(), bind_address.ip()) {
            (IpAddr::V4(multicast_ipv4), IpAddr::V4(bind_ipv4)) => {
                info!(
                    "Joining multicast group {} on if {}",
                    multicast_ipv4, bind_ipv4
                );
                domain = Domain::IPV4;
            }
            (IpAddr::V6(multicast_ipv6), IpAddr::V6(bind_ipv6)) => {
                info!(
                    "Joining multicast group {} on if {}",
                    multicast_ipv6, bind_ipv6
                );
                domain = Domain::IPV6
            }
            _ => {
                panic!(
                    "Bind address and multicast group used different address family. They must both be ipv4 or both ipv6."
                );
            }
        }

        let udp_receive = socket2::Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
        if mtu >= u16::MAX as usize {
            bail!("Maximum MTU supported by noatun is 65534");
        }
        info!("Binding to group {:?}", multicast_group);
        let receive_socket;
        match (multicast_group.ip(), bind_address) {
            (IpAddr::V4(multicast_ipv4), SocketAddr::V4(bind_ipv4)) => {
                udp_receive.set_reuse_address(true)?;
                udp_receive.set_nonblocking(true)?;
                udp_receive.bind(&multicast_group.into())?;
                receive_socket = UdpSocket::from_std(udp_receive.into())?;
                receive_socket.join_multicast_v4(multicast_ipv4, *bind_ipv4.ip())?;
                receive_socket.set_multicast_loop_v4(true)?;
            }
            (IpAddr::V6(multicast_ipv6), SocketAddr::V6(bind_ipv6)) => {
                udp_receive.set_reuse_address(true)?;
                udp_receive.set_nonblocking(true)?;
                udp_receive.set_multicast_loop_v6(true)?;
                udp_receive
                    .bind(&multicast_group.into())
                    .context("binding multicast group")?;
                udp_receive.join_multicast_v6(&multicast_ipv6, bind_ipv6.scope_id())?;

                receive_socket = UdpSocket::from_std(udp_receive.into())?;
            }
            _ => {
                unreachable!()
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
        s.parse().context(format!("couldn't parse {s:?}"))
    }
}

/// Noatun communication driver using UDP multicast, powered by tokio
pub struct TokioUdpDriver;

/// Send half for [`TokioUdpDriver`]
pub struct CommunicationUdpSendSocket {
    multicast_addr: SocketAddr,
    socket: UdpSocket,
}
impl CommunicationSendSocket<SocketAddr> for CommunicationUdpSendSocket {
    fn local_addr(&self) -> anyhow::Result<Option<SocketAddr>> {
        Ok(Some(self.socket.local_addr()?))
    }

    async fn send_to(&mut self, buf: &[u8]) -> std::io::Result<()> {
        trace!(
            "#{} {:?} Sent {} bytes",
            cur_node(),
            test_elapsed(),
            buf.len()
        );
        let res = UdpSocket::send_to(&self.socket, buf, self.multicast_addr).await;
        match res {
            Ok(sent_size) => {
                if sent_size != buf.len() {
                    return Err(std::io::Error::other(format!(
                        "Packet send failure. Expected to send {}, sent {sent_size}",
                        buf.len()
                    )));
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

impl CommunicationReceiveSocket<SocketAddr> for tokio::net::UdpSocket {
    async fn recv_buf_from<B: bytes::BufMut + Send>(
        &mut self,
        buf: &mut B,
    ) -> std::io::Result<(usize, Option<SocketAddr>)> {
        let (size, addr) = UdpSocket::recv_buf_from(self, buf).await?;
        trace!(
            "#{} {:?} Received {} bytes",
            cur_node(),
            test_elapsed(),
            size
        );
        Ok((size, Some(addr)))
    }
}
