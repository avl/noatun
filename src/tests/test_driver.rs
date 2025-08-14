use crate::colors::colored_int;
use crate::communication::{
    CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket, DebugEvent,
    DebugEventMsg,
};
use crate::tests::all_up_sync_test::MY_THREAD_RNG;
use arcshift::ArcShift;
use bytes::BufMut;
use rand::distributions::uniform::SampleUniform;
use rand::Rng;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;

#[derive(Clone, Default)]
pub(crate) struct Partitionings {
    partitionings: HashSet<(u8, u8)>,
}
#[derive(Clone)]
pub(crate) struct TestDriverInner {
    unallocated_rx: Arc<Mutex<Vec<Option<Receiver<(u8, Vec<u8>)>>>>>,
    senders: Vec<Sender<(u8 /*src*/, Vec<u8>)>>,
    sender_count: usize,
    raw_messages: Arc<
        Mutex<
            Vec<(
                Instant,
                u8, /*src*/
                /*data:*/ Vec<u8>,
                /*delivered to:*/ Vec<u8>,
            )>,
        >,
    >,
    loss: f32,
}

impl TestDriverInner {
    fn new(size: usize) -> Self {
        let mut senders = vec![];
        let mut receivers = vec![];
        for _ in 0..size {
            let (tx, rx) = tokio::sync::mpsc::channel(100);
            senders.push(tx);
            receivers.push(Some(rx));
        }
        TestDriverInner {
            unallocated_rx: Arc::new(Mutex::new(receivers)),
            senders,
            sender_count: 0,
            raw_messages: Arc::new(Mutex::new(vec![])),
            loss: 0.0,
        }
    }
}

pub(crate) struct TestDriver {
    pub(crate) driver_start: Instant,
    pub(crate) senders: ArcShift<TestDriverInner>,
    pub(crate) partitionings: ArcShift<Partitionings>,
    pub(crate) debug_events: Arc<Mutex<Vec<DebugEvent>>>,
}
impl TestDriver {
    pub fn new(size: usize) -> TestDriver {
        TestDriver {
            driver_start: Instant::now(),
            senders: ArcShift::new(TestDriverInner::new(size)),
            partitionings: Default::default(),
            debug_events: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn partition_all(&mut self) {
        let num_nodes = self.senders.get().senders.len();
        self.partitionings.rcu(|prev| {
            let mut res: Partitionings = prev.clone();
            for i in 0..num_nodes {
                for j in 0..num_nodes {
                    let i = i as u8;
                    let j = j as u8;
                    res.partitionings.insert((i, j));
                    res.partitionings.insert((j, i));
                }
            }
            res
        });
    }
    pub fn partition_node(&mut self, node: u8) {
        let num_nodes = self.senders.get().senders.len();
        self.partitionings.rcu(|prev| {
            let mut res: Partitionings = prev.clone();
            for i in 0..num_nodes as u8 {
                if i == node {
                    continue;
                }
                res.partitionings.insert((i, node));
                res.partitionings.insert((node, i));
            }
            res
        });
    }
    pub fn unpartition_bidirectional_links(&mut self, pairs: impl IntoIterator<Item = (u8, u8)>) {
        let vec: Vec<(u8, u8)> = pairs.into_iter().collect();
        self.partitionings.rcu(move |prev| {
            let mut res: Partitionings = prev.clone();
            for (a, b) in vec.iter().copied() {
                res.partitionings.remove(&(a, b));
                res.partitionings.remove(&(b, a));
            }
            res
        });
    }

    pub fn unpartition_node(&mut self, node: u8) {
        let num_nodes = self.senders.get().senders.len();
        self.partitionings.rcu(|prev| {
            let mut res: Partitionings = prev.clone();
            for i in 0..num_nodes as u8 {
                if i == node {
                    continue;
                }
                res.partitionings.remove(&(i, node));
                res.partitionings.remove(&(node, i));
            }
            res
        });
    }
    pub fn raw_frames_snapshot(&mut self) -> String {
        let mut ret = String::new();
        use std::fmt::Write;

        writeln!(&mut ret, "TIME:       SRC: DST:         Len  Data").unwrap();
        for (t, src, msg, dst) in self.senders.get().raw_messages.lock().unwrap().iter() {
            use itertools::Itertools;

            let data: crate::communication::NetworkPacket =
                savefile::Deserializer::bare_deserialize(&mut std::io::Cursor::new(msg), 0)
                    .unwrap();

            let rawlen = dst.iter().map(|x| format!("{x}")).join(",");
            let padcount = 12usize.saturating_sub(rawlen.len());
            let padding = " ".repeat(padcount);

            writeln!(
                &mut ret,
                "{:>10?}: {}    {}{} {} B {:?}",
                t.duration_since(self.driver_start),
                colored_int((*src).into()),
                dst.iter().map(|x| colored_int((*x).into())).join(","),
                padding,
                msg.len(),
                data
            )
            .unwrap();
        }

        ret
    }
    pub fn sent_messages_count(&self) -> usize {
        self.debug_events
            .lock()
            .unwrap()
            .iter()
            .filter(|x| matches!(&x.msg, DebugEventMsg::Send(_)))
            .count()
    }
    pub fn messages_snapshot(&self) -> String {
        self.messages_snapshot_impl(false)
    }
    pub fn sent_messages_snapshot(&self) -> String {
        self.messages_snapshot_impl(true)
    }
    pub fn messages_snapshot_impl(&self, sent_only: bool) -> String {
        let mut ret = String::new();

        let mut evs = self.debug_events.lock().unwrap();
        evs.sort_by_key(|x| x.time);

        let mut receive_of = vec![None; evs.len()];
        for (receive_index, receive_ev) in evs.iter().enumerate().rev() {
            if let DebugEventMsg::Receive(_) = &receive_ev.msg {
                if let Some((send_index, send_ev)) = evs
                    .iter()
                    .enumerate()
                    .take(receive_index)
                    .rev()
                    .find(|(_send_index, send_ev)| send_ev.is_send_of(receive_ev))
                {
                    if send_ev.is_send_of(receive_ev) {
                        receive_of[send_index] = Some(receive_index);
                    }
                }
            }
        }

        for (index, ev) in evs.iter().enumerate() {
            use std::fmt::Write;
            let node: u32 = ev.node.parse().unwrap();
            let elapsed = ev.time.duration_since(self.driver_start);
            let lost = match &ev.msg {
                DebugEventMsg::Send(_) => {
                    if let Some(receive_index) = receive_of[index] {
                        format!("{:?}", evs[receive_index].time.duration_since(ev.time))
                    } else {
                        "*".to_string()
                    }
                }
                DebugEventMsg::Receive(_) => {
                    if sent_only {
                        continue;
                    }
                    String::new()
                }
            };
            writeln!(
                &mut ret,
                "{elapsed:>8?}: #{}:{lost:>8} {:?}",
                colored_int(node),
                ev.msg
            )
            .unwrap();
        }
        ret = ret.replace("2020-01-01T00:", "*");

        ret
    }
    pub fn set_loss(&mut self, loss: f32) {
        self.senders.rcu(|item| {
            let mut cloned = item.clone();
            cloned.loss = loss;
            cloned
        });
    }
}
pub(crate) struct TestDriverReceiver(Receiver<(u8 /*src*/, Vec<u8>)>, /*own id*/ u8);
pub(crate) struct TestDriverSender(
    u8, /*own addr*/
    ArcShift<TestDriverInner>,
    ArcShift<Partitionings>,
);

impl CommunicationReceiveSocket<u8> for TestDriverReceiver {
    async fn recv_buf_from<B: BufMut + Send>(
        &mut self,
        buf: &mut B,
    ) -> std::io::Result<(usize, Option<u8>)> {
        loop {
            let (src_addr, data) = self.0.recv().await.ok_or(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "all senders have gone away",
            ))?;

            if src_addr == self.1 {
                continue;
            }

            buf.put(&*data);
            return Ok((data.len(), Some(src_addr)));
        }
    }
}

pub(crate) fn random<T: SampleUniform + PartialOrd>(range: std::ops::Range<T>) -> T {
    MY_THREAD_RNG.with(|rng| rng.borrow_mut().as_mut().unwrap().gen_range(range))
}

impl CommunicationSendSocket<u8> for TestDriverSender {
    fn local_addr(&self) -> anyhow::Result<Option<u8>> {
        Ok(Some(self.0))
    }

    async fn send_to(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let driver_inner = self.1.get();
        let data = buf.to_vec();
        let mut delivered_to = vec![];
        let partitionings = self.2.get();
        for (dst_node, item) in driver_inner.senders.iter().enumerate() {
            let partitioned = partitionings
                .partitionings
                .contains(&(self.0, dst_node as u8));
            if driver_inner.loss <= random(0.0..1.0) && !partitioned {
                item.send((self.0 /*src*/, data.clone()))
                    .await
                    .map_err(|e| std::io::Error::other(format!("simulated net failed {e:?}")))?;
                delivered_to.push(dst_node as u8);
            }
        }
        driver_inner.raw_messages.lock().unwrap().push((
            Instant::now(),
            self.0,
            data.clone(),
            delivered_to,
        ));
        Ok(())
    }
}

impl CommunicationDriver for TestDriver {
    type Receiver = TestDriverReceiver;
    type Sender = TestDriverSender;
    type Endpoint = u8;

    async fn initialize(
        &mut self,
        _bind_address: &str,
        _multicast_group: &str,
        _mtu: usize,
    ) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
        //let (tx, rx) = tokio::sync::mpsc::channel(100);

        let mut rx = None;
        let mut own_id = None;
        self.senders.rcu(|prev| {
            let mut senders = prev.clone();

            let mut unallocated = senders.unallocated_rx.lock().unwrap();

            if senders.sender_count < unallocated.len() {
                rx = Some(unallocated[senders.sender_count].take().unwrap());
            } else {
                let (tx, temp_rx) = tokio::sync::mpsc::channel(100);
                rx = Some(temp_rx);
                senders.senders.push(tx.clone());
            }
            own_id = Some(senders.sender_count);
            senders.sender_count += 1;
            drop(unallocated);

            senders
        });

        let own_id = own_id.unwrap() as u8;
        Ok((
            TestDriverSender(own_id, self.senders.clone(), self.partitionings.clone()),
            TestDriverReceiver(rx.unwrap(), own_id),
        ))
    }

    fn parse_endpoint(s: &str) -> anyhow::Result<Self::Endpoint> {
        Ok(s.parse()?)
    }
}
