#![allow(clippy::needless_range_loop)]
use crate::colors::colored_int;
use crate::communication::{
    CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket,
    DatabaseCommunication, DatabaseCommunicationConfig, DebugEvent, DebugEventMsg,
};
use crate::cutoff::{CutOffDuration, CutoffHash};
use crate::database::DatabaseSettings;
use crate::distributor::Status;
use crate::tests::setup_tracing;
use crate::{
    set_test_epoch, test_elapsed, Application, Database, Message, MessageId, NoatunTime, Object,
};
use crate::{Persistence, Savefile};
use arcshift::ArcShift;
use bytes::BufMut;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use datetime_literal::datetime;
use indexmap::IndexSet;
use insta::assert_snapshot;
use rand::distributions::uniform::SampleUniform;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::Write;
use std::ops::Add;
use std::ops::Sub;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;
use tracing::info;

thread_local! {
    pub static MY_THREAD_RNG: RefCell<Option<SmallRng>> = const { RefCell::new(None) };
}

noatun_object!(
    #[derive(PartialEq)]
    struct SyncApp {
        pod counter: u32,
        pod sum: u32,
    }
);

#[derive(Debug, Savefile, PartialEq)]
pub struct SyncMessage {
    value: u32,
    reset: bool,
    persist: bool,
}

impl Message for SyncMessage {
    type Root = SyncApp;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut Self::Root>) {
        let project = root.pin_project();

        if self.reset {
            // Make sure we observe the db state, so that the non-tainted message
            // removal optimization doesn't kick in for this test

            project.counter.set(1);
            project.sum.set(1);
        } else {
            let prev_counter = project.counter.get();
            let prev_sum = project.sum.get();
            project.counter.set(prev_counter.wrapping_add(1));
            project.sum.set(prev_sum.wrapping_add(self.value));
        }
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        crate::msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        crate::msg_serialize(self, writer)
    }

    fn persistence(&self) -> Persistence {
        if self.persist {
            Persistence::AtLeastUntilCutoff
        } else {
            Persistence::UntilOverwritten
        }
    }
}

#[derive(Clone, Default)]
struct Partitionings {
    partitionings: HashSet<(u8, u8)>,
}
#[derive(Clone, Default)]
struct TestDriverInner {
    senders: Vec<(u8 /*node nr*/, Sender<(u8 /*src*/, Vec<u8>)>)>,
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

struct TestDriver {
    driver_start: Instant,
    senders: ArcShift<TestDriverInner>,
    partitionings: ArcShift<Partitionings>,
    debug_events: Arc<Mutex<Vec<DebugEvent>>>,
}
impl TestDriver {
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
        let ret = String::new();

        println!("TIME:       SRC: DST:         Len  Data");
        for (t, src, msg, dst) in self.senders.get().raw_messages.lock().unwrap().iter() {
            use itertools::Itertools;

            let data: crate::communication::NetworkPacket =
                savefile::Deserializer::bare_deserialize(&mut std::io::Cursor::new(msg), 0)
                    .unwrap();

            let rawlen = dst.iter().map(|x| format!("{}", x)).join(",");
            let padcount = 12usize.saturating_sub(rawlen.len());
            let padding = " ".repeat(padcount);

            println!(
                "{:>10?}: {}    {}{} {} B {:?}",
                t.duration_since(self.driver_start),
                colored_int((*src).into()),
                dst.iter().map(|x| colored_int((*x).into())).join(","),
                padding,
                msg.len(),
                data
            );
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
                crate::colored_int(node),
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
impl Default for TestDriver {
    fn default() -> Self {
        TestDriver {
            driver_start: Instant::now(),
            senders: ArcShift::new(TestDriverInner::default()),
            partitionings: Default::default(),
            debug_events: Arc::new(Mutex::new(vec![])),
        }
    }
}
struct TestDriverReceiver(Receiver<(u8 /*src*/, Vec<u8>)>);
struct TestDriverSender(
    u8, /*own addr*/
    ArcShift<TestDriverInner>,
    ArcShift<Partitionings>,
);

impl CommunicationReceiveSocket<u8> for TestDriverReceiver {
    async fn recv_buf_from<B: BufMut + Send>(
        &mut self,
        buf: &mut B,
    ) -> std::io::Result<(usize, u8)> {
        let (src_addr, data) = self.0.recv().await.ok_or(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "all senders have gone away",
        ))?;

        buf.put(&*data);
        Ok((data.len(), src_addr))
    }
}

impl CommunicationSendSocket<u8> for TestDriverSender {
    fn local_addr(&self) -> anyhow::Result<u8> {
        Ok(self.0)
    }

    async fn send_to(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let driver_inner = self.1.get();
        let data = buf.to_vec();
        let mut delivered_to = vec![];
        let partitionings = self.2.get();
        for (dst_node, item) in driver_inner.senders.iter() {
            let partitioned = partitionings.partitionings.contains(&(self.0, *dst_node));
            if driver_inner.loss <= random(0.0..1.0) && !partitioned {
                item.send((self.0 /*src*/, data.clone()))
                    .await
                    .map_err(|e| std::io::Error::other(format!("simulated net failed {:?}", e)))?;
                delivered_to.push(*dst_node);
            } else {
                //println!("== SIMULATOR CAUSED PACKET LOSS ==: partitioned: {}", partitioned);
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
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let mut index = None;
        self.senders.rcu(|prev| {
            let mut senders = prev.clone();
            index = Some(senders.senders.len());
            senders
                .senders
                .push((index.unwrap().try_into().unwrap(), tx.clone()));
            senders
        });

        Ok((
            TestDriverSender(
                index.unwrap().try_into().unwrap(),
                self.senders.clone(),
                self.partitionings.clone(),
            ),
            TestDriverReceiver(rx),
        ))
    }

    fn parse_endpoint(s: &str) -> anyhow::Result<Self::Endpoint> {
        Ok(s.parse()?)
    }
}

impl Application for SyncApp {
    type Message = SyncMessage;
    type Params = ();
}

const START_TIME: DateTime<Utc> = datetime!(2020-01-01 Z);

async fn create_app(
    driver: &mut TestDriver,
    modify: Option<&mut dyn FnMut(&mut Database<SyncApp>, &mut DatabaseCommunicationConfig)>,
) -> DatabaseCommunication<SyncApp> {
    let mut db: Database<SyncApp> = Database::create_in_memory(
        2_500_000,
        DatabaseSettings {
            mock_time: Some(START_TIME.into()),
            projection_time_limit: None,
            ..DatabaseSettings::default()
        },
        (),
    )
    .unwrap();

    let log = driver.debug_events.clone();
    let mut config = DatabaseCommunicationConfig {
        listen_address: "dummy".to_string(),
        multicast_address: "dummy".to_string(),
        mtu: 1500,
        bandwidth_limit_bytes_per_second: 1000,
        retransmit_interval_seconds: 1.0,
        retransmit_buffer_size_bytes: 1_000_000,
        debug_logger: Some(Box::new(move |ev| {
            let mut log = log.lock().unwrap();
            log.push(ev);
        })),
        periodic_message_interval: Duration::from_secs(5),
        initial_ephemeral_node_id: None,
        disable_retransmit: false,
    };

    if let Some(modify) = modify {
        modify(&mut db, &mut config);
    }

    let comm = DatabaseCommunication::new_custom(driver, db, config)
        .await
        .unwrap();
    comm
}

#[tokio::test(start_paused = true)]
async fn all_up_simple_sync_test() {
    set_test_epoch(Instant::now());
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));

    let mut driver = TestDriver::default();
    let app1 = create_app(&mut driver, None).await;
    let app2 = create_app(&mut driver, None).await;

    app1.add_message(SyncMessage {
        persist: false,
        value: 1,
        reset: false,
    })
    .await
    .unwrap();
    app2.add_message(SyncMessage {
        persist: false,
        value: 2,
        reset: false,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    assert_eq!(root1.sum, 3);
    assert_eq!(root2.sum, 3);
    assert_eq!(root1.counter, 2);
    assert_eq!(root2.counter, 2);
    assert_eq!(root1, root2);

    assert_snapshot!(driver.messages_snapshot());
}

fn random<T: SampleUniform + PartialOrd>(range: std::ops::Range<T>) -> T {
    MY_THREAD_RNG.with(|rng| rng.borrow_mut().as_mut().unwrap().gen_range(range))
}

#[test]
fn old_local_messages_without_effect_are_removed0() {
    let mut db: Database<SyncApp> = Database::create_in_memory(
        10000,
        DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 00:01:00 Z).into()),
            cutoff_interval: CutOffDuration::from_days(2).unwrap(), // 2 days
            ..Default::default()
        },
        (),
    )
    .unwrap();
    let mut sess = db.begin_session_mut().unwrap();
    let msg1 = sess
        .append_local(SyncMessage {
            persist: true,
            value: 1,
            reset: false,
        })
        .unwrap();
    println!("Add msg1 {:?}", msg1.id);
    println!("Cutoff-hash: {:?}", sess.current_cutoff_state().unwrap());
    sess.set_mock_time(datetime!(2020-01-01 00:01:10 Z).into())
        .unwrap();
    let _msg2 = sess
        .append_local(SyncMessage {
            persist: true,
            value: 2,
            reset: false,
        })
        .unwrap();
    println!("Add msg2 {:?}", msg1.id);
    sess.set_mock_time(datetime!(2020-01-02 00:00:00 Z).into())
        .unwrap();
    assert_eq!(sess.get_all_messages().unwrap().len(), 2);

    sess.set_mock_time(datetime!(2024-01-02 Z).into()).unwrap();
    let last = sess
        .append_local(SyncMessage {
            persist: true,
            value: 0,
            reset: true,
        })
        .unwrap();
    println!("Add msg3 {:?}", last.id);

    let all_msgs = sess.get_all_messages().unwrap();
    assert_eq!(all_msgs.len(), 1);
    assert_eq!(all_msgs[0].header.id, last.id);
    println!("Cutoff-hash: {:?}", sess.current_cutoff_state().unwrap());
}

#[test]
fn old_transmitted_messages_without_effect_are_removed1() {
    super::setup_tracing();
    let mut db: Database<SyncApp> = Database::create_in_memory(
        10000,
        DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 00:01:00 Z).into()),
            cutoff_interval:         CutOffDuration::from_days(2).unwrap(), // 2 days
            ..Default::default()
        },
        (),
    )
    .unwrap();
    let mut sess = db.begin_session_mut().unwrap();
    let msg1 = sess
        .append_local(SyncMessage {
            persist: true,
            value: 1,
            reset: false,
        })
        .unwrap();
    sess.mark_transmitted(msg1.id).unwrap();
    println!("Add msg1 {:?}", msg1.id);
    //println!("Cutoff-hash: {:?}", db.nominal_cutoff_state().unwrap());
    sess.set_mock_time(datetime!(2020-01-01 00:01:10 Z).into())
        .unwrap();
    let msg2 = sess
        .append_local(SyncMessage {
            persist: true,
            value: 0,
            reset: true,
        })
        .unwrap();
    sess.mark_transmitted(msg2.id).unwrap();
    println!("Add msg2 {:?}", msg2.id);
    assert_eq!(sess.get_all_messages().unwrap().len(), 2);

    //println!("Advancing time to 2024");
    sess.set_mock_time(datetime!(2024-01-02 Z).into()).unwrap();
    sess.maybe_advance_cutoff().unwrap();

    let all_msgs = sess.get_all_messages().unwrap();
    assert_eq!(all_msgs.len(), 1);
    assert_eq!(all_msgs[0].header.id, msg2.id);
    println!("Cutoff-hash: {:?}", sess.current_cutoff_state().unwrap());
    assert_eq!(
        sess.current_cutoff_state().unwrap().hash,
        CutoffHash::from_msg(msg2.id)
    );
}

#[test]
fn old_transmitted_messages_without_effect_are_removed2() {
    let mut db: Database<SyncApp> = Database::create_in_memory(
        100000,
        DatabaseSettings {
            mock_time: Some(START_TIME.into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();
    let mut sess = db.begin_session_mut().unwrap();

    let msg1 = sess
        .append_local(SyncMessage {
            persist: true,
            value: 1,
            reset: false,
        })
        .unwrap();
    sess.mark_transmitted(msg1.id).unwrap();
    println!("Add msg1 {:?}", msg1.id);
    //println!("Cutoff-hash: {:?}", db.nominal_cutoff_state().unwrap());
    sess.set_mock_time(datetime!(2020-01-01 01:00:00 Z).into())
        .unwrap();
    let msg2 = sess
        .append_local(SyncMessage {
            persist: true,
            value: 0,
            reset: true,
        })
        .unwrap();
    sess.mark_transmitted(msg2.id).unwrap();
    println!("Add msg2 {:?}", msg2.id);
    assert_eq!(sess.get_all_messages().unwrap().len(), 2);

    //println!("Advancing time to 2024");
    sess.set_mock_time(datetime!(2024-01-02 Z).into()).unwrap();
    sess.maybe_advance_cutoff().unwrap();

    let all_msgs = sess.get_all_messages().unwrap();
    assert_eq!(all_msgs.len(), 1);
    assert_eq!(all_msgs[0].header.id, msg2.id);
    println!("Cutoff-hash: {:?}", sess.current_cutoff_state().unwrap());
    assert_eq!(
        sess.current_cutoff_state().unwrap().hash,
        CutoffHash::from_msg(msg2.id)
    );
}

#[tokio::test(start_paused = true)]
async fn all_up_gradual_update_sync_test() {
    set_test_epoch(Instant::now());

    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let app1 = create_app(&mut driver, None).await;
    let app2 = create_app(&mut driver, None).await;

    driver.set_loss(0.5);
    let mut correct_count = 0;
    let mut correct_sum = 0;
    let start_time = tokio::time::Instant::now();
    for _i in 0..10 {
        //println!("I = {}", i);
        //println!("Msgs1: {:#?}", app1.get_all_messages().unwrap());
        assert!(app1
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        assert!(app2
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        let elapsed = start_time.elapsed();
        app1.add_message_at(
            datetime!(2020-01-01 Z)
                .add(elapsed)
                //.add(TimeDelta::seconds(random(0..100)))
                .into(),
            SyncMessage {
                value: 1,
                persist: false,
                reset: false,
            },
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        app2.add_message_at(
            datetime!(2020-01-01 Z)
                .add(TimeDelta::seconds(random(0..100)))
                .into(),
            SyncMessage {
                persist: false,
                value: 2,
                reset: false,
            },
        )
        .await
        .unwrap();
        correct_count += 2;
        correct_sum += 3;
    }
    //info!(" -------------- NETWORK HEALED -----------------");
    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(50)).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    let msgs1 = app1
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages()
        .unwrap();
    let msgs2 = app2
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages()
        .unwrap();
    assert!(msgs1.is_sorted_by_key(|x| x.header.id));
    assert!(msgs2.is_sorted_by_key(|x| x.header.id));
    //println!("Msgs 1:\n{:#?}\nMsgs 2:\n{:#?}", msgs1, msgs2);
    assert_eq!(msgs1.len(), msgs2.len());
    assert_eq!(msgs1, msgs2);
    assert_eq!(root1.sum, correct_sum);
    assert_eq!(root2.sum, correct_sum);
    assert_eq!(root1.counter, correct_count);
    assert_eq!(root2.counter, correct_count);
    assert_eq!(root1, root2);

    assert_eq!(app1.get_status().await.unwrap(), Status::Nominal);
    app1.add_message(SyncMessage {
        persist: true,
        value: 1,
        reset: false,
    })
    .await
    .unwrap();
    app1.close().await.unwrap();
    app2.close().await.unwrap();
    println!("{}", driver.messages_snapshot());
    //  assert_snapshot!(driver.messages_snapshot());
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_old_messages() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 7200, true, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages_persist() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 10, true, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_mid_age_messages_persist() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 900, true, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages_no_persist() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 10, false, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages_no_persist_no_reset() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 10, false, usize::MAX, false).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_mid_age_messages_no_persist() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 900, false, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_special_seed() {
    super::setup_tracing();
    for seed in 0..1 {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 15, false, 10, true).await;
    }
}

async fn all_up_general_update_sync_test_impl(
    seed: u64,
    max_message_age_seconds: u64,
    persist: bool,
    maxlen: usize,
    allow_reset: bool,
) {
    set_test_epoch(Instant::now());
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(seed)));

    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;

    let noatun_start_time: NoatunTime = START_TIME.into();
    let start_instant = tokio::time::Instant::now();
    let mut total_count = 0;
    let mut total_added = 0;

    #[cfg(not(debug_assertions))]
    {
        //TODO: re-enable the packet loss in this test!
        compile_error!("Don't set loss to 0 below! That invalidates the test! Should be 0.15")
    }
    driver.set_loss(0.0); //0.15
    for i in 0..MY_THREAD_RNG
        .with(|x| x.borrow_mut().as_mut().unwrap().gen_range(1..20))
        .min(maxlen)
    {
        info!("==== write {} =====", i);
        let time_now = noatun_start_time + start_instant.elapsed();
        app1.set_mock_time(time_now).unwrap();
        app2.set_mock_time(time_now).unwrap();

        assert!(app1
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        assert!(app2
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;

        {
            let my_span = tracing::span!(tracing::Level::INFO, "app1.add");
            let _e = my_span.enter();
            app1.add_message_at(
                time_now - (Duration::from_secs(random(0..max_message_age_seconds))),
                SyncMessage {
                    value: 0,
                    reset: if allow_reset {
                        MY_THREAD_RNG.with(|x| x.borrow_mut().as_mut().unwrap().gen_bool(0.3))
                    } else {
                        false
                    },
                    persist,
                },
            )
            .await
            .unwrap();
            total_count += 1;
        }
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        {
            let my_span = tracing::span!(tracing::Level::INFO, "app2.add");
            let _e = my_span.enter();
            app2.add_message_at(
                time_now - (Duration::from_secs(random(0..max_message_age_seconds))),
                SyncMessage {
                    value: 2,
                    persist,
                    reset: if allow_reset {
                        MY_THREAD_RNG.with(|x| x.borrow_mut().as_mut().unwrap().gen_bool(0.3))
                    } else {
                        false
                    },
                },
            )
            .await
            .unwrap();
            total_added += 2;
            total_count += 1;
        }
    }
    info!(" -------------- NETWORK HEALED -----------------");
    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(20)).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    let msgs1 = app1
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages()
        .unwrap();
    let msgs2 = app2
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages()
        .unwrap();
    assert!(msgs1.is_sorted_by_key(|x| x.header.id));
    assert!(msgs2.is_sorted_by_key(|x| x.header.id));
    /*let smsgs1: IndexSet<_> = msgs1.iter().map(|x| x.header.id).collect();
    let smsgs2: IndexSet<_> = msgs2.iter().map(|x| x.header.id).collect();
    println!("Cutoff time1: {:?}", app1.get_cutoff_time().unwrap());
    println!("Cutoff time2: {:?}", app2.get_cutoff_time().unwrap());
    println!("Only in 1: {:?}", smsgs1.sub(&smsgs2));
    println!("Only in 2: {:?}", smsgs2.sub(&smsgs1));*/
    if persist {
        assert_eq!(msgs1.len(), msgs2.len());
        assert_eq!(msgs1, msgs2, "Failed for seed {seed}");
    }

    assert!(root1.sum >= 1);
    assert!(root2.sum >= 1);
    assert!(root1.counter >= 1);
    assert!(root2.counter >= 1);
    if !allow_reset {
        assert_eq!(root1.sum, total_added);
        assert_eq!(root2.sum, total_added);
        assert_eq!(root1.counter, total_count);
        assert_eq!(root2.counter, total_count);
    }
    assert_eq!(root1, root2);
    //println!("Roots: {:?} {:?}", root1, root2);
    //println!("Msgs 1:\n{:#?}\nMsgs 2:\n{:#?}", msgs1, msgs2);

    let correct_root = root1;

    info!("Test case done");
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    assert_eq!(app1.get_status().await.unwrap(), Status::Nominal);
    assert_eq!(app2.get_status().await.unwrap(), Status::Nominal);
    /*app1.add_message(SyncMessage {
            value: 1,
            reset: false,
            persist,
        })
        .await
        .unwrap();
    */

    /*app1.inner_database().do_recovery().unwrap();
    app2.inner_database().do_recovery().unwrap();*/

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    assert_eq!(root1, correct_root);
    assert_eq!(root2, correct_root);

    app1.close().await.unwrap();
    app2.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn all_up_big_severely_desynced_test() {
    set_test_epoch(Instant::now());
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    for _msg in 0..100 {
        app1.add_message(SyncMessage {
            value: 1,
            persist: false,
            reset: false,
        })
        .await
        .unwrap();
        app2.add_message(SyncMessage {
            persist: false,
            value: 2,
            reset: false,
        })
        .await
        .unwrap();
        app1.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        ))
        .unwrap();
        app2.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        ))
        .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    tokio::time::sleep(Duration::from_secs(900)).await;
    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(60)).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    assert_eq!(root1, root2);
    println!("{}", driver.messages_snapshot());
}

#[tokio::test(start_paused = true)]
async fn all_up_big_nominal_test() {
    set_test_epoch(Instant::now());

    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    for msg in 0..100 {
        app1.add_message(SyncMessage {
            value: 1,
            persist: false,
            reset: false,
        })
        .await
        .unwrap();
        app2.add_message(SyncMessage {
            persist: false,
            value: 2,
            reset: false,
        })
        .await
        .unwrap();
        app1.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        ))
        .unwrap();
        app2.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        ))
        .unwrap();
        tokio::time::sleep(Duration::from_secs(120)).await;
        if msg > 15 {
            driver.set_loss(0.0);
        }
    }
    tokio::time::sleep(Duration::from_secs(120)).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    println!("{}", driver.messages_snapshot());

    let msgs1 = app1
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages()
        .unwrap();
    let msgs2 = app2
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages()
        .unwrap();
    assert!(msgs1.is_sorted_by_key(|x| x.header.id));
    assert!(msgs2.is_sorted_by_key(|x| x.header.id));
    let smsgs1: IndexSet<_> = msgs1.iter().map(|x| x.header.id).collect();
    let smsgs2: IndexSet<_> = msgs2.iter().map(|x| x.header.id).collect();
    println!("Cutoff time1: {:?}", app1.get_cutoff_time().unwrap());
    println!("Cutoff time2: {:?}", app2.get_cutoff_time().unwrap());
    println!("Only in 1: {:?}", smsgs1.sub(&smsgs2));
    println!("Only in 2: {:?}", smsgs2.sub(&smsgs1));
    {
        assert_eq!(msgs1.len(), msgs2.len());
    }

    assert_eq!(root1, root2);
}

#[tokio::test(start_paused = true)]
async fn all_up_huge_desynced_test() {
    set_test_epoch(Instant::now());
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    for _msg in 0..200 {
        app1.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        ))
        .unwrap();
        app2.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        ))
        .unwrap();
        app1.add_message(SyncMessage {
            value: 1,
            persist: false,
            reset: false,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(120)).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());
    println!("{}", driver.messages_snapshot());
    assert_snapshot!(driver.messages_snapshot());
    assert_eq!(root1, root2);
}

#[tokio::test(start_paused = true)]
async fn all_up_three_node_resync() {
    /*
        compile_error!("
    * Try to avoid sending RequestUpstream from multiple nodes
    * Automatic relay-routing-requests (unsquelch - rename?) (witrh also squelch if duplicates)
        ")
    */

    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;
    let mut app3 = create_app(&mut driver, None).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    let start_time = datetime!(2020-01-01 T 00:00:00 Z);

    for app in [&mut app1, &mut app2, &mut app3] {
        app.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    for i in 0..2 {
        app3.add_message(SyncMessage {
            value: i,
            persist: false,
            reset: false,
        })
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1000)).await;
    app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
        .unwrap();
    driver.set_loss(0.0);

    /*
        compile_error!("

    Document the following behavior:

    * When do we inhibit sending retransmit requests on the packet level (i.e, the low-level retransmit)

    * When do we inhibit sending the various retransmit-requests on the Message-layer
     - Response to RequestUpstream
     - Response to SendMessageAndAllDescendants

    * Calmly observe the written documentation. What can be made simpler? Do we even need the
    heuristic, couldn't  we just delay a random amount based on the number of neighbors, combined
    with inhibiting sending stuff we've just seen sent anyway?

    * Implement Unsquelch/squelch based on detecting when a node emits an object with a parent
    we don't know, where we previously considered ourselves to be up-to-date with the node (i.e,
    initial sync complete). I.e, when the first sync-process started, completes. We then take
    note whenever we receive a Message with a parent unknown to us, and add an Unsquelch for that
    messages "original" source. We also check if we receive messages twice. If we do,
    we squelch the one that's usually slowest. Determine slowest by averaging or something, device
    some data structure for this!

    * Figure out how to even test this.

    * Add tests for EphemeralNodeId collisions (figure out how to cause them, probably
    by overriding default EphemeralNodeId in config)

    * Create perf benchmarks, compare with other tools (which?)


        ")
    */

    for _ in 0..35 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    println!("Start time: {:?}", start_time);
    println!("{}", driver.messages_snapshot());

    assert_eq!(
        app1.get_update_heads().unwrap(),
        app2.get_update_heads().unwrap()
    );
    assert_eq!(
        app2.get_update_heads().unwrap(),
        app3.get_update_heads().unwrap()
    );

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());
    let root3 = app3.with_root(|root| root.detach());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
}

#[tokio::test(start_paused = true)]
async fn all_up_three_node_partial_resync1() {
    /*
        compile_error!("
    * Try to avoid sending RequestUpstream from multiple nodes
    * Automatic relay-routing-requests (unsquelch - rename?) (witrh also squelch if duplicates)
        ")
    */

    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();

    let mut add = |db: &mut Database<SyncApp>, _config: &mut DatabaseCommunicationConfig| {
        let mut sess = db.begin_session_mut().unwrap();
        sess.append_single(
            &crate::MessageFrame::new(
                MessageId::from_parts_for_test(datetime!(2020-01-01 T 01:01:01 Z).into(), 1),
                vec![],
                SyncMessage {
                    value: 0,
                    reset: false,
                    persist: false,
                },
            ),
            false,
        )
        .unwrap();
    };
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, Some(&mut add)).await;
    let mut app3 = create_app(&mut driver, Some(&mut add)).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    let start_time = datetime!(2020-01-01 T 00:00:00 Z);

    for app in [&mut app1, &mut app2, &mut app3] {
        app.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;
    app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
        .unwrap();
    driver.set_loss(0.0);

    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());
    let root3 = app3.with_root(|root| root.detach());

    println!("Start time: {:?}", start_time);
    println!("{}", driver.messages_snapshot());

    // TODO: Add actual assertions on:
    //assert_snapshot!(driver.messages_snapshot());
    //assert_snapshot!(driver.raw_frames_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
}

#[tokio::test(start_paused = true)]
async fn all_up_three_node_partial_resync2() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();

    let mut add = |db: &mut Database<SyncApp>, _config: &mut DatabaseCommunicationConfig| {
        let mut sess = db.begin_session_mut().unwrap();
        sess.append_single(
            &crate::MessageFrame::new(
                MessageId::from_parts_for_test(datetime!(2020-01-01 T 01:01:01 Z).into(), 1),
                vec![],
                SyncMessage {
                    value: 0,
                    reset: false,
                    persist: false,
                },
            ),
            false,
        )
        .unwrap();
    };
    let mut app1 = create_app(&mut driver, Some(&mut add)).await;
    let mut app2 = create_app(&mut driver, None).await;
    let mut app3 = create_app(&mut driver, Some(&mut add)).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    let start_time = datetime!(2020-01-01 T 00:00:00 Z);

    for app in [&mut app1, &mut app2, &mut app3] {
        app.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;
    app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
        .unwrap();
    driver.set_loss(0.0);

    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());
    let root3 = app3.with_root(|root| root.detach());

    println!("Start time: {:?}", start_time);
    //println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
}

#[tokio::test(start_paused = true)]
async fn all_up_four_node_partial_resync1() {
    /*
        compile_error!("
    * Try to avoid sending RequestUpstream from multiple nodes
    * Automatic relay-routing-requests (unsquelch - rename?) (witrh also squelch if duplicates)
        ")
    */

    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();

    let mut add = |db: &mut Database<SyncApp>, _config: &mut DatabaseCommunicationConfig| {
        let mut sess = db.begin_session_mut().unwrap();
        sess.append_single(
            &crate::MessageFrame::new(
                MessageId::from_parts_for_test(datetime!(2020-01-01 T 01:01:01 Z).into(), 1),
                vec![],
                SyncMessage {
                    value: 0,
                    reset: false,
                    persist: false,
                },
            ),
            false,
        )
        .unwrap();
    };
    let mut app1 = create_app(&mut driver, Some(&mut add)).await;
    let app2 = create_app(&mut driver, None).await;
    let app3 = create_app(&mut driver, None).await;
    let app4 = create_app(&mut driver, Some(&mut add)).await;

    let start = Instant::now();
    let start_time = datetime!(2020-01-01 T 00:00:00 Z);

    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());
    let root3 = app3.with_root(|root| root.detach());
    let root4 = app4.with_root(|root| root.detach());

    println!("Start time: {:?}", start_time);
    //println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
    assert_eq!(root3, root4);
}

#[tokio::test(start_paused = true)]
async fn all_up_four_node_partial_resync1_node1_isolated() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();

    let mut add = |db: &mut Database<SyncApp>, config: &mut DatabaseCommunicationConfig| {
        config.disable_retransmit = false;
        let mut sess = db.begin_session_mut().unwrap();
        sess.append_single(
            &crate::MessageFrame::new(
                MessageId::from_parts_for_test(datetime!(2020-01-01 T 01:01:01 Z).into(), 1),
                vec![],
                SyncMessage {
                    value: 0,
                    reset: false,
                    persist: false,
                },
            ),
            false,
        )
        .unwrap();
    };
    let mut noadd = |_db: &mut Database<SyncApp>, config: &mut DatabaseCommunicationConfig| {
        config.disable_retransmit = false;
    };
    let mut app1 = create_app(&mut driver, Some(&mut add)).await;
    let app2 = create_app(&mut driver, Some(&mut noadd)).await;
    let app3 = create_app(&mut driver, Some(&mut noadd)).await;
    let app4 = create_app(&mut driver, Some(&mut add)).await;

    let start = Instant::now();
    let start_time = datetime!(2020-01-01 T 00:00:00 Z);

    for i in 0..35 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();

        if i >= 20 {
            driver.unpartition_node(1);
        } else if i >= 9 {
            driver.partition_node(1);
        }
    }

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());
    let root3 = app3.with_root(|root| root.detach());
    let root4 = app4.with_root(|root| root.detach());

    println!("Start time: {:?}", start_time);
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
    assert_eq!(root3, root4);
}

#[tokio::test(start_paused = true)]
async fn ten_nodes_sync_test() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();

    let mut add = |db: &mut Database<SyncApp>, _config: &mut DatabaseCommunicationConfig| {
        let mut sess = db.begin_session_mut().unwrap();
        sess.append_single(
            &crate::MessageFrame::new(
                MessageId::from_parts_for_test(datetime!(2020-01-01 T 01:01:01 Z).into(), 1),
                vec![],
                SyncMessage {
                    value: 0,
                    reset: false,
                    persist: false,
                },
            ),
            false,
        )
        .unwrap();
    };
    let mut apps = vec![];
    for i in 0..10 {
        let inject: Option<
            &mut dyn for<'a> FnMut(
                &'a mut _,
                &mut crate::communication::DatabaseCommunicationConfig,
            ),
        > = if i != 7 { Some(&mut add) } else { None };
        apps.push(create_app(&mut driver, inject).await);
    }

    let start = Instant::now();
    let start_time = datetime!(2020-01-01 T 00:00:00 Z);

    for i in 0..35 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        for app in apps.iter_mut() {
            app.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
                .unwrap();
        }

        if i >= 20 {
            driver.unpartition_node(1);
        } else if i >= 0 {
            driver.partition_node(1);
        }
    }

    println!("Start time: {:?}", start_time);
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    println!("Sent Messages: {} ", driver.sent_messages_count());
    assert!(
        driver.sent_messages_count() < 75,
        "every one sends heads 7 times, plus a few messages to bring 7 up-to-date"
    );

    let root0 = apps[0].with_root(|root| root.detach());
    for i in 1..10 {
        let root = apps[i].with_root(|root| root.detach());
        assert_eq!(root0, root);
    }
}

#[tokio::test(start_paused = true)]
async fn complex_forwarding_test() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();

    let mut apps = vec![];

    /*
    a0 ____          ______ b2
    |      \___c4___/       |
    a1 ___/ \__c5_/ \______ b3

    a* can't hear b* and vice-versa
    c* can hear everyone
    */

    let a0 = {
        let t = apps.len() as u8;
        apps.push(create_app(&mut driver, None).await);
        t
    };
    let a1 = {
        let t = apps.len() as u8;
        apps.push(create_app(&mut driver, None).await);
        t
    };
    let b2 = {
        let t = apps.len() as u8;
        apps.push(create_app(&mut driver, None).await);
        t
    };
    let b3 = {
        let t = apps.len() as u8;
        apps.push(create_app(&mut driver, None).await);
        t
    };
    let c4 = {
        let t = apps.len() as u8;
        apps.push(create_app(&mut driver, None).await);
        t
    };
    let c5 = {
        let t = apps.len() as u8;
        apps.push(create_app(&mut driver, None).await);
        t
    };

    driver.partition_all();
    // a/b can hear each other
    driver.unpartition_bidirectional_links([(a0, a1), (b2, b3)]);
    //cs can hear everyone
    driver.unpartition_node(c4);
    driver.unpartition_node(c5);

    /*
    c* should detect that b* can't hear a*, and should retransmit automatically.
    c1 should not retransmit that which c2 sends and vice versa.
    neither a* nor b* should retransmit anything
    */
    let start_time = datetime!(2020-01-01 T 00:00:00 Z);

    for i in 0..14 {
        tokio::time::sleep(Duration::from_millis(5000)).await;
        for app in apps.iter_mut() {
            app.set_mock_time(NoatunTime::from_datetime(start_time + test_elapsed()))
                .unwrap();
        }

        if i < 5 {
            for app in apps.iter_mut() {
                app.add_message(SyncMessage {
                    persist: true,
                    value: 1,
                    reset: false,
                })
                .await
                .unwrap();
            }
        }
    }

    println!("Start time: {:?}", start_time);
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.sent_messages_snapshot());

    println!("Sent Messages: {} ", driver.sent_messages_count());

    let root0 = apps[0].with_root(|root| root.detach());
    for i in 1..6 {
        let root = apps[i].with_root(|root| root.detach());
        assert_eq!(root0, root, "node {} and {} should have same state", 0, i);
    }

    assert_snapshot!(driver.sent_messages_snapshot());

}
