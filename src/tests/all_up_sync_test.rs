use crate::communication::{
    CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket,
    DatabaseCommunication, DatabaseCommunicationConfig, DebugEvent, DebugEventMsg,
};
use crate::cutoff::{CutOffDuration, CutoffHash};
use crate::database::DatabaseSettings;
use crate::distributor::Status;
use crate::{Application, Database, Message, NoatunTime, Object};
use crate::{Persistence, Savefile};
use arcshift::ArcShift;
use bytes::BufMut;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use datetime_literal::datetime;
use indexmap::IndexSet;
use insta::{assert_debug_snapshot, assert_snapshot};
use rand::distributions::uniform::SampleUniform;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::cell::RefCell;
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
struct TestDriverInner {
    senders: Vec<Sender<(u8 /*src*/, Vec<u8>)>>,
    loss: f32,
}

struct TestDriver {
    driver_start: Instant,
    senders: ArcShift<TestDriverInner>,
    debug_events: Arc<Mutex<Vec<DebugEvent>>>,
}
impl TestDriver {
    pub fn messages_snapshot(&self) -> String {
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
                DebugEventMsg::Receive(_) => String::new(),
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
            debug_events: Arc::new(Mutex::new(vec![])),
        }
    }
}
struct TestDriverReceiver(Receiver<(u8 /*src*/, Vec<u8>)>);
struct TestDriverSender(u8 /*own addr*/, ArcShift<TestDriverInner>);

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

    async fn send_to(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let driver_inner = self.1.get();
        let data = buf.to_vec();
        for item in driver_inner.senders.iter() {
            if driver_inner.loss <= random(0.0..1.0) {
                item.send((self.0 /*src*/, data.clone()))
                    .await
                    .map_err(|e| std::io::Error::other(format!("simulated net failed {e:?}")))?;
            } else {
                //println!("== SIMULATOR CAUSED PACKET LOSS ==");
            }
        }
        Ok(buf.len())
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
            senders.senders.push(tx.clone());
            senders
        });

        Ok((
            TestDriverSender(index.unwrap().try_into().unwrap(), self.senders.clone()),
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

async fn create_app(driver: &mut TestDriver) -> DatabaseCommunication<SyncApp> {
    let db: Database<SyncApp> = Database::create_in_memory(
        2_500_000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(START_TIME.into()),
            projection_time_limit: None,
            ..DatabaseSettings::default()
        },
        (),
    )
    .unwrap();

    let log = driver.debug_events.clone();
    let comm = DatabaseCommunication::async_tokio_new(
        driver,
        db,
        DatabaseCommunicationConfig {
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
        },
    )
    .await
    .unwrap();
    comm
}

#[tokio::test(start_paused = true)]
async fn all_up_simple_sync_test() {
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));

    let mut driver = TestDriver::default();
    let app1 = create_app(&mut driver).await;
    let app2 = create_app(&mut driver).await;

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
        CutOffDuration::from_days(2).unwrap(), // 2 days
        DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 00:01:00 Z).into()),
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
        CutOffDuration::from_days(2).unwrap(), // 2 days
        DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 00:01:00 Z).into()),
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
        CutOffDuration::from_minutes(15),
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
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let app1 = create_app(&mut driver).await;
    let app2 = create_app(&mut driver).await;

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
    for seed in 0..50 {
        /* Notes:
        on 1:
        The problem now seems to be:

        0-0-0 is written, and is followed by 3-0-0 which depends on 0-0-0
        0-0-0 is thus not deleted
        2-0-0 then comes in, deleting 3-0-0.
        At this point, since 3-0-0 no longer exists, we should reproject 0-0-0, and could then delete it.

        How would we know to travel to 0-0-0 to delete it?

        */
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
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(seed)));

    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver).await;
    let mut app2 = create_app(&mut driver).await;

    let noatun_start_time: NoatunTime = START_TIME.into();
    let start_instant = tokio::time::Instant::now();
    let mut total_count = 0;
    let mut total_added = 0;

    driver.set_loss(0.15);
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
    info!("Test case done");

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
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver).await;
    let mut app2 = create_app(&mut driver).await;

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
        )).unwrap();
        app2.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        )).unwrap();
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
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver).await;
    let mut app2 = create_app(&mut driver).await;

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
        )).unwrap();
        app2.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        )).unwrap();
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
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver).await;
    let mut app2 = create_app(&mut driver).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    for _msg in 0..200 {
        app1.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        )).unwrap();
        app2.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        )).unwrap();
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
    assert_debug_snapshot!(driver.messages_snapshot());
    assert_eq!(root1, root2);
}


#[tokio::test(start_paused = true)]
async fn all_up_three_node_resync() {
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver).await;
    let app2 = create_app(&mut driver).await;
    let app3 = create_app(&mut driver).await;

    driver.set_loss(1.0);
    let start = Instant::now();
    app1.set_mock_time(NoatunTime::from_datetime(
        datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
    )).unwrap();

    for i in 0..2 {
        app1.add_message(SyncMessage {
            value: i,
            persist: false,
            reset: false,
        })
            .await
            .unwrap();

    }
    tokio::time::sleep(Duration::from_millis(1000)).await;
    app1.set_mock_time(NoatunTime::from_datetime(
        datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
    )).unwrap();
    driver.set_loss(0.0);

    for _ in 0..15 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        app1.set_mock_time(NoatunTime::from_datetime(
            datetime!(2020-01-01 T 01:01:01 Z) + start.elapsed(),
        )).unwrap();
    }

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());
    let root3 = app3.with_root(|root| root.detach());
    println!("{}", driver.messages_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
}
