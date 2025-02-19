use crate::communication::{
    CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket,
    DatabaseCommunication, DatabaseCommunicationConfig,
};
use crate::cutoff::{CutOffDuration, CutoffHash};
use crate::distributor::Status;
use crate::{Persistence, Savefile};
use crate::{Application, Database, MessagePayload, NoatunContext, NoatunTime, Object};
use arcshift::ArcShift;
use bytes::BufMut;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use datetime_literal::datetime;
use rand::distributions::uniform::SampleUniform;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::cell::RefCell;
use std::fmt::Debug;
use std::io::Write;
use std::ops::{Add};
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
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
}

impl MessagePayload for SyncMessage {
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
        Persistence::AtLeastUntilCutoff
    }
}

#[derive(Clone, Default)]
struct TestDriverInner {
    senders: Vec<Sender<(u8 /*src*/, Vec<u8>)>>,
    loss: f32,
}

struct TestDriver {
    senders: ArcShift<TestDriverInner>,
}
impl TestDriver {
    pub fn set_loss(&mut self, loss: f32) {
        self.senders.rcu_safe(|item| {
            let mut cloned = item.clone();
            cloned.loss = loss;
            cloned
        })
    }
}
impl Default for TestDriver {
    fn default() -> Self {
        TestDriver {
            senders: ArcShift::new(TestDriverInner::default()),
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
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            } else {
                //info!("== SIMULATOR CAUSED PACKET LOSS ==");
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
        self.senders.rcu_safe(|prev| {
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

    fn initialize_root<'a>(_params: &Self::Params) -> Pin<&'a mut Self> {
        NoatunContext.allocate_pod()
    }
}

const START_TIME: DateTime<Utc> = datetime!(2020-01-01 Z);

async fn create_app(driver: &mut TestDriver) -> DatabaseCommunication<SyncApp> {
    let db: Database<SyncApp> = Database::create_in_memory(
        500000,
        CutOffDuration::from_minutes(15),
        Some(START_TIME.into()),
        None,
        (),
    )
    .unwrap();

    DatabaseCommunication::async_tokio_new(
        driver,
        db,
        DatabaseCommunicationConfig {
            listen_address: "dummy".to_string(),
            multicast_address: "dummy".to_string(),
            mtu: 1500,
            bandwidth_limit_bytes_per_second: 1000,
        },
    )
    .await
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn all_up_simple_sync_test() {
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));

    let mut driver = TestDriver::default();
    let app1 = create_app(&mut driver).await;
    let app2 = create_app(&mut driver).await;

    app1.add_message(SyncMessage {
        value: 1,
        reset: false,
    })
    .await
    .unwrap();
    app2.add_message(SyncMessage {
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
}

fn random<T: SampleUniform + PartialOrd>(range: std::ops::Range<T>) -> T {
    MY_THREAD_RNG.with(|rng| rng.borrow_mut().as_mut().unwrap().gen_range(range))
}

#[test]
fn old_local_messages_without_effect_are_removed0() {
    let mut db: Database<SyncApp> = Database::create_in_memory(
        10000,
        CutOffDuration::from_days(2).unwrap(), // 2 days
        Some(datetime!(2020-01-01 00:01:00 Z).into()),
        None,
        (),
    )
    .unwrap();
    let msg1 = db
        .append_local(SyncMessage {
            value: 1,
            reset: false,
        })
        .unwrap();
    println!("Add msg1 {:?}", msg1.id);
    println!("Cutoff-hash: {:?}", db.current_cutoff_state().unwrap());
    db.set_mock_time(datetime!(2020-01-01 00:01:10 Z).into());
    let _msg2 = db
        .append_local(SyncMessage {
            value: 2,
            reset: false,
        })
        .unwrap();
    println!("Add msg2 {:?}", msg1.id);
    db.set_mock_time(datetime!(2020-01-02 00:00:00 Z).into());
    assert_eq!(db.get_all_messages().unwrap().len(), 2);

    db.set_mock_time(datetime!(2024-01-02 Z).into());
    let last = db
        .append_local(SyncMessage {
            value: 0,
            reset: true,
        })
        .unwrap();
    println!("Add msg3 {:?}", last.id);

    let all_msgs = db.get_all_messages().unwrap();
    assert_eq!(all_msgs.len(), 1);
    assert_eq!(all_msgs[0].header.id, last.id);
    println!("Cutoff-hash: {:?}", db.current_cutoff_state().unwrap());
}

#[test]
fn old_transmitted_messages_without_effect_are_removed1() {
    super::setup_tracing();
    let mut db: Database<SyncApp> = Database::create_in_memory(
        10000,
        CutOffDuration::from_days(2).unwrap(), // 2 days
        Some(datetime!(2020-01-01 00:01:00 Z).into()),
        None,
        (),
    )
    .unwrap();
    let msg1 = db
        .append_local(SyncMessage {
            value: 1,
            reset: false,
        })
        .unwrap();
    db.mark_transmitted(msg1.id).unwrap();
    println!("Add msg1 {:?}", msg1.id);
    //println!("Cutoff-hash: {:?}", db.nominal_cutoff_state().unwrap());
    db.set_mock_time(datetime!(2020-01-01 00:01:10 Z).into());
    let msg2 = db
        .append_local(SyncMessage {
            value: 0,
            reset: true,
        })
        .unwrap();
    db.mark_transmitted(msg2.id).unwrap();
    println!("Add msg2 {:?}", msg2.id);
    assert_eq!(db.get_all_messages().unwrap().len(), 2);

    //println!("Advancing time to 2024");
    db.set_mock_time(datetime!(2024-01-02 Z).into());
    db.maybe_advance_cutoff().unwrap();

    let all_msgs = db.get_all_messages().unwrap();
    assert_eq!(all_msgs.len(), 1);
    assert_eq!(all_msgs[0].header.id, msg2.id);
    println!("Cutoff-hash: {:?}", db.current_cutoff_state().unwrap());
    assert_eq!(
        db.current_cutoff_state().unwrap().hash,
        CutoffHash::from_msg(msg2.id)
    );
}

#[test]
fn old_transmitted_messages_without_effect_are_removed2() {
    let mut db: Database<SyncApp> = Database::create_in_memory(
        100000,
        CutOffDuration::from_minutes(15),
        Some(START_TIME.into()),
        None,
        (),
    )
    .unwrap();

    let msg1 = db
        .append_local(SyncMessage {
            value: 1,
            reset: false,
        })
        .unwrap();
    db.mark_transmitted(msg1.id).unwrap();
    println!("Add msg1 {:?}", msg1.id);
    //println!("Cutoff-hash: {:?}", db.nominal_cutoff_state().unwrap());
    db.set_mock_time(datetime!(2020-01-01 01:00:00 Z).into());
    let msg2 = db
        .append_local(SyncMessage {
            value: 0,
            reset: true,
        })
        .unwrap();
    db.mark_transmitted(msg2.id).unwrap();
    println!("Add msg2 {:?}", msg2.id);
    assert_eq!(db.get_all_messages().unwrap().len(), 2);

    //println!("Advancing time to 2024");
    db.set_mock_time(datetime!(2024-01-02 Z).into());
    db.maybe_advance_cutoff().unwrap();

    let all_msgs = db.get_all_messages().unwrap();
    assert_eq!(all_msgs.len(), 1);
    assert_eq!(all_msgs[0].header.id, msg2.id);
    println!("Cutoff-hash: {:?}", db.current_cutoff_state().unwrap());
    assert_eq!(
        db.current_cutoff_state().unwrap().hash,
        CutoffHash::from_msg(msg2.id)
    );
}

#[tokio::test(start_paused = true)]
async fn all_up_gradual_update_sync_test() {
    let mut driver = TestDriver::default();
    let app1 = create_app(&mut driver).await;
    let app2 = create_app(&mut driver).await;

    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));

    driver.set_loss(0.5);
    let mut correct_count = 0;
    let mut correct_sum = 0;
    for _i in 0..10 {
        //println!("I = {}", i);
        //println!("Msgs1: {:#?}", app1.get_all_messages().unwrap());
        assert!(app1
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        assert!(app2
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        app1.add_message_at(
            datetime!(2020-01-01 Z)
                .add(TimeDelta::seconds(random(0..100)))
                .into(),
            SyncMessage {
                value: 1,
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

    let msgs1 = app1.get_all_messages().unwrap();
    let msgs2 = app2.get_all_messages().unwrap();
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
        value: 1,
        reset: false,
    })
    .await
    .unwrap();
    app1.close().await.unwrap();
    app2.close().await.unwrap();
}


#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_old_messages() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {}", seed);
        all_up_general_update_sync_test_impl(seed,7200).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {}", seed);
        all_up_general_update_sync_test_impl(seed,10).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_mid_age_messages() {
    //setup_tracing();
    for seed in 0..100 {
        println!("Seed = {}", seed);
        all_up_general_update_sync_test_impl(seed,900).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_special_seed() {
    super::setup_tracing();
    //for seed in 0..100 {
        println!("Seed = {}", 0);
        all_up_general_update_sync_test_impl(5, 7200).await;
    //}
}

/* TODO:
More all-up synch tests:\
1: With most action _after_ cutoff
2: With no reset, so we can easily know expected value
3: With _recovery_!

*/

async fn all_up_general_update_sync_test_impl(seed: u64, max_message_age_seconds: u64) {
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(seed)));

    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver).await;
    let mut app2 = create_app(&mut driver).await;

    let noatun_start_time: NoatunTime = START_TIME.into();
    let start_instant = tokio::time::Instant::now();

    driver.set_loss(0.15);
    for _i in 0..MY_THREAD_RNG.with(|x|x.borrow_mut().as_mut().unwrap().gen_range(1..20)) {
        let time_now = noatun_start_time + start_instant.elapsed();
        app1.set_mock_time(time_now);
        app2.set_mock_time(time_now);

        //println!("I = {}", i);
        //println!("Msgs: {:#?}", app1.get_all_messages().unwrap());
        assert!(app1
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        assert!(app2
            .get_all_messages()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;

        app1.add_message_at(
            time_now - (Duration::from_secs(random(0..max_message_age_seconds))),
            SyncMessage {
                value: 0,
                reset: MY_THREAD_RNG.with(|x|x.borrow_mut().as_mut().unwrap().gen_bool(0.3)),
            },
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        app2.add_message_at(
            time_now - (Duration::from_secs(random(0..max_message_age_seconds))),
            SyncMessage {
                value: 2,
                reset: MY_THREAD_RNG.with(|x|x.borrow_mut().as_mut().unwrap().gen_bool(0.3)),
            },
        )
        .await
        .unwrap();
    }
    //info!(" -------------- NETWORK HEALED -----------------");
    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(50)).await;
    let time_now = noatun_start_time + start_instant.elapsed();
    app1.set_mock_time(time_now);
    app2.set_mock_time(time_now);
    tokio::time::sleep(Duration::from_secs(20)).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    let msgs1 = app1.get_all_messages().unwrap();
    let msgs2 = app2.get_all_messages().unwrap();
    assert!(msgs1.is_sorted_by_key(|x| x.header.id));
    assert!(msgs2.is_sorted_by_key(|x| x.header.id));
    //println!("Msgs 1:\n{:#?}\nMsgs 2:\n{:#?}", msgs1, msgs2);
    /*let smsgs1: IndexSet<_> = msgs1.iter().map(|x| x.header.id).collect();
    let smsgs2: IndexSet<_> = msgs2.iter().map(|x| x.header.id).collect();
    println!("Cutoff time1: {:?}", app1.get_cutoff_time().unwrap());
    println!("Cutoff time2: {:?}", app2.get_cutoff_time().unwrap());
    println!("Only in 1: {:?}", smsgs1.sub(&smsgs2));
    println!("Only in 2: {:?}", smsgs2.sub(&smsgs1));*/
    assert_eq!(msgs1.len(), msgs2.len());

    assert_eq!(msgs1, msgs2, "Failed for seed {}", seed);
    assert!(root1.sum >= 1);
    assert!(root2.sum >= 1);
    assert!(root1.counter >= 1);
    assert!(root2.counter >= 1);
    assert_eq!(root1, root2);

    assert_eq!(app1.get_status().await.unwrap(), Status::Nominal);
    app1.add_message(SyncMessage {
        value: 1,
        reset: false,
    })
    .await
    .unwrap();

    info!("Test case done");
    app1.close().await.unwrap();
    app2.close().await.unwrap();
}
