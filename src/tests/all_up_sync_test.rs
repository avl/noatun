#![allow(clippy::needless_range_loop)]
use super::test_driver::*;
#[allow(unused)]
use crate::colors::colored_int;
use crate::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use crate::cutoff::{CutOffDuration, CutoffHash};
use crate::database::DatabaseSettings;
use crate::distributor::Status;
use crate::noatun_instant::Instant;
use crate::simple_metrics::SimpleMetricsRecorder;
use crate::tests::setup_tracing;
use crate::{reset_random_id, SavefileMessageSerializer};
use crate::{set_test_epoch, test_elapsed, Database, Message, MessageId, NoatunTime, Object};
use crate::{Persistence, Savefile};
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use datetime_literal::datetime;
use indexmap::IndexSet;
use insta::assert_snapshot;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::cell::RefCell;
use std::fmt::Debug;
use std::ops::Add;
use std::ops::Sub;
use std::pin::Pin;
use std::time::Duration;
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
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
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

    fn persistence(&self) -> Persistence {
        if self.persist {
            Persistence::AtLeastUntilCutoff
        } else {
            Persistence::UntilOverwritten
        }
    }
}

const START_TIME: DateTime<Utc> = datetime!(2020-01-01 Z);

async fn create_app(
    driver: &mut TestDriver,
    modify: Option<&mut dyn FnMut(&mut Database<SyncMessage>, &mut DatabaseCommunicationConfig)>,
) -> DatabaseCommunication<SyncMessage> {
    let mut db: Database<SyncMessage> = Database::create_in_memory(
        2_500_000,
        DatabaseSettings {
            mock_time: Some(START_TIME.into()),
            projection_time_limit: None,
            ..DatabaseSettings::default()
        },
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
        enable_diagnostics: false,
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

    let mut driver = TestDriver::new(2);
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

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());

    assert_eq!(root1.sum, 3);
    assert_eq!(root2.sum, 3);
    assert_eq!(root1.counter, 2);
    assert_eq!(root2.counter, 2);
    assert_eq!(root1, root2);

    assert_snapshot!(driver.messages_snapshot());
}

#[test]
fn old_local_messages_without_effect_are_removed0() {
    let mut db: Database<SyncMessage> = Database::create_in_memory(
        10000,
        DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 00:01:00 Z).into()),
            cutoff_interval: CutOffDuration::from_days(2).unwrap(),
            ..Default::default()
        },
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
    assert_eq!(sess.get_all_messages_vec().unwrap().len(), 2);

    sess.set_mock_time(datetime!(2024-01-02 Z).into()).unwrap();
    let last = sess
        .append_local(SyncMessage {
            persist: true,
            value: 0,
            reset: true,
        })
        .unwrap();
    println!("Add msg3 {:?}", last.id);

    let all_msgs = sess.get_all_messages_vec().unwrap();
    assert_eq!(all_msgs.len(), 1);
    assert_eq!(all_msgs[0].header.id, last.id);
    println!("Cutoff-hash: {:?}", sess.current_cutoff_state().unwrap());
}

#[test]
fn old_transmitted_messages_without_effect_are_removed1() {
    super::setup_tracing();
    let mut db: Database<SyncMessage> = Database::create_in_memory(
        10000,
        DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 00:01:00 Z).into()),
            cutoff_interval: CutOffDuration::from_days(2).unwrap(),
            ..Default::default()
        },
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
    assert_eq!(sess.get_all_messages_vec().unwrap().len(), 2);

    sess.set_mock_time(datetime!(2024-01-02 Z).into()).unwrap();

    let all_msgs = sess.get_all_messages_vec().unwrap();
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
    let mut db: Database<SyncMessage> = Database::create_in_memory(
        100000,
        DatabaseSettings {
            mock_time: Some(START_TIME.into()),
            ..Default::default()
        },
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
    println!("Added msg1 {:?}", msg1.id);

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
    println!("Added msg2 {:?}", msg2.id);
    assert_eq!(sess.get_all_messages_vec().unwrap().len(), 2);

    sess.set_mock_time(datetime!(2024-01-02 Z).into()).unwrap();

    let all_msgs = sess.get_all_messages_vec().unwrap();
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
    let mut driver = TestDriver::new(2);
    let app1 = create_app(&mut driver, None).await;
    let app2 = create_app(&mut driver, None).await;

    driver.set_loss(0.5);
    let mut correct_count = 0;
    let mut correct_sum = 0;
    let start_time = Instant::now();
    for _i in 0..10 {
        assert!(app1
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages_vec()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        assert!(app2
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages_vec()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        let elapsed = start_time.elapsed();
        app1.add_message_at(
            datetime!(2020-01-01 Z).add(elapsed).into(),
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

    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(50)).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());

    let msgs1 = app1
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages_vec()
        .unwrap();
    let msgs2 = app2
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages_vec()
        .unwrap();
    assert!(msgs1.is_sorted_by_key(|x| x.header.id));
    assert!(msgs2.is_sorted_by_key(|x| x.header.id));

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
}

#[cfg(debug_assertions)]
const NUM_CASES: u64 = 200;
#[cfg(not(debug_assertions))]
const NUM_CASES: u64 = 1000;

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_old_messages_all() {
    let test_recorder = SimpleMetricsRecorder::default();
    let _guard = test_recorder.register_local();

    for seed in 0..NUM_CASES {
        println!("=========== Seed = {seed} ===========");
        reset_random_id();
        all_up_general_update_sync_test_impl(seed, 7200, true, usize::MAX, true).await;
    }
    #[cfg(not(debug_assertions))]
    assert_snapshot!(test_recorder.get_metrics_text());
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_old_messages_654() {
    {
        let seed = 654;
        println!("=========== Seed = {seed} ===========");
        reset_random_id();
        all_up_general_update_sync_test_impl(seed, 7200, true, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages_persist() {
    for seed in 0..NUM_CASES {
        println!("=========== Seed = {seed} ===========");
        all_up_general_update_sync_test_impl(seed, 10, true, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_mid_age_messages_persist_all() {
    setup_tracing();
    for seed in 0..NUM_CASES {
        println!("=========== Seed = {seed} ===========");
        all_up_general_update_sync_test_impl(seed, 900, true, usize::MAX, true).await;
    }
}
#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_mid_age_messages_persist_2413() {
    setup_tracing();
    {
        let seed = 2413;
        println!("=========== Seed = {seed} ===========");
        all_up_general_update_sync_test_impl(seed, 900, true, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages_no_persist_all() {
    for seed in 0..NUM_CASES {
        println!("\n\n============ Seed {seed} ==============\n\n");
        all_up_general_update_sync_test_impl(seed, 10, false, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages_no_persist87() {
    {
        let seed = 87;
        println!("\n\n============ Seed {seed} ==============\n\n");
        all_up_general_update_sync_test_impl(seed, 10, false, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_newer_messages_no_persist_no_reset() {
    for seed in 0..NUM_CASES {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 10, false, usize::MAX, false).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_general_update_sync_test_mid_age_messages_no_persist_all() {
    setup_tracing();
    for seed in 0..NUM_CASES {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 900, false, usize::MAX, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn all_up_special_seed() {
    super::setup_tracing();
    for seed in 0..NUM_CASES {
        println!("Seed = {seed}");
        all_up_general_update_sync_test_impl(seed, 15, false, 10, true).await;
    }
}

pub async fn assert_equal<T: Message + Send + PartialEq>(
    app1: &mut DatabaseCommunication<T>,
    app2: &mut DatabaseCommunication<T>,
    seed: u64,
) {
    let msgs1 = app1
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages_vec()
        .unwrap();
    let msgs2 = app2
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages_vec()
        .unwrap();
    println!("Node 0 messages:\n{msgs1:#?}");
    println!("Node 1 messages:\n{msgs2:#?}");
    assert!(msgs1.is_sorted_by_key(|x| x.header.id));
    assert!(msgs2.is_sorted_by_key(|x| x.header.id));
    let smsgs1: IndexSet<_> = msgs1.iter().map(|x| x.header.id).collect();
    let smsgs2: IndexSet<_> = msgs2.iter().map(|x| x.header.id).collect();

    println!("Seed: {seed}");
    println!("Only in 0: {:?}", smsgs1.sub(&smsgs2));
    println!("Only in 1: {:?}", smsgs2.sub(&smsgs1));

    if msgs1 != msgs2 {
        println!("App0 cutoff time: {:?}", app1.get_cutoff_time());
        println!("App1 cutoff time: {:?}", app2.get_cutoff_time());
        for (i, (msg1, msg2)) in msgs1.iter().zip(msgs2.iter()).enumerate() {
            assert_eq!(
                msg1,
                msg2,
                "message mismatch for msg #{i}, comparing node {} and {}",
                app1.ephemeral_node_id().await.unwrap(),
                app2.ephemeral_node_id().await.unwrap(),
            );
        }
    }
    assert_eq!(msgs1.len(), msgs2.len());
    assert_eq!(msgs1, msgs2, "Failed for seed {seed}");
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

    let mut driver = TestDriver::new(2);
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;

    let noatun_start_time: NoatunTime = START_TIME.into();
    let start_instant = Instant::now();
    let mut total_count = 0;
    let mut total_added = 0;

    driver.set_loss(0.15);
    let steps = MY_THREAD_RNG
        .with(|x| x.borrow_mut().as_mut().unwrap().gen_range(1..20))
        .min(maxlen);
    println!("Steps: {steps}");
    for i in 0..steps {
        info!("==== write {} =====", i);
        let time_now = noatun_start_time + start_instant.elapsed();
        app1.set_mock_time(time_now).unwrap();
        app2.set_mock_time(time_now).unwrap();

        assert!(app1
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages_vec()
            .unwrap()
            .is_sorted_by_key(|x| x.header.id.timestamp()));
        assert!(app2
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages_vec()
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
    println!(
        "{:?} -------------- NETWORK HEALED -----------------",
        test_elapsed()
    );
    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(90)).await;

    let time_now = noatun_start_time + start_instant.elapsed() + Duration::from_secs(1000);
    app1.set_mock_time(time_now).unwrap();
    app2.set_mock_time(time_now).unwrap();

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());

    info!("Test case done");
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    if persist {
        assert_equal(&mut app1, &mut app2, seed).await;
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

    let correct_root = root1;

    assert_eq!(app1.get_status().await.unwrap(), Status::Nominal);
    assert_eq!(app2.get_status().await.unwrap(), Status::Nominal);

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());

    assert_eq!(root1, correct_root);
    assert_eq!(root2, correct_root);

    app1.close().await.unwrap();
    app2.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn all_up_big_severely_desynced_test() {
    set_test_epoch(Instant::now());
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);
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
    tokio::time::sleep(Duration::from_secs(70)).await;

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());

    assert_eq!(root1, root2);
    println!("{}", driver.messages_snapshot());
}

#[tokio::test(start_paused = true)]
async fn all_up_big_nominal_test() {
    set_test_epoch(Instant::now());

    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);
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

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());

    println!("{}", driver.messages_snapshot());

    let msgs1 = app1
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages_vec()
        .unwrap();
    let msgs2 = app2
        .inner_database()
        .begin_session()
        .unwrap()
        .get_all_messages_vec()
        .unwrap();
    assert!(msgs1.is_sorted_by_key(|x| x.header.id));
    assert!(msgs2.is_sorted_by_key(|x| x.header.id));
    let smsgs1: IndexSet<_> = msgs1.iter().map(|x| x.header.id).collect();
    let smsgs2: IndexSet<_> = msgs2.iter().map(|x| x.header.id).collect();
    println!("Cutoff time1: {:?}", app1.get_cutoff_time().unwrap());
    println!("Cutoff time2: {:?}", app2.get_cutoff_time().unwrap());
    println!("Only in 0: {:?}", smsgs1.sub(&smsgs2));
    println!("Only in 1: {:?}", smsgs2.sub(&smsgs1));
    {
        assert_eq!(msgs1.len(), msgs2.len());
    }

    assert_eq!(root1, root2);
}

#[tokio::test(start_paused = true)]
async fn all_up_huge_desynced_test() {
    set_test_epoch(Instant::now());
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);
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

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());
    println!("{}", driver.messages_snapshot());
    assert_snapshot!(driver.messages_snapshot());
    assert_eq!(root1, root2);
}

#[tokio::test(start_paused = true)]
async fn all_up_three_node_resync() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);
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

    for _ in 0..35 {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        app1.set_mock_time(NoatunTime::from_datetime(start_time + start.elapsed()))
            .unwrap();
    }

    println!("Start time: {start_time:?}");
    println!("{}", driver.messages_snapshot());

    assert_eq!(
        app1.get_update_heads().unwrap(),
        app2.get_update_heads().unwrap()
    );
    assert_eq!(
        app2.get_update_heads().unwrap(),
        app3.get_update_heads().unwrap()
    );

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());
    let root3 = app3.with_root(|root| root.export());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
}

#[tokio::test(start_paused = true)]
async fn all_up_three_node_partial_resync1() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);

    let mut add = |db: &mut Database<SyncMessage>, _config: &mut DatabaseCommunicationConfig| {
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

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());
    let root3 = app3.with_root(|root| root.export());

    println!("Start time: {start_time:?}");
    println!("{}", driver.messages_snapshot());

    assert_snapshot!(driver.messages_snapshot());
    assert_snapshot!(driver.raw_frames_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
}

#[tokio::test(start_paused = true)]
async fn all_up_three_node_partial_resync2() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);

    let mut add = |db: &mut Database<SyncMessage>, _config: &mut DatabaseCommunicationConfig| {
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

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());
    let root3 = app3.with_root(|root| root.export());

    println!("Start time: {start_time:?}");

    println!("{}", driver.messages_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
}

#[tokio::test(start_paused = true)]
async fn all_up_four_node_partial_resync1() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);

    let mut add = |db: &mut Database<SyncMessage>, _config: &mut DatabaseCommunicationConfig| {
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

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());
    let root3 = app3.with_root(|root| root.export());
    let root4 = app4.with_root(|root| root.export());

    println!("Start time: {start_time:?}");

    println!("{}", driver.messages_snapshot());

    assert_eq!(root1, root2);
    assert_eq!(root2, root3);
    assert_eq!(root3, root4);
}

#[tokio::test(start_paused = true)]
async fn all_up_four_node_partial_resync1_node1_isolated() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);

    let mut add = |db: &mut Database<SyncMessage>, config: &mut DatabaseCommunicationConfig| {
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
    let mut noadd = |_db: &mut Database<SyncMessage>, config: &mut DatabaseCommunicationConfig| {
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

    let root1 = app1.with_root(|root| root.export());
    let root2 = app2.with_root(|root| root.export());
    let root3 = app3.with_root(|root| root.export());
    let root4 = app4.with_root(|root| root.export());

    println!("Start time: {start_time:?}");
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
    let mut driver = TestDriver::new(2);

    let mut add = |db: &mut Database<SyncMessage>, _config: &mut DatabaseCommunicationConfig| {
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

    println!("Start time: {start_time:?}");
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    println!("Sent Messages: {} ", driver.sent_messages_count());
    assert!(
        driver.sent_messages_count() < 75,
        "every one sends heads 7 times, plus a few messages to bring 7 up-to-date"
    );

    let root0 = apps[0].with_root(|root| root.export());
    for i in 1..10 {
        let root = apps[i].with_root(|root| root.export());
        assert_eq!(root0, root);
    }
}

#[tokio::test(start_paused = true)]
async fn complex_forwarding_test() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let mut driver = TestDriver::new(2);

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
    // cs can hear everyone
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

    println!("Start time: {start_time:?}");
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.sent_messages_snapshot());

    println!("Sent Messages: {} ", driver.sent_messages_count());

    let root0 = apps[0].with_root(|root| root.export());
    for i in 1..6 {
        let root = apps[i].with_root(|root| root.export());
        assert_eq!(root0, root, "node {} and {} should have same state", 0, i);
    }
    assert_snapshot!(driver.sent_messages_snapshot());
}

#[tokio::test(start_paused = true)]
async fn all_up_clock_mismatch_test() {
    setup_tracing();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    let start_instant = Instant::now();
    let noatun_start_time: NoatunTime = START_TIME.into();
    let mut driver = TestDriver::new(2);

    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;

    app1.add_message(SyncMessage {
        value: 1,
        persist: false,
        reset: false,
    })
    .await
    .unwrap();

    app2.add_message(SyncMessage {
        value: 2,
        persist: false,
        reset: false,
    })
    .await
    .unwrap();

    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let time_now = noatun_start_time + start_instant.elapsed();
        app1.set_mock_time(time_now).unwrap();
        app2.set_mock_time(time_now + Duration::from_secs(60))
            .unwrap();
    }

    app2.inner_database()
        .begin_session_mut()
        .unwrap()
        .set_mock_time(NoatunTime::from_datetime(
            START_TIME + Duration::from_secs(30),
        ))
        .unwrap();

    println!("Start time: {START_TIME:?}");
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.sent_messages_snapshot());

    println!("Sent Messages: {} ", driver.sent_messages_count());

    assert_snapshot!(driver.sent_messages_snapshot());
}
