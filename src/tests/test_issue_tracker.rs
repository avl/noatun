use rand::{Rng, SeedableRng};
use std::pin::Pin;
use std::time::Duration;
use anyhow::{Result};
use chrono::{DateTime, Utc};
use datetime_literal::datetime;
use insta::assert_debug_snapshot;
use rand::prelude::SmallRng;
use savefile_derive::Savefile;
use tokio::time::Instant;
use crate::data_types::{NoatunHashMap, NoatunString, OpaqueNoatunVec};
use crate::{set_test_epoch, Database, DatabaseSettings, Message, MessageId, NoatunTime, Object, OpenMode, SavefileMessageSerializer};
use crate::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use crate::tests::all_up_sync_test::{MY_THREAD_RNG};
use crate::tests::setup_tracing;
use crate::tests::test_driver::TestDriver;

noatun_object!(
    #[derive(PartialEq)]
    struct DescriptionText {
        pod time: NoatunTime,
        object text: NoatunString,
        object added_by: NoatunString,
    }
);

noatun_object!(
    #[derive(PartialEq)]
    struct Issue {
        pod created: NoatunTime,
        object reporter: NoatunString,
        object heading: NoatunString,
        object description: OpaqueNoatunVec<DescriptionText>,
    }
);

noatun_object!(
    #[derive(PartialEq)]
    struct IssueDb {
        object issues: NoatunHashMap<NoatunString, Issue>,
    }
);

#[derive(Savefile, Debug, PartialEq)]
enum IssueMessage {
    AddIssue {
        reporter: String,
        heading: String,
    },
    AppendText {
        id: String,
        reporter: String,
        text: String,
    },
    RemoveIssue {
        id: String,
    }
}

impl Message for IssueMessage {
    type Root = IssueDb;
    type Serializer = SavefileMessageSerializer<IssueMessage>;

    fn apply(&self, message_id: MessageId, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        match self {
            IssueMessage::AddIssue { reporter, heading } => {
                let issue = root.issues.get_insert(heading.as_str());
                let issue = issue.pin_project();
                issue.created.set(message_id.timestamp());
                issue.reporter.assign(reporter);
                issue.heading.assign(heading);
            }
            IssueMessage::RemoveIssue { id } => {
                root.issues.remove(id.as_str());
            }
            IssueMessage::AppendText { id, reporter, text } => {
                if let Some(issue) = root.issues.get_mut_val(id.as_str()) {
                    let issue = issue.pin_project();
                    issue.description.push(DescriptionTextDetached {
                        time: message_id.timestamp(),
                        text: text.to_string(),
                        added_by: reporter.to_string(),
                    });
                }
            }
        }
    }
}

#[test]
fn test_issue_tracker() {
    let mut db: Database<IssueMessage> =
        Database::create_new(
            "test/test_issue_db",
            OpenMode::Overwrite, DatabaseSettings::default()).unwrap();

    let mut sess = db.begin_session_mut().unwrap();

    sess.append_nonlocal(IssueMessage::AddIssue {
        reporter: "anders".to_string(),
        heading: "test1".to_string(),
    }).unwrap();

    std::thread::sleep(Duration::from_millis(10));

    sess.append_nonlocal(IssueMessage::RemoveIssue {
        id: "test1".to_string(),
    }).unwrap();

    std::thread::sleep(Duration::from_millis(10));

    sess.append_nonlocal(IssueMessage::AddIssue {
        reporter: "anders".to_string(),
        heading: "test1".to_string(),
    }).unwrap();
}


#[test]
fn test_issue_tracker2() {
    let mut db: Database<IssueMessage> =
        Database::create_new(
            "test/test_issue_db",
            OpenMode::Overwrite, DatabaseSettings::default()).unwrap();


    let mut sess = db.begin_session_mut().unwrap();
    sess.set_mock_time_no_advance(NoatunTime::debug_time(1)).unwrap();

    sess.append_nonlocal(IssueMessage::AddIssue {
        reporter: "user1".to_string(),
        heading: "heading2".to_string(),
    }).unwrap();
    sess.set_mock_time_no_advance(NoatunTime::debug_time(61)).unwrap();
    sess.append_nonlocal(IssueMessage::AppendText {
        id: "heading2".to_string(),
        reporter: "user1".to_string(),

        text: "think".to_string(),
    }).unwrap();
    sess.set_mock_time_no_advance(NoatunTime::debug_time(62)).unwrap();

    sess.append_nonlocal(IssueMessage::AddIssue {
        reporter: "user1".to_string(),
        heading: "bfgd".to_string(),
    }).unwrap();
    sess.set_mock_time_no_advance(NoatunTime::debug_time(63)).unwrap();

    sess.append_nonlocal(IssueMessage::RemoveIssue {
        id: "bfgd".to_string(),
    }).unwrap();

    sess.set_mock_time_no_advance(NoatunTime::debug_time(64)).unwrap();
    sess.append_nonlocal(IssueMessage::AddIssue {
        reporter: "user1".to_string(),
        heading: "4".to_string(),
    }).unwrap();
    sess.set_mock_time_no_advance(NoatunTime::debug_time(240)).unwrap();
    sess.append_nonlocal(IssueMessage::AddIssue {
        reporter: "user1".to_string(),
        heading: "5".to_string(),
    }).unwrap();

    println!("Messages: {:?}", sess.get_all_messages_vec());
    assert_debug_snapshot!(sess.get_all_messages_vec());
}


const START_TIME: DateTime<Utc> = datetime!(2020-01-01 Z);

async fn create_app(
    driver: &mut TestDriver,
    modify: Option<&mut dyn FnMut(&mut Database<IssueMessage>, &mut DatabaseCommunicationConfig)>,
) -> DatabaseCommunication<IssueMessage> {
    let mut db: Database<IssueMessage> = Database::create_in_memory(
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

#[cfg(debug_assertions)]
const COUNT:u64 = 100;
#[cfg(not(debug_assertions))]
const COUNT:u64 = 2000;


#[tokio::test(start_paused = true)]
async fn all_up_issue_tracker_all() {
    setup_tracing();
    for seed in 0..COUNT {
        println!("-----------Seed: {}-------------", seed);
        all_up_issue_iteration(seed).await;
    }
}
#[tokio::test(start_paused = true)]
async fn all_up_issue_tracker_8() {
    setup_tracing();
    {
        let seed = 8;
        println!("-----------Seed: {}-------------", seed);
        all_up_issue_iteration(seed).await;
    }
}
async fn all_up_issue_iteration(seed: u64) {
    // Note, this test triggers a suboptimal (but mostly benign) behavior:
    // It removes a message that has been referenced as the parent of an outgoing
    // message
    let start_instant = Instant::now();
    set_test_epoch(start_instant);
    let noatun_start_time: NoatunTime = START_TIME.into();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(seed)));

    let mut driver = TestDriver::new(2);
    let mut app1 = create_app(&mut driver, None).await;
    let mut app2 = create_app(&mut driver, None).await;

    let advance_time = async |app1: &mut DatabaseCommunication<IssueMessage>, millis: u64| {
        tokio::time::sleep(Duration::from_millis(millis)).await;
        let time_now = noatun_start_time + start_instant.elapsed();
        app1.set_mock_time(time_now).unwrap();
    };

    for _ in 0..20 {
        let msg = MY_THREAD_RNG.with(|rng|{
            let mut rng = rng.borrow_mut();
            let rng = rng.as_mut().unwrap();
            match rng.gen_range(0..=2) {
                0 => {
                    IssueMessage::AddIssue {
                        reporter: format!("user{}", rng.gen_range(0..4u32)),
                        heading: format!("head{}", rng.gen_range(0..4u32)),
                    }
                }
                1 => {
                    IssueMessage::AppendText {
                        id: format!("head{}", rng.gen_range(0..4u32)),
                        reporter: format!("user{}", rng.gen_range(0..4u32)),
                        text: format!("text{}", rng.gen::<u128>()),
                    }
                }
                2 => {
                    IssueMessage::RemoveIssue {
                        id: format!("head{}", rng.gen_range(0..4u32)),
                    }
                }
                _ => unreachable!()
            }
        });
        app1.add_message(msg).await.unwrap();
    }


    advance_time(&mut app1, 50000).await;


    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    crate::tests::all_up_sync_test::assert_equal(&mut app1, &mut app2, seed).await;

    let root1 = app1.with_root(|root| root.detach());
    let root2 = app2.with_root(|root| root.detach());

    println!("End state: {:?}", root1);
    assert_eq!(root1, root2);

    //assert_snapshot!(driver.messages_snapshot());
}
