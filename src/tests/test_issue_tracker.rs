use rand::SeedableRng;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use datetime_literal::datetime;
use insta::assert_snapshot;
use rand::prelude::SmallRng;
use savefile_derive::Savefile;
use tokio::time::Instant;
use crate::data_types::{NoatunHashMap, NoatunString, NoatunVec, OpaqueNoatunVec};
use crate::{set_test_epoch, Database, DatabaseSettings, Message, MessageId, NoatunTime, Object, OpenMode, SavefileMessageSerializer};
use crate::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use crate::tests::all_up_sync_test::{SyncMessage, MY_THREAD_RNG};
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

#[derive(Savefile, Debug)]
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
        let mut root = root.pin_project();
        match self {
            IssueMessage::AddIssue { reporter, heading } => {
                let issue = root.issues.get_insert(heading.as_str());
                let mut issue = issue.pin_project();
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
            "issue_db",
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
async fn all_up_issue_tracker_repro1() {
    // Note, this test triggers a suboptimal (but mostly benign) behavior:
    // It removes a message that has been referenced as the parent of an outgoing
    // message
    setup_tracing();
    let start_instant = Instant::now();
    set_test_epoch(start_instant);
    let noatun_start_time: NoatunTime = START_TIME.into();
    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));

    let mut driver = TestDriver::new(2);
    let mut app1 = create_app(&mut driver, None).await;

    let mut advance_time = async |app1: &mut DatabaseCommunication<IssueMessage>, millis: u64| {
        tokio::time::sleep(Duration::from_millis(millis)).await;
        let time_now = noatun_start_time + start_instant.elapsed();
        app1.set_mock_time(time_now).unwrap();
    };

    println!("Root0: {:?}", app1.with_root(|root| root.detach()));
    app1.add_message(IssueMessage::AddIssue {
        reporter: "user1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa".to_string(),
        heading: "heading1".to_string(),
    })
        .await
        .unwrap();
    advance_time(&mut app1, 500).await;

    println!("Root0a: {:?}", app1.with_root(|root| root.detach()));
    app1.add_message(IssueMessage::AppendText {
        reporter: "user1BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBa".to_string(),
        id: "heading1".to_string(),
        text: "text1".to_string(),
    })
        .await
        .unwrap();
    advance_time(&mut app1, 500).await;

    println!("Root0b: {:?}", app1.with_root(|root| root.detach()));
    app1.add_message(IssueMessage::AppendText {
        reporter: "user1".to_string(),
        id: "heading1".to_string(),
        text: "text2".to_string(),
    })
        .await
        .unwrap();
    advance_time(&mut app1, 500).await;
    println!("Root1: {:?}", app1.with_root(|root| root.detach()));
    app1.add_message(IssueMessage::RemoveIssue {
        id: "heading1".to_string(),
    })
        .await
        .unwrap();
    compile_error!("Continue testing issue tracker. Does it actually work now? \
    Add more stats to the issue-tracker ui
 Probably make it possible to show metrics from issue-tracker ui.
Then show all metrics.
Add good metrics that allow easy troubleshooting
    

    ")
    advance_time(&mut app1, 500).await;
    println!("Root2: {:?}", app1.with_root(|root| root.detach()));
    app1.add_message(IssueMessage::AddIssue {
        reporter: "user1".to_string(),
        heading: "heading1".to_string(),
    })
        .await
        .unwrap();
    advance_time(&mut app1, 500).await;
    println!("Root3: {:?}", app1.with_root(|root| root.detach()));
    app1.add_message(IssueMessage::AppendText {
        reporter: "user1".to_string(),
        id: "heading1".to_string(),
        text: "text1".to_string(),
    })
        .await
        .unwrap();
    advance_time(&mut app1, 500).await;
    app1.add_message(IssueMessage::AppendText {
        reporter: "user1".to_string(),
        id: "heading1".to_string(),
        text: "text2".to_string(),
    })
        .await
        .unwrap();

    advance_time(&mut app1, 15000).await;


    let root1 = app1.with_root(|root| root.detach());

    println!("End state: {:?}", root1);
    assert_eq!(root1.issues.len(), 1);
    println!("{}", driver.raw_frames_snapshot());
    println!("{}", driver.messages_snapshot());

    //assert_snapshot!(driver.messages_snapshot());
}
