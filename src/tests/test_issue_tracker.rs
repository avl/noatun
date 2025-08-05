use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use anyhow::{Context, Result};
use savefile_derive::Savefile;
use crate::data_types::{NoatunHashMap, NoatunString, NoatunVec, OpaqueNoatunVec};
use crate::{Database, DatabaseSettings, Message, MessageId, NoatunTime, Object, OpenMode, SavefileMessageSerializer};
use crate::communication::{DatabaseCommunication, DatabaseCommunicationConfig};


noatun_object!(
    struct DescriptionText {
        pod time: NoatunTime,
        object text: NoatunString,
        object added_by: NoatunString,
    }
);

noatun_object!(
    struct Issue {
        pod created: NoatunTime,
        object reporter: NoatunString,
        object heading: NoatunString,
        object description: OpaqueNoatunVec<DescriptionText>,
    }
);

noatun_object!(
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