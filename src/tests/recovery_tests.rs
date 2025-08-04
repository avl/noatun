use crate::data_types::{NoatunString, NoatunVec};
use crate::database::{DatabaseSettings, LoadingStatus, OpenMode};
use crate::{Database, Message, Object};
use crate::{MessageId, SavefileMessageSerializer};
use chrono::{DateTime, Utc};
use datetime_literal::datetime;
use savefile_derive::Savefile;
use std::fmt::Debug;
use std::pin::Pin;

noatun_object!(
    #[derive(PartialEq)]
    struct KeyValItem {
        object key: NoatunString,
        object value: NoatunString,
    }
);

noatun_object!(
    #[derive(PartialEq)]
    struct KeyValStore {
        object keyval: NoatunVec<KeyValItem>,
        pod edit_count: u32,
    }
);

noatun_object!(
    #[derive(PartialEq)]
    struct KeyValStore2 {
        object keyval: NoatunVec<KeyValItem>,
        pod edit_count: u64,
    }
);


#[derive(Debug, Savefile)]
pub struct KeyValMessage {
    key: String,
    val: String,
}

impl Message for KeyValMessage {
    type Root = KeyValStore;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        let mut projected = root.pin_project();
        projected
            .keyval
            .as_mut()
            .retain(|item| **item.key() != self.key);
        projected.keyval.push(KeyValItemDetached {
            key: self.key.clone(),
            value: self.val.clone(),
        });
        let new_count = projected.edit_count.get() + 1;
        projected.edit_count.set(new_count);
    }
}


#[derive(Debug, Savefile)]
pub struct KeyValMessage2 {
    key: String,
    val: String,
}
impl Message for KeyValMessage2 {
    type Root = KeyValStore2;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        println!("Applying: {:?}", self);
        let mut projected = root.pin_project();
        projected
            .keyval
            .as_mut()
            .retain(|item| **item.key() != self.key);
        projected.keyval.push(KeyValItemDetached {
            key: self.key.clone(),
            value: self.val.clone(),
        });
        let new_count = projected.edit_count.get() + 1;
        projected.edit_count.set(new_count);
    }
}

const START_TIME: DateTime<Utc> = datetime!(2020-01-01 Z);

#[test]
fn test_nominal_load_without_recovery() {
    let mut db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover1",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();

    let mut sess = db.begin_session_mut().unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    })
    .unwrap();

    sess.with_root(|root| {
        assert_eq!(root.edit_count.get(), 3);
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit2".to_string(),
                    value: "Orange".to_string(),
                },
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Apple".to_string(),
                }
            ]
        );
    });
    drop(sess);
    drop(db);

    let db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover1",
        OpenMode::OpenCreate,
        DatabaseSettings::default(),
    )
    .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::CleanLoad);
    db.with_root(|root| {
        assert_eq!(root.edit_count.get(), 3);
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit2".to_string(),
                    value: "Orange".to_string(),
                },
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Apple".to_string(),
                }
            ]
        );
    });
}


#[test]
fn test_recovery_schema_changed() {
    super::setup_tracing();
    let mut db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover_schema_changed1",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
        .unwrap();
    let mut sess = db.begin_session_mut().unwrap();
    sess.disable_filesystem_sync().unwrap();

    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    })
        .unwrap();

    assert_eq!(sess.get_all_message_ids().unwrap().len(), 1);
    drop(sess);
    drop(db);


    // Loading with the exact same schema means no recovery is needed
    let db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover_schema_changed1",
        OpenMode::OpenCreate,
        DatabaseSettings::default(),
    ).unwrap();
    assert_eq!(db.load_status(), LoadingStatus::CleanLoad);
    drop(db);

    // Loading with a different schema, causes recovery
    let db: Database<KeyValMessage2> = Database::create_new(
        "test/test_recover_schema_changed1",
        OpenMode::OpenCreate,
        DatabaseSettings::default(),
    ).unwrap();
    assert_eq!(db.load_status(), LoadingStatus::RecoveryPerformed);

    db.with_root(|root| {
        assert_eq!(root.edit_count.get(), 1);
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Banana".to_string(),
                }
            ]
        );
    });

}


#[test]
fn test_recovery_simple() {
    super::setup_tracing();
    let mut db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover2",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    let mut sess = db.begin_session_mut().unwrap();
    sess.disable_filesystem_sync().unwrap();

    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    })
    .unwrap();

    assert_eq!(sess.get_all_message_ids().unwrap().len(), 3);

    sess.with_root(|root| {
        assert_eq!(root.keyval.len(), 2);
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit2".to_string(),
                    value: "Orange".to_string(),
                },
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Apple".to_string(),
                }
            ]
        );
    });
    assert_eq!(sess.get_all_message_ids().unwrap().len(), 3);
    drop(sess);
    drop(db);

    Database::<KeyValMessage>::remove_caches("test/test_recover2").unwrap();

    let db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover2",
        OpenMode::OpenCreate,
        DatabaseSettings::default(),
    )
    .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::RecoveryPerformed);

    db.with_root(|root| {
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit2".to_string(),
                    value: "Orange".to_string(),
                },
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Apple".to_string(),
                }
            ]
        );
    });

    assert_eq!(
        db.begin_session()
            .unwrap()
            .get_all_message_ids()
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn test_recovery_corrupted_file() {
    Database::<KeyValMessage>::remove_db_files("test/test_recover3").unwrap();
    let mut db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover3",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::NewDatabase);

    let mut sess = db.begin_session_mut().unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    })
    .unwrap();

    assert_eq!(sess.get_all_message_ids().unwrap().len(), 3);
    sess.with_root(|root| {
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit2".to_string(),
                    value: "Orange".to_string(),
                },
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Apple".to_string(),
                }
            ]
        );
    });

    drop(sess);
    drop(db);

    Database::<KeyValMessage>::remove_caches("test/test_recover3").unwrap();

    let mut contents = std::fs::read("test/test_recover3/data0.bin").unwrap();
    // Corrupt the file, replace Orange with Banana
    let orange_index = memchr::memmem::find(&contents, b"Orange").unwrap();
    contents[orange_index..orange_index + 6].copy_from_slice(b"Borang");
    std::fs::write("test/test_recover3/data0.bin", contents).unwrap();

    let db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover3",
        OpenMode::OpenCreate,
        DatabaseSettings::default(),
    )
    .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::RecoveryPerformed);

    db.with_root(|root| {
        assert_eq!(
            root.keyval.detach(),
            vec![KeyValItemDetached {
                key: "Fruit1".to_string(),
                value: "Apple".to_string(),
            }]
        );
    });
    let db = db.begin_session().unwrap();
    let all_ids = db.get_all_message_ids().unwrap();
    assert_eq!(all_ids.len(), 2);
    for (msg, children) in db.get_all_messages_with_children().unwrap() {
        assert!(msg.header.parents.iter().all(|x| all_ids.contains(x)));
        assert!(children.iter().all(|x| all_ids.contains(x)));
    }

    assert_eq!(db.get_all_message_ids().unwrap().len(), 2);
}

fn test_recovery_arbitrary_corruption_impl(corrupt_at_index: usize) {
    let mut db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover4",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    let mut sess = db.begin_session_mut().unwrap();
    sess.disable_filesystem_sync().unwrap();

    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    })
    .unwrap();
    sess.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    })
    .unwrap();

    assert_eq!(sess.get_all_message_ids().unwrap().len(), 3);
    sess.with_root(|root| {
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit2".to_string(),
                    value: "Orange".to_string(),
                },
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Apple".to_string(),
                }
            ]
        );
    });
    drop(sess);
    drop(db);

    Database::<KeyValMessage>::remove_caches("test/test_recover4").unwrap();

    let mut contents = std::fs::read("test/test_recover4/data0.bin").unwrap();
    contents[corrupt_at_index] = contents[corrupt_at_index].wrapping_sub(20);

    std::fs::write("test/test_recover4/data0.bin", &contents).unwrap();

    let db: Database<KeyValMessage> = Database::create_new(
        "test/test_recover4",
        OpenMode::OpenCreate,
        DatabaseSettings::default(),
    )
    .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::RecoveryPerformed);

    let db = db.begin_session().unwrap();
    let all_ids = db.get_all_message_ids().unwrap();
    assert!(all_ids.len() >= 2);
    assert!(all_ids.len() <= 3);

    for (msg, children) in db.get_all_messages_with_children().unwrap() {
        assert!(msg.header.parents.iter().all(|x| all_ids.contains(x)));
        assert!(children.iter().all(|x| all_ids.contains(x)));
    }
}

#[test]
#[ignore] //This test is too long running for every-day runs
fn test_recovery_arbitrary_corruption_long() {
    for i in 0..450 {
        println!("Corrupting by writing at index {i}");
        test_recovery_arbitrary_corruption_impl(i);
    }
}

#[test]
fn test_recovery_arbitrary_corruption() {
    for i in (0..450).step_by(50) {
        println!("Corrupting by writing at index {i}");
        test_recovery_arbitrary_corruption_impl(i);
    }
}
