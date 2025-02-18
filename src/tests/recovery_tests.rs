use std::fmt::{Debug};
use std::io::Write;
use std::pin::Pin;
use savefile_derive::Savefile;
use crate::data_types::NoatunString;
use crate::{Application, CutOffDuration, Database, DatabaseVec, MessagePayload, NoatunContext, NoatunTime, Object};
use datetime_literal::datetime;
use chrono::{DateTime, Utc};
use crate::database::LoadingStatus;

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
        object keyval: DatabaseVec<KeyValItem>,
        pod edit_count: u32,
    }
);

#[derive(Debug, Savefile)]
pub struct KeyValMessage {
    key: String,
    val: String,
}

impl Application for KeyValStore {
    type Message = KeyValMessage;
    type Params = ();

    fn initialize_root<'a>(_params: &Self::Params) -> Pin<&'a mut Self> {
        NoatunContext.allocate_pod()
    }
}

impl MessagePayload for KeyValMessage {
    type Root = KeyValStore;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut Self::Root>) {
        let mut projected = root.pin_project();
        projected.keyval.as_mut().retain(|item|&**item.key() != self.key);
        projected.keyval.push(KeyValItemDetached {
            key: self.key.clone(),
            value: self.val.clone()
        });
        let new_count = projected.edit_count.get()+1;
        projected.edit_count.set(new_count);
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        crate::msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        crate::msg_serialize(self, writer)
    }
}

const START_TIME: DateTime<Utc> = datetime!(2020-01-01 Z);



#[test]
fn test_nominal_load_without_recovery() {
    let mut db: Database<KeyValStore> = Database::create_new(
        "test/test_recover1",
        true,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();

    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    }).unwrap();

    db.with_root(|root|{
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
            ]);
    });
    drop(db);

    let db: Database<KeyValStore> = Database::create_new(
        "test/test_recover1",
        false,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::CleanLoad);
    db.with_root(|root|{
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
            ]);
    });

}

#[test]
fn test_recovery() {
    let mut db: Database<KeyValStore> = Database::create_new(
        "test/test_recover2",
        true,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    db.disable_filesystem_sync();

    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    }).unwrap();

    assert_eq!(db.get_all_message_ids().unwrap().len(), 3);
    db.with_root(|root|{
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
            ]);
    });
    drop(db);

    Database::<KeyValStore>::remove_caches("test/test_recover2").unwrap();

    let db: Database<KeyValStore> = Database::create_new(
        "test/test_recover2",
        false,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::RecoveryPerformed);

    db.with_root(|root|{
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
            ]);
    });

    assert_eq!(db.get_all_message_ids().unwrap().len(), 3);

}


#[test]
fn test_recovery_corrupted_file() {
    let mut db: Database<KeyValStore> = Database::create_new(
        "test/test_recover3",
        true,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();

    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    }).unwrap();

    assert_eq!(db.get_all_message_ids().unwrap().len(), 3);
    db.with_root(|root|{
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
            ]);
    });
    drop(db);

    Database::<KeyValStore>::remove_caches("test/test_recover3").unwrap();

    let mut contents = std::fs::read("test/test_recover3/data0.bin").unwrap();
    // Corrupt the file, replace Orange with Banana
    let orange_index = memchr::memmem::find(&contents, b"Orange").unwrap();
    contents[orange_index..orange_index+6].copy_from_slice(b"Borang");
    std::fs::write("test/test_recover3/data0.bin", contents).unwrap();

    let db: Database<KeyValStore> = Database::create_new(
        "test/test_recover3",
        false,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::RecoveryPerformed);

    db.with_root(|root|{
        assert_eq!(
            root.keyval.detach(),
            vec![
                KeyValItemDetached {
                    key: "Fruit1".to_string(),
                    value: "Apple".to_string(),
                }
            ]);
    });

    let all_ids = db.get_all_message_ids().unwrap();
    assert_eq!(all_ids.len(), 2);
    for (msg, children) in db.get_all_messages_with_children().unwrap() {
        assert!(msg.header.parents.iter().all(|x|all_ids.contains(x)));
        assert!(children.iter().all(|x|all_ids.contains(x)));
    }

    assert_eq!(db.get_all_message_ids().unwrap().len(), 2);

}


fn test_recovery_arbitrary_corruption_impl(corrupt_at_index: usize) {
    let mut db: Database<KeyValStore> = Database::create_new(
        "test/test_recover4",
        true,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    db.disable_filesystem_sync();

    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Banana".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit2".to_string(),
        val: "Orange".to_string(),
    }).unwrap();
    db.append_local(KeyValMessage {
        key: "Fruit1".to_string(),
        val: "Apple".to_string(),
    }).unwrap();

    assert_eq!(db.get_all_message_ids().unwrap().len(), 3);
    db.with_root(|root|{
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
            ]);
    });
    drop(db);

    Database::<KeyValStore>::remove_caches("test/test_recover4").unwrap();

    let mut contents = std::fs::read("test/test_recover4/data0.bin").unwrap();
    // Corrupt the file, replace Orange with Banana
    /*for i in 0..20 {
        contents[i] = 0x43;
    }*/
    contents[corrupt_at_index] = contents[corrupt_at_index].wrapping_sub(20);

    //println!("\n ========= Corrupted {} file at {}\n", contents.len(), corrupt_at_index);

    std::fs::write("test/test_recover4/data0.bin", &contents).unwrap();

    let db: Database<KeyValStore> = Database::create_new(
        "test/test_recover4",
        false,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    assert_eq!(db.load_status(), LoadingStatus::RecoveryPerformed);


    let all_ids = db.get_all_message_ids().unwrap();
    assert!(all_ids.len() >= 2);
    assert!(all_ids.len() <= 3);

    for (msg, children) in db.get_all_messages_with_children().unwrap() {
        assert!(msg.header.parents.iter().all(|x|all_ids.contains(x)));
        assert!(children.iter().all(|x|all_ids.contains(x)));
    }
}

#[test]
#[ignore] //This test is too long running for every-day runs
fn test_recovery_arbitrary_corruption_long() {
    for i in 0..450 {
        println!("Corrupting by writing at index {}", i);
        test_recovery_arbitrary_corruption_impl(i);
    }
}

#[test]
fn test_recovery_arbitrary_corruption() {
    for i in (0..450).step_by(50) {
        println!("Corrupting by writing at index {}", i);
        test_recovery_arbitrary_corruption_impl(i);
    }
}
