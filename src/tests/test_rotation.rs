use crate::SavefileMessageSerializer;
use crate::database::{DatabaseSettings, OpenMode};

use crate::Database;
use crate::{Message, NoatunTime};
use savefile_derive::Savefile;
use std::pin::Pin;

noatun_object!(
    #[derive(PartialEq)]
    struct RotationDoc {
        pod counter: u64,
    }
);

#[derive(Savefile, Debug)]
pub struct RotMessage {
    increment: u64,
    reset: u64,
}

impl Message for RotMessage {
    type Root = RotationDoc;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        if self.reset != 0 {
            root.counter.set(0);
        } else {
            let new_counter = root.counter.get().wrapping_add(self.increment);
            root.counter.set(new_counter);
        }
    }

    
}

#[test]
fn test_rotation1() {
    let mut db: Database<RotMessage> =
        Database::create_new("test/test_rotation1", OpenMode::Overwrite, DatabaseSettings::default()).unwrap();
    let mut db = db.begin_session_mut().unwrap();
    for _ in 0..5 {
        for _ in 0..10 {
            db.append_local(RotMessage {
                increment: 1,
                reset: 0,
            })
            .unwrap();
            db.compact().unwrap();
        }
        db.append_local(RotMessage {
            increment: 0,
            reset: 1,
        })
        .unwrap();
        db.compact().unwrap();
    }
}


#[test]
fn test_rotation_big1() {
    let mut db: Database<RotMessage> =
        Database::create_new("test/test_rotation2", OpenMode::Overwrite, DatabaseSettings::default()).unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();
    for _ in 0..75 {
        for _ in 0..50 {
            db.append_local(RotMessage {
                increment: 1,
                reset: 0,
            })
            .unwrap();
            db.compact().unwrap();
        }
        db.append_local(RotMessage {
            increment: 0,
            reset: 1,
        })
        .unwrap();
        db.compact().unwrap();
    }

    assert_eq!(db.count_messages(), 1);
    db.with_root(|root|{
        assert_eq!(root.counter.get(), 0);
    })
}

#[test]
#[cfg(feature="expensive_tests")]
fn test_rotation_big2() {
    let mut db: Database<RotMessage> =
        Database::create_new("test/test_rotation2", true, DatabaseSettings::default()).unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();
    for _ in 0..200 {
        for _ in 0..50 {
            db.append_local(RotMessage {
                increment: 1,
                reset: 0,
            })
                .unwrap();
            db.compact().unwrap();
        }
        db.append_local(RotMessage {
            increment: 1,
            reset: 0,
        })
            .unwrap();
        db.compact().unwrap();
    }

    db.with_root(|root|{
        assert_eq!(root.counter.get(), 200*51);
    })
}
