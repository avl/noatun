use std::io::Write;
use std::pin::Pin;
use crate::{MessagePayload, NoatunContext, NoatunTime};
use savefile_derive::Savefile;
use crate::Application;
use crate::Database;
use crate::CutOffDuration;
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

impl Application for RotationDoc {
    type Message = RotMessage;
    type Params = ();

    fn initialize_root<'a>(_params: &Self::Params) -> Pin<&'a mut Self> {
        NoatunContext.allocate_pod()
    }
}
impl MessagePayload for RotMessage {
    type Root = RotationDoc;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        if self.reset!=0 {
            root.counter.set(0);
        }else {
            let new_counter = root.counter.get().wrapping_add(self.increment);
            root.counter.set(new_counter);
        }
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

#[test]
fn test_rotation1() {
    let mut db: Database<RotationDoc> = Database::create_new(
        "test/test_rotation1",
        true,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    for _ in 0..5 {
        for _ in 0..10 {
            db.append_local(RotMessage {
                increment: 1,
                reset: 0,
            }).unwrap();
            db.compact().unwrap();
        }
        db.append_local(RotMessage {
            increment: 0,
            reset: 1,
        }).unwrap();
        db.compact().unwrap();
    }
}

/*
compile_error!("
 * Dare to test the taint-system more - reenable persistence = off for all-up-tests!
 * Hashmaps!
 * More chaos testing!
 * More testing of noatun-objects. Do observe etc actually work for all types?
")
*/

#[test]
fn test_rotation_big1() {
    let mut db: Database<RotationDoc> = Database::create_new(
        "test/test_rotation2",
        true,
        10_000_000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();
    db.disable_filesystem_sync().unwrap();
    for _ in 0..200 {
        for _ in 0..50 {
            db.append_local(RotMessage {
                increment: 1,
                reset: 0,
            }).unwrap();
            db.compact().unwrap();
        }
        db.append_local(RotMessage {
            increment: 0,
            reset: 1,
        }).unwrap();
        db.compact().unwrap();
    }
}
