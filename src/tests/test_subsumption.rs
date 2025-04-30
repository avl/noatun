use crate::data_types::NoatunVec;
use crate::database::DatabaseSettings;
use crate::{
    msg_deserialize, msg_serialize, Application, Database, Message, NoatunCell,
    NoatunTime,
};
use savefile_derive::Savefile;
use std::io::Write;
use std::pin::Pin;

noatun_object!(
    struct VecDoc {
        object items: NoatunVec<NoatunCell<u32>>,
    }
);

#[derive(Savefile, Debug)]
pub struct VecMessage {
    index: usize,
    val: u32,
    reset: bool,
}

impl Application for VecDoc {
    type Message = VecMessage;
    type Params = ();
}

impl Message for VecMessage {
    type Root = VecDoc;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        if self.reset {
            root.items.clear();
        } else {
            if self.index >= root.items.len() {
                root.items.push(self.val);
            } else {
                root.items.set_item_infallible(self.index, self.val);
            }
        }
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        msg_serialize(self, writer)
    }
}

#[test]
fn test_vec1() {
    super::setup_tracing();
    let mut db: Database<VecDoc> = Database::create_new(
        "test/test_subsumption1",
        true,
        DatabaseSettings::default(),
        (),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    for i in 0..3 {
        db.append_local(VecMessage {
            index: i,
            val: (i + 10) as u32,
            reset: false,
        })
        .unwrap();
        db.compact().unwrap();
    }
    assert_eq!(db.count_messages(), 3);
    db.append_local(VecMessage {
        index: 0,
        val: 0,
        reset: true,
    })
    .unwrap();

    assert_eq!(db.count_messages(), 1);
}
#[test]
fn test_vec2() {
    super::setup_tracing();
    let mut db: Database<VecDoc> = Database::create_new(
        "test/test_subsumption2",
        true,
        DatabaseSettings::default(),
        (),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    for _i in 0..2 {
        let msg = db
            .append_local(VecMessage {
                index: 0,
                val: 0,
                reset: true,
            })
            .unwrap();
        db.mark_transmitted(msg.id).unwrap();
    }
    assert_eq!(db.count_messages(), 1);
}
