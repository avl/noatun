use crate::data_types::NoatunVec;
use crate::database::DatabaseSettings;
use crate::{msg_deserialize, msg_serialize, Database, Message, MessageId, NoatunCell, NoatunTime};
use savefile_derive::Savefile;
use std::io::Write;
use std::pin::Pin;
use crate::MessageFrame;

noatun_object!(
    struct Doc {
        opod counter: u32,
    }
);

#[derive(Savefile, Debug)]
pub struct DocMessage {
    val: u32,
    reset: bool,
}


impl Message for DocMessage {
    type Root = Doc;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        if self.reset {
            root.counter.set(0);
        } else {
            root.counter += self.val;
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
fn test_subsume_nonlocal() {
    super::setup_tracing();
    let msg0_time = MessageId::new_debug2(0).timestamp();
    let mut db: Database<DocMessage> = Database::create_new(
        "test/test_subsumption_nonlocal1",
        true,
        DatabaseSettings {
            mock_time: Some(msg0_time),
            ..DatabaseSettings::default()
        },
        ).unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    println!("Cur cutoff time: {:?}", db.current_cutoff_time());

    for i in 0..3 {
        db.append_single(
            &MessageFrame::new(MessageId::new_debug2(i),vec![], DocMessage {
                val: 1,
                reset: false,
            }), false)
            .unwrap();
    }
    assert_eq!(db.count_messages(), 3);

    db.append_single(
        &MessageFrame::new(MessageId::new_debug2(10),vec![], DocMessage {
            val: 0,
            reset: true
        }), false)
        .unwrap();

    assert_eq!(db.count_messages(), 1);
}
