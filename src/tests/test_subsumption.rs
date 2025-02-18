use std::io::Write;
use std::pin::Pin;
use savefile_derive::Savefile;
use crate::{msg_deserialize, msg_serialize, Application, CutOffDuration, Database, DatabaseCell, MessagePayload, NoatunContext, NoatunTime};
use crate::tests::test_rotation::{RotMessage, RotationDoc};
use crate::DatabaseVec;

noatun_object!(
    struct VecDoc {
        object items: DatabaseVec<DatabaseCell<u32>>,
    }
);


#[derive(Savefile, Debug)]
pub struct VecMessage {
    index: usize,
    val: u32,
    reset: bool
}

impl Application for VecDoc {
    type Message = VecMessage;
    type Params = ();

    fn initialize_root<'a>(params: &Self::Params) -> Pin<&'a mut Self> {
        NoatunContext.allocate_pod()
    }
}

impl MessagePayload for VecMessage {
    type Root = VecDoc;

    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        if self.reset {
            root.items.clear();
        } else {
            if self.index >= root.items.len() {
                root.items.push(self.val);
            } else {
                root.items.set_item(self.index, self.val);
            }
        }
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        msg_serialize(self, writer)
    }
}

#[test]
fn test_vec1() {
    let mut db: Database<VecDoc> = Database::create_new(
        "test/test_subsumption1",
        true,
        100000,
        CutOffDuration::from_minutes(15),
        None,
        (),
    )
        .unwrap();

    for i in 0..1 {
        db.append_local(VecMessage {
            index: i,
            val: i as u32,
            reset: false,
        }).unwrap();
        db.compact().unwrap();
    }
    db.append_local(VecMessage {
        index: 0,
        val: 0,
        reset: true,
    }).unwrap();

    db.reproject().unwrap();

    compile_error!("Check why the original message isn't deleted!")
    assert_eq!(db.count_messages(), 1);
}
