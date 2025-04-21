use crate::cutoff::CutOffDuration;
use crate::data_types::*;
use crate::sequence_nr::SequenceNr;
use crate::{NoatunStorable, Object};
use crate::{
    msg_deserialize, msg_serialize, Application, Database, MessagePayload, NoatunTime,
};
use datetime_literal::datetime;
use rand::prelude::SliceRandom;
use savefile_derive::Savefile;
use serde_derive::{Deserialize, Serialize};
use std::io::Write;
use std::pin::Pin;
use std::time::Duration;
use crate::database::DatabaseSettings;

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Savefile)]
#[repr(C)]
pub struct DummyObj {
    x: u32,
    y: u32,
}

unsafe impl NoatunStorable for DummyObj{}

noatun_object!(
    struct SubSubObj {
        pod dummy: DummyObj
    }
);

noatun_object!(
    struct SubObj {
        pod counter: u32,
        object subsub: NoatunVec<SubSubObj>,
    }
);

impl PartialEq for SubObjDetached {
    fn eq(&self, other: &Self) -> bool {
        self.counter == other.counter
    }
}

noatun_object!(
    struct TestDb {
        pod now: NoatunTime,
        object items: NoatunVec<SubObj>,
    }
);

#[derive(Savefile, Debug, Clone)]
pub struct TestMessage {
    insert: u32,
}

impl MessagePayload for TestMessage {
    type Root = TestDb;

    fn apply(&self, time: NoatunTime, mut root: Pin<&mut Self::Root>) {
        root.as_mut().items_mut().push(SubObjDetached {
            counter: self.insert,
            subsub: vec![SubSubObjDetached {
                dummy: DummyObj { x: 1, y: 2 },
            }],
        });
        root.as_mut().set_now(time);
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(msg_deserialize(buf)?)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        msg_serialize(self, writer)
    }
}

impl Application for TestDb {
    type Message = TestMessage;
    type Params = ();
}

#[test]
fn test() {
    for _ in 0..20 {
        let fake_time = datetime!(2024-01-01 00:00:00 Z).into();
        const TIME_LIMIT: usize = 15;
        const NUM_MSGS: usize = 20;

        let limit_time = fake_time + Duration::from_secs(TIME_LIMIT as u64);
        let mut db = Database::create_in_memory(
            1_000_000,
            CutOffDuration::from_hours(1).unwrap(),
            DatabaseSettings {
                mock_time: Some(fake_time),
                projection_time_limit: Some(limit_time),
                ..Default::default()
            },
            (),
        )
        .unwrap();

        let mut msgs: Vec<TestMessage> = (0..NUM_MSGS)
            .map(|x| TestMessage { insert: x as u32 })
            .collect();
        let orig: Vec<SubObjDetached> = msgs
            .iter()
            .map(|x| SubObjDetached {
                counter: x.insert,
                subsub: vec![SubSubObjDetached {
                    dummy: DummyObj {
                        x: x.insert,
                        y: x.insert,
                    },
                }],
            })
            .collect();

        msgs.shuffle(&mut rand::thread_rng());

        let then = fake_time;
        for msg in msgs.iter() {
            let at = then + Duration::from_secs(msg.insert as u64);
            db.append_local_at(at, msg.clone()).unwrap();
        }

        db.force_rewind(SequenceNr::from_index(0));
        db.with_root(|root: &TestDb| {
            assert_eq!(root.now().0, 0);
            assert_eq!(root.items().len(), 0);
        });

        db.reproject().unwrap();
        db.with_root(|root: &TestDb| {
            assert_eq!(&root.items.detach(), &orig[0..=TIME_LIMIT]);
            assert!(root.now().0 > 0);
        });

        db.reproject().unwrap();
        db.set_projection_time_limit(fake_time + Duration::from_secs(1000))
            .unwrap();
        db.reproject().unwrap();
        db.with_root(|root: &TestDb| {
            assert_eq!(&root.items.detach(), &orig);
        });
    }
}
