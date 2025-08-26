use crate::cutoff::CutOffDuration;
use crate::data_types::*;
use crate::database::DatabaseSettings;
use crate::sequence_nr::SequenceNr;
use crate::{Database, Message, NoatunPod, NoatunTime, SchemaHasher};
use crate::{MessageId, SavefileMessageSerializer};
use crate::{NoatunStorable, Object};
use datetime_literal::datetime;
use rand::prelude::SliceRandom;
use savefile_derive::Savefile;

use std::pin::Pin;
use std::time::Duration;

#[derive(Copy, Clone, Debug, Savefile)]
#[repr(C)]
pub struct DummyObj {
    x: u32,
    y: u32,
}

unsafe impl NoatunStorable for DummyObj {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::DummyObj/1");
    }
}
unsafe impl NoatunPod for DummyObj {}

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

impl PartialEq for SubObjNative {
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

impl Message for TestMessage {
    type Root = TestDb;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, time: MessageId, mut root: Pin<&mut Self::Root>) {
        root.as_mut().items_mut().push(SubObjNative {
            counter: self.insert,
            subsub: vec![SubSubObjNative {
                dummy: DummyObj { x: 1, y: 2 },
            }],
        });
        root.as_mut().set_now(time.timestamp());
    }
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
            DatabaseSettings {
                mock_time: Some(fake_time),
                projection_time_limit: Some(limit_time),
                cutoff_interval: CutOffDuration::from_hours(1).unwrap(), // 2 days
                ..Default::default()
            },
        )
        .unwrap();

        let mut msgs: Vec<TestMessage> = (0..NUM_MSGS)
            .map(|x| TestMessage { insert: x as u32 })
            .collect();
        let orig: Vec<SubObjNative> = msgs
            .iter()
            .map(|x| SubObjNative {
                counter: x.insert,
                subsub: vec![SubSubObjNative {
                    dummy: DummyObj {
                        x: x.insert,
                        y: x.insert,
                    },
                }],
            })
            .collect();

        msgs.shuffle(&mut rand::thread_rng());

        let then = fake_time;
        let mut db = db.begin_session_mut().unwrap();
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
            assert_eq!(&root.items.export(), &orig[0..=TIME_LIMIT]);
            assert!(root.now().0 > 0);
        });

        db.reproject().unwrap();
        db.set_projection_time_limit(fake_time + Duration::from_secs(1000))
            .unwrap();
        db.reproject().unwrap();
        db.with_root(|root: &TestDb| {
            assert_eq!(&root.items.export(), &orig);
        });
    }
}
