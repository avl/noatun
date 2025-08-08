use crate::data_types::{NoatunHashMap, NoatunString, NoatunVec};
use crate::database::DatabaseSettings;
use crate::tests::{DummyTestMessage, DummyTestMessageApply};
use crate::{
    CutOffDuration, Database, FixedSizeObject, MessageFrame, MessageId, NoatunCell, NoatunTime,
};
use datetime_literal::datetime;
use std::marker::PhantomData;
use std::pin::Pin;

fn rewind_tester<T>()
where
    T: FixedSizeObject + DummyTestMessageApply + std::fmt::Debug,
{
    let mut db: Database<DummyTestMessage<T>> = Database::create_in_memory(
        10000,
        DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 Z).into()),
            cutoff_interval: CutOffDuration::from_days(365).unwrap(),

            auto_prune: false,
            ..Default::default()
        },
    )
    .unwrap();
    fn snapshotter<T: std::fmt::Debug>(t: &T) -> String {
        format!("{t:#?}")
    }

    let clean_snapshot = db.with_root(snapshotter);
    let mut db = db.begin_session_mut().unwrap();
    db.append_single(
        &MessageFrame::new(
            MessageId::from_parts_for_test(datetime!(2020-01-02 Z).into(), 0),
            vec![],
            DummyTestMessage(PhantomData),
        ),
        false,
    )
    .unwrap();

    let snapshot1 = db.with_root(snapshotter);

    db.append_single(
        &MessageFrame::new(
            MessageId::from_parts_for_test(datetime!(2020-01-04 Z).into(), 0),
            vec![],
            DummyTestMessage(PhantomData),
        ),
        false,
    )
    .unwrap();

    let snapshot2 = db.with_root(snapshotter);

    db.set_projection_time_limit(datetime!(2020-01-03 Z).into())
        .unwrap();
    println!("Have rewound to 01-03");

    let rewound_snapshot1 = db.with_root(snapshotter);
    db.set_projection_time_limit(datetime!(2020-01-01 Z).into())
        .unwrap();
    let rewound_clean = db.with_root(snapshotter);

    println!("snap1: {snapshot1}");
    println!("rewound-snap1: {rewound_snapshot1}");
    println!("snap2: {snapshot2}");

    assert_eq!(clean_snapshot, rewound_clean);
    assert_eq!(snapshot1, rewound_snapshot1);

    assert_ne!(snapshot1, clean_snapshot);
    assert_ne!(snapshot2, snapshot1);
}

#[test]
fn rewind_test_cell() {
    impl DummyTestMessageApply for NoatunCell<u32> {
        fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>) {
            root.set(time.0 as u32)
        }
    }

    rewind_tester::<NoatunCell<u32>>();
}

#[test]
fn rewind_test_string_mod() {
    impl DummyTestMessageApply for NoatunString {
        fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>) {
            root.assign(&time.to_string());
        }
    }

    rewind_tester::<NoatunString>();
}

#[test]
fn rewind_test_vec_add() {
    impl DummyTestMessageApply for NoatunVec<NoatunCell<u16>> {
        fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>) {
            if root.is_empty() {
                root.push(time.0 as u16);
            } else {
                root.get_index_mut(0).set(time.0 as u16);
            }
        }
    }

    rewind_tester::<NoatunVec<NoatunCell<u16>>>();
}
#[test]
fn rewind_test_vec_remove() {
    impl DummyTestMessageApply for NoatunVec<NoatunCell<u32>> {
        fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>) {
            if root.is_empty() {
                root.push(time.0 as u32);
            } else {
                root.swap_remove(0);
            }
        }
    }

    rewind_tester::<NoatunVec<NoatunCell<u32>>>();
}

#[test]
fn test_vec_retain() {
    impl DummyTestMessageApply for NoatunVec<NoatunCell<u64>> {
        fn test_message_apply(_time: NoatunTime, root: Pin<&mut Self>) {
            if root.is_empty() {
                root.push(1);
            } else {
                root.retain(|_| false);
            }
        }
    }
    rewind_tester::<NoatunVec<NoatunCell<u64>>>();
}

#[test]
fn test_vec_retain_some() {
    impl DummyTestMessageApply for NoatunVec<NoatunCell<i64>> {
        fn test_message_apply(_time: NoatunTime, mut root: Pin<&mut Self>) {
            if root.is_empty() {
                root.as_mut().push(1);
                root.as_mut().push(2);
            } else {
                root.as_mut().retain(|x| **x == 2);
            }
            assert_eq!(**root.iter().next().unwrap(), 1);
        }
    }
    rewind_tester::<NoatunVec<NoatunCell<i64>>>();
}

#[test]
fn rewind_test_hashmap_insert() {
    impl DummyTestMessageApply for crate::data_types::NoatunHashMap<u16, NoatunCell<u16>> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            root.insert_internal(time.0 as u16, &(time.0 as u16))
        }
    }

    rewind_tester::<NoatunHashMap<u16, NoatunCell<u16>>>();
}
#[test]
fn rewind_test_hashmap_remove() {
    super::setup_tracing();
    impl DummyTestMessageApply for NoatunHashMap<u64, NoatunCell<u32>> {
        fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
            if root.iter().next().is_none() {
                root.insert_internal(time.0, &(time.0 as u32))
            } else {
                let key = *root.iter().next().unwrap().0;
                root.remove(&key);
            }
        }
    }

    rewind_tester::<NoatunHashMap<u64, NoatunCell<u32>>>();
}
