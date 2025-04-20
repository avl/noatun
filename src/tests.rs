#![allow(non_local_definitions)]
use super::*;
use crate::data_types::{DatabaseCellArrayExt, NoatunString};
use crate::disk_access::FileAccessor;
use crate::sequence_nr::SequenceNr;
use byteorder::{LittleEndian, WriteBytesExt};

use crate::cutoff::CutOffDuration;
use data_types::DatabaseCell;
use data_types::DatabaseObjectHandle;
use data_types::DatabaseVec;
use database::Database;
use datetime_literal::datetime;
use savefile::{load_noschema, save_noschema, Deserialize, Packed, SavefileError, Schema, Serialize, Serializer, WithSchema, WithSchemaContext};
use savefile_derive::Savefile;
use std::io::{Cursor, Read, SeekFrom};
use tracing_subscriber::Layer;
use crate::database::DatabaseSettings;

mod all_up_sync_test;
mod distributor_tests;
mod fuzz_test_insert;
mod tests_using_noatun_object_macro;
mod recovery_tests;
mod test_rotation;
mod test_subsumption;

mod test_types_rewind {
    use std::marker::PhantomData;
    use datetime_literal::datetime;
    use crate::{CutOffDuration, Database, DatabaseCell, FixedSizeObject, Message, MessageId, NoatunTime};
    use crate::tests::{DummyTestApp, DummyTestMessage, DummyTestMessageApply};
    use std::pin::Pin;
    //TODO: Move DatabaseVec to data_types, like the rest of the noatun types
    use crate::DatabaseVec;
    use crate::data_types::{DatabaseHash, NoatunString};
    use crate::database::DatabaseSettings;

    fn rewind_tester<T>() where
        T: FixedSizeObject + DummyTestMessageApply + std::fmt::Debug {
        let mut db: Database<DummyTestApp<T>> = Database::create_in_memory(
            10000,
            CutOffDuration::from_days(365).unwrap(),
            DatabaseSettings {
                mock_time: Some(datetime!(2020-01-01 Z).into()),
                auto_delete: false,
                ..Default::default()
            },
            (),
        )
            .unwrap();
        fn snapshotter<T:std::fmt::Debug>(t: &T) -> String {
            format!("{:#?}", t)
        }

        let clean_snapshot = db.with_root(snapshotter);

        db.append_single(
            Message::new(MessageId::from_parts_for_test(datetime!(2020-01-02 Z).into(), 0), vec![], DummyTestMessage(PhantomData))
            , false).unwrap();

        let snapshot1 = db.with_root(snapshotter);

        db.append_single(
            Message::new(MessageId::from_parts_for_test(datetime!(2020-01-04 Z).into(), 0), vec![], DummyTestMessage(PhantomData))
            , false).unwrap();

        let snapshot2 = db.with_root(snapshotter);

        db.set_projection_time_limit(datetime!(2020-01-03 Z).into()).unwrap();
        println!("Have rewound to 01-03");

        let rewound_snapshot1 = db.with_root(snapshotter);
        db.set_projection_time_limit(datetime!(2020-01-01 Z).into()).unwrap();
        let rewound_clean = db.with_root(snapshotter);

        println!("snap1: {}",snapshot1);
        println!("rewound-snap1: {}",rewound_snapshot1);
        println!("snap2: {}",snapshot2);

        assert_eq!(clean_snapshot, rewound_clean);
        assert_eq!(snapshot1, rewound_snapshot1);

        assert_ne!(snapshot1, clean_snapshot);
        assert_ne!(snapshot2, snapshot1);


    }

    #[test]
    fn rewind_test_cell() {
        impl DummyTestMessageApply for DatabaseCell<u32> {
            fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>) {
                root.set(time.0 as u32)
            }
        }

        rewind_tester::<DatabaseCell<u32>>();
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
        impl DummyTestMessageApply for DatabaseVec<DatabaseCell<u16>> {
            fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>) {
                if root.is_empty() {
                    root.push(time.0 as u16);
                } else {
                    root.getmut(0).set(time.0 as u16);
                }
            }
        }

        rewind_tester::<DatabaseVec<DatabaseCell<u16>>>();
    }
    #[test]
    fn rewind_test_vec_remove() {
        impl DummyTestMessageApply for DatabaseVec<DatabaseCell<u32>> {
            fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>) {
                if root.is_empty() {
                    root.push(time.0 as u32);
                } else {
                    root.shift_remove(0);
                }
            }
        }

        rewind_tester::<DatabaseVec<DatabaseCell<u32>>>();
    }
    #[test]
    fn rewind_test_hashmap_insert() {
        impl DummyTestMessageApply for crate::data_types::DatabaseHash<u16,DatabaseCell<u16>> {
            fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
                root.insert(time.0 as u16, &(time.0 as u16))
            }
        }

        rewind_tester::<crate::data_types::DatabaseHash<u16,DatabaseCell<u16>>>();
    }
    #[test]
    fn rewind_test_hashmap_remove() {
        super::setup_tracing();
        impl DummyTestMessageApply for DatabaseHash<u64,DatabaseCell<u32>> {
            fn test_message_apply(time: NoatunTime, mut root: Pin<&mut Self>) {
                if root.is_empty() {
                    root.insert(time.0, &(time.0 as u32))
                } else {
                    println!("Before remove: {:?}", root);
                    let key = *root.iter().next().unwrap().0;
                    root.remove(key);
                    println!("After remove: {:?}", root);
                    //root.insert(time.0 as u64, &(time.0 as u32));
                    println!("After re-add: {:?}", root);
                }
            }
        }

        rewind_tester::<DatabaseHash<u64,DatabaseCell<u32>>>();
    }
}

#[repr(transparent)]
#[derive(Clone,Copy,AnyBitPattern,Debug)]
pub struct DummyTestApp<Root>(pub Root);

impl<Root> DummyTestApp<Root> {
    pub fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut Root> {
        unsafe {
            self.map_unchecked_mut(|x|&mut x.0)
        }
    }
    pub fn inner(&self) -> &Root {
        &self.0
    }
}

impl<Root:FixedSizeObject> Object for DummyTestApp<Root> {
    type Ptr = ThinPtr;
    type DetachedType = ();
    type DetachedOwnedType = ();

    fn detach(&self) -> Self::DetachedOwnedType {
    }

    fn clear(self: Pin<&mut Self>) {
        unimplemented!()
    }

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {
    }

    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        unimplemented!()
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        NoatunContext.access_object(index)
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        NoatunContext.access_object_mut(index)
    }
}



pub struct DummyTestMessage<Root>(std::marker::PhantomData<Root>);
impl<Root> Debug for DummyTestMessage<Root> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DummyTestMessage")
    }
}

impl<T> WithSchema for DummyTestMessage<T> {
    fn schema(_version: u32, _context: &mut WithSchemaContext) -> Schema {
        Schema::Custom("DummyTestMessage".to_string())
    }
}
impl<T> Packed for DummyTestMessage<T> {}
impl<T> Serialize for DummyTestMessage<T> {
    fn serialize(&self, _serializer: &mut Serializer<impl Write>) -> std::result::Result<(), SavefileError> {
        Ok(())
    }
}
impl<T> Deserialize for DummyTestMessage<T> {
    fn deserialize(_deserializer: &mut Deserializer<impl Read>) -> std::result::Result<Self, SavefileError> {
        Ok(DummyTestMessage(std::marker::PhantomData))
    }
}




pub(crate) trait DummyTestMessageApply {
    fn test_message_apply(time: NoatunTime, root: Pin<&mut Self>);
}

impl<Root:FixedSizeObject+DummyTestMessageApply> MessagePayload for DummyTestMessage<Root> {
    type Root = DummyTestApp<Root>;

    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
        Root::test_message_apply(time, root.inner_mut())
    }

    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized
    {
        msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> Result<()> {
        msg_serialize(self, writer)
    }
}

impl<Root:FixedSizeObject+DummyTestMessageApply> Application for DummyTestApp<Root>{
    type Message = DummyTestMessage<Root>;
    type Params = ();

    //TODO: Should `initialize_root' be unsafe?
    fn initialize_root<'a>(_params: &Self::Params) -> Pin<&'a mut Self> {
        unsafe {
            std::mem::transmute(NoatunContext.allocate_pod::<Root>())
        }
    }
}


pub fn setup_tracing() {
    pub struct TracingTimer(tokio::time::Instant);

    impl tracing_subscriber::fmt::time::FormatTime for TracingTimer {
        fn format_time(
            &self,
            w: &mut tracing_subscriber::fmt::format::Writer<'_>,
        ) -> core::fmt::Result {
            let t = tokio::time::Instant::now();
            write!(w, "{:>10?}", (t - self.0))
        }
    }

    let stdout_log = tracing_subscriber::fmt::layer()
        .with_timer(TracingTimer(tokio::time::Instant::now()))
        .with_ansi(false)
        .pretty()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(stdout_log);
    _ = tracing::subscriber::set_global_default(subscriber);
}

#[test]
fn test_mmap_big() {
    let _mmap = FileAccessor::new(
        &Target::CreateNewOrOverwrite("test/mmap_test_big".into()),
        "mmap",
        0,
        1024 * 1024 * 1024,
    );
    //use std::io::Read;
    //let _ =  std::io::stdin().read(&mut [0u8]).unwrap();
}

#[test]
fn test_mmap_helper() {
    let mut mmap = FileAccessor::new(
        &Target::CreateNewOrOverwrite("test/mmap_test1".into()),
        "mmap",
        0,
        16 * 1024 * 1024,
    )
    .unwrap();
    mmap.write_u32::<LittleEndian>(0x2b).unwrap();
    use byteorder::ReadBytesExt;
    use std::io::Read;
    use std::io::Seek;
    mmap.seek(SeekFrom::Start(12)).unwrap();
    mmap.write_u64::<LittleEndian>(0x2c).unwrap();
    mmap.seek(SeekFrom::Start(12)).unwrap();
    let initial_ptr = mmap.map_mut_ptr();
    assert_eq!(mmap.read_u64::<LittleEndian>().unwrap(), 0x2c);

    mmap.seek(SeekFrom::Start(3_000_000)).unwrap();
    mmap.write_u8(1).unwrap();
    assert_eq!(initial_ptr, mmap.map_mut_ptr());

    mmap.seek(SeekFrom::Start(3_000_000)).unwrap();
    assert_eq!(mmap.read_u8().unwrap(), 1);

    mmap.flush_all().unwrap();

    mmap.truncate(0).unwrap();
    mmap.seek(SeekFrom::Start(0)).unwrap();

    let mut buf = [0];
    let got = mmap.read(&mut buf).unwrap();
    assert_eq!(got, 0);
    mmap.write_u8(42).unwrap();
    mmap.write_u8(42).unwrap();
    mmap.seek(SeekFrom::Start(0)).unwrap();
    assert_eq!(mmap.read_u8().unwrap(), 42);
}

pub struct DummyMessage<T> {
    phantom_data: std::marker::PhantomData<T>,
}
impl<T> Debug for DummyMessage<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DummyMessage")
    }
}

impl<T: Object> MessagePayload for DummyMessage<T> {
    type Root = T;

    fn apply(&self, _time: NoatunTime, _root: Pin<&mut Self::Root>) {
        unimplemented!()
    }

    fn deserialize(_buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn serialize<W: Write>(&self, _writer: W) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Clone, Copy, AnyBitPattern)]
#[repr(C)]
struct CounterObject {
    counter: DatabaseCell<u32>,
    counter2: DatabaseCell<u32>,
}

impl Object for CounterObject {
    type Ptr = ThinPtr;
    type DetachedType = ();
    type DetachedOwnedType = ();

    fn detach(&self) -> Self::DetachedOwnedType {
        todo!()
    }

    fn clear(self: Pin<&mut Self>) {
        unsafe {
            let tself = self.get_unchecked_mut();
            Pin::new_unchecked(&mut tself.counter).clear();
            Pin::new_unchecked(&mut tself.counter2).clear();
        }
    }

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {
        todo!()
    }

    unsafe fn allocate_from_detached<'a>(_detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        todo!()
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_object_mut(index) }
    }
}

impl CounterObject {
    fn set_counter(mut self: Pin<&mut Self>, value1: u32, value2: u32) {
        unsafe {
            self.as_mut()
                .map_unchecked_mut(|x| &mut x.counter)
                .set(value1);
            self.as_mut()
                .map_unchecked_mut(|x| &mut x.counter2)
                .set(value2);
        }
    }
}

impl Application for CounterObject {
    type Message = CounterMessage;
    type Params = ();

    fn initialize_root<'a>(_params: &Self::Params) -> Pin<&'a mut Self> {
        let new_obj = NoatunContext.allocate_pod();
        new_obj
    }
}

#[derive(Debug)]
struct IncrementMessage {
    increment_by: u32,
}

impl MessagePayload for IncrementMessage {
    type Root = CounterObject;

    fn apply(&self, _time: NoatunTime, _root: Pin<&mut Self::Root>) {
        unimplemented!()
    }

    fn deserialize(_buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn serialize<W: Write>(&self, _writer: W) -> Result<()> {
        unimplemented!()
    }
}

#[test]
fn test1() {
    let mut db: Database<CounterObject> = Database::create_new(
        "test/test1.bin",
        true,
        1000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings::default(),
        (),
    )
    .unwrap();

    db.with_root_mut(|counter| unsafe {
        let counter = counter.get_unchecked_mut();
        assert_eq!(counter.counter.get(), 0);
        Pin::new_unchecked(&mut counter.counter).set(42);
        Pin::new_unchecked(&mut counter.counter2).set(43);
        Pin::new_unchecked(&mut counter.counter).set(44);

        assert_eq!(counter.counter.get(), 44);
        assert_eq!(counter.counter.get(), 44);
        assert_eq!(counter.counter2.get(), 43);
    })
    .unwrap();
}

#[derive(Debug, Clone, Savefile)]
struct CounterMessage {
    id: MessageId,
    parent: Vec<MessageId>,
    inc1: i32,
    set1: u32,
}
impl CounterMessage {
    fn wrap(&self) -> Message<CounterMessage> {
        Message::new(self.id, self.parent.clone(), self.clone())
    }
}
impl MessagePayload for CounterMessage {
    type Root = CounterObject;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut CounterObject>) {
        unsafe {
            if self.inc1 != 0 {
                let val = root.counter.get().saturating_add_signed(self.inc1);
                root.map_unchecked_mut(|x| &mut x.counter).set(val);
            } else {
                root.map_unchecked_mut(|x| &mut x.counter).set(self.set1);
            }
        }
    }

    fn deserialize(buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(load_noschema(&mut Cursor::new(buf), 1)?)
    }

    fn serialize<W: Write>(&self, mut writer: W) -> Result<()> {
        Ok(save_noschema(&mut writer, 1, self)?)
    }
}
#[test]
fn test_projection_time_limit() {
    let mut db: Database<CounterObject> = Database::create_new(
        "test/msg_store_time_limit.bin",
        true,
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            projection_time_limit: Some(datetime!(2024-01-02 00:00:00 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();

    db.append_single(
        CounterMessage {
            parent: vec![],
            id: MessageId::from_parts(datetime!(2024-01-01 00:00:00 Z).into(), [0; 10])
                .unwrap(),
            inc1: 1,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();

    db.mark_transmitted(MessageId::new_debug(0x100)).unwrap();

    db.append_single(
        CounterMessage {
            parent: vec![],
            id: MessageId::from_parts(datetime!(2024-01-02 00:00:00 Z).into(), [0; 10])
                .unwrap(),
            inc1: 1,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.append_single(
        CounterMessage {
            parent: vec![],
            id: MessageId::from_parts(datetime!(2024-01-03 00:00:00 Z).into(), [0; 10])
                .unwrap(),
            inc1: 1, //This is never projected, because of time limit
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();

    db.with_root_mut(|root| {
        // Time limit means last message isn't projected
        assert_eq!(root.counter.get(), 2);
    })
    .unwrap();

    db.with_root_preview(
        datetime!(2024-01-03 00:00:00 Z),
        [CounterMessage {
            parent: vec![],
            id: MessageId::from_parts(datetime!(2024-01-03 00:00:00 Z).into(), [0; 10])
                .unwrap(),
            inc1: 2,
            set1: 0,
        }]
        .into_iter(),
        |root| {
            assert_eq!(root.counter.get(), 4);
        },
    )
    .unwrap();

    db.with_root_mut(|root| {
        // Time limit means last message isn't projected
        assert_eq!(root.counter.get(), 2);
    })
    .unwrap();
}
#[test]
fn test_msg_store_real() {
    let mut db: Database<CounterObject> = Database::create_new(
        "test/msg_store.bin",
        true,
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings::default(),
        (),
    )
    .unwrap();

    db.append_single(
        CounterMessage {
            parent: vec![],
            id: MessageId::new_debug(0x100),
            inc1: 2,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();

    db.mark_transmitted(MessageId::new_debug(0x100)).unwrap();

    db.append_single(
        CounterMessage {
            parent: vec![MessageId::new_debug(0x100)],
            id: MessageId::new_debug(0x101),
            inc1: 0,
            set1: 42,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.append_single(
        CounterMessage {
            parent: vec![MessageId::new_debug(0x101)],
            id: MessageId::new_debug(0x102),
            inc1: 1,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();

    println!("Update heads: {:?}", db.get_update_heads());
    // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

    db.with_root_mut(|root| {
        assert_eq!(root.counter.get(), 43);
    })
    .unwrap();
}

#[test]
fn test_msg_store_inmem_miri() {
    let mut db: Database<CounterObject> = Database::create_in_memory(
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(datetime!(2021-01-01 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();

    db.append_single(
        CounterMessage {
            parent: vec![],
            id: MessageId::new_debug(0x100),
            inc1: 2,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.append_single(
        CounterMessage {
            parent: vec![MessageId::new_debug(0x100)],
            id: MessageId::new_debug(0x101),
            inc1: 0,
            set1: 42,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.append_single(
        CounterMessage {
            parent: vec![MessageId::new_debug(0x101)],
            id: MessageId::new_debug(0x102),
            inc1: 1,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();

    // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);
    assert!(!db.contains_message(MessageId::new_debug(0x100)).unwrap());
    assert!(db.contains_message(MessageId::new_debug(0x101)).unwrap());
    assert!(db.contains_message(MessageId::new_debug(0x102)).unwrap());

    db.with_root_mut(|root| {
        assert_eq!(root.counter.get(), 43);
    })
    .unwrap();
}

#[test]
fn test_msg_store_after_cutoff_inmem_miri() {
    let mut db: Database<CounterObject> = Database::create_in_memory(
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(datetime!(2024-01-01 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();

    let m1 = MessageId::from_parts(datetime!(2024-01-01 Z).into(), [0u8; 10]).unwrap();
    db.append_single(
        CounterMessage {
            parent: vec![],
            id: m1,
            inc1: 2,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.mark_transmitted(m1).unwrap();
    let m2 = MessageId::from_parts(datetime!(2024-01-01 Z).into(), [1u8; 10]).unwrap();
    db.append_single(
        CounterMessage {
            parent: vec![MessageId::new_debug(0x100)],
            id: m2,
            inc1: 0,
            set1: 42,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.set_mock_time(datetime!(2024-01-10 Z).into()).unwrap();
    db.reproject().unwrap();
    println!("Appending 2nd");
    let m3 = MessageId::from_parts(datetime!(2024-01-10 Z).into(), [2u8; 10]).unwrap();
    db.append_single(
        CounterMessage {
            parent: vec![MessageId::new_debug(0x101)],
            id: m3,
            inc1: 1,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();

    assert!(!db.contains_message(m1).unwrap());
    assert!(db.contains_message(m2).unwrap());
    assert!(db.contains_message(m3).unwrap());

    db.with_root_mut(|root| {
        assert_eq!(root.counter.get(), 43);
    })
    .unwrap();
}

#[test]
fn test_cutoff_handling() {
    let mut db: Database<CounterObject> =
        Database::create_in_memory(10000, CutOffDuration::from_minutes(15),
                                   DatabaseSettings::default(),
                                   ())
            .unwrap();

    db.append_single(
        CounterMessage {
            id: MessageId::new_debug(0x100),
            parent: vec![],
            inc1: 2,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.append_single(
        CounterMessage {
            id: MessageId::new_debug(0x101),
            parent: vec![MessageId::new_debug(0x100)],
            inc1: 0,
            set1: 42,
        }
        .wrap(),
        true,
    )
    .unwrap();
    db.append_single(
        CounterMessage {
            id: MessageId::new_debug(0x102),
            parent: vec![MessageId::new_debug(0x101)],
            inc1: 1,
            set1: 0,
        }
        .wrap(),
        true,
    )
    .unwrap();


    /*
    let mut d = distributor::Distributor::new("1");

    //println!("Heads: {:?}", d.get_periodic_message(&db));
    let r = d
        .receive_message(
            &mut db,
            std::iter::once(DistributorMessage::RequestUpstream {
                query: vec![(MessageId::new_debug(0x102), 2)],
            }),
        )
        .unwrap();
    println!("Clarify: {:?}", r);
    */

    // Fix, this is what was done here before: messages.apply_missing_messages(&mut db);

    db.with_root_mut(|root| {
        assert_eq!(root.counter.get(), 43);
    })
    .unwrap();
}

#[test]
fn test_handle() {
    let db: Database<DatabaseObjectHandle<DatabaseCell<u32>>> = Database::create_new(
        "test/test_handle.bin",
        true,
        1000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings::default(),
        (),
    )
    .unwrap();

    db.with_root(|handle| {
        assert_eq!(handle.get().get(), 43);
    });
}

impl Application for DatabaseObjectHandle<DatabaseCell<u32>> {
    type Message = DummyMessage<DatabaseObjectHandle<DatabaseCell<u32>>>;
    type Params = ();

    fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
        let obj = DatabaseObjectHandle::allocate(DatabaseCell::new(43u32));
        obj
    }
}
impl Application for DatabaseObjectHandle<[DatabaseCell<u8>]> {
    type Message = DummyMessage<DatabaseObjectHandle<[DatabaseCell<u8>]>>;
    type Params = ();

    fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
        let obj = DatabaseObjectHandle::allocate_unsized(
            [43u8, 45].map(DatabaseCell::new).as_slice(),
        );
        obj
    }
}

#[test]
fn test_handle_to_unsized_miri() {
    let db: Database<DatabaseObjectHandle<[DatabaseCell<u8>]>> = Database::create_in_memory(
        1000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(datetime!(2021-01-01 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();

    db.with_root(|handle| {
        assert_eq!(handle.get().observe(), &[43, 45]);
    });
}

#[test]
fn test_handle_miri() {
    let mut db: Database<DatabaseObjectHandle<DatabaseCell<u32>>> = Database::create_in_memory(
        1000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(datetime!(2021-01-01 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();

    db.with_root(|handle| {
        assert_eq!(handle.get().get(), 43);
    });

    db.with_root_mut(|root| {
        let a1 = root.getmut();
        assert_eq!(a1.get(), 43);
    })
    .unwrap();
}
impl Application for DatabaseVec<CounterObject> {
    type Params = ();

    fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
        let obj: Pin<&mut DatabaseVec<CounterObject>> = DatabaseVec::new();
        obj
    }

    type Message = DummyMessage<DatabaseVec<CounterObject>>;
}
impl Application for NoatunString {
    type Params = ();

    fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
        unsafe { NoatunString::allocate_from_detached("hello") }
    }

    type Message = DummyMessage<NoatunString>;
}

#[test]
fn test_string0() {
    let mut db: Database<NoatunString> = Database::create_new(
        "test/test_string0",
        true,
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings::default(),
        (),
    )
    .unwrap();
    db.with_root_mut(|mut test_str| {
        assert_eq!(test_str.len(), 5);
        assert_eq!(test_str.get(), "hello");
        let ptr = test_str.get().as_ptr();
        test_str.as_mut().assign("hell");
        assert_eq!(ptr, test_str.get().as_ptr());
        assert_eq!(test_str.get(), "hell");
        test_str.as_mut().assign("hello world!");
        assert_eq!(test_str.get(), "hello world!");
    })
    .unwrap();
}

#[test]
fn test_vec0() {
    let mut db: Database<DatabaseVec<CounterObject>> = Database::create_new(
        "test/test_vec0",
        true,
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings::default(),
        (),
    )
    .unwrap();
    db.with_root_mut(|mut counter_vec| {
        unsafe {
            assert_eq!(counter_vec.len(), 0);

            let _new_element = counter_vec.as_mut().push_zeroed();
            let new_element = counter_vec.as_mut().getmut(0);

            new_element.map_unchecked_mut(|x| &mut x.counter).set(47);
            let new_element = counter_vec.as_mut().push_zeroed();
            new_element.map_unchecked_mut(|x| &mut x.counter).set(48);

            assert_eq!(counter_vec.len(), 2);

            let item = counter_vec.as_mut().getmut(1);
            //let item2 = counter_vec.get_mut(context, 1);
            assert_eq!(item.counter.get(), 48);
            //assert_eq!(*item2.counter, 48);

            for _ in 0..10 {
                let _new_element = counter_vec.as_mut().push_zeroed();
            }

            let item = counter_vec.as_mut().getmut(1);
            assert_eq!(item.counter.get(), 48);
        }
    })
    .unwrap();
}

#[test]
fn test_vec_miri0() {
    let mut db: Database<DatabaseVec<CounterObject>> = Database::create_in_memory(
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(datetime!(2021-01-01 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();
    db.with_root_mut(|mut counter_vec| {
        assert_eq!(counter_vec.len(), 0);

        let _new_element = counter_vec.as_mut().push_zeroed();

        let new_element = counter_vec.as_mut().getmut(0);

        unsafe {
            new_element.map_unchecked_mut(|x| &mut x.counter).set(47);
        }
        let new_element = counter_vec.as_mut().push_zeroed();
        unsafe {
            new_element.map_unchecked_mut(|x| &mut x.counter).set(48);
        }

        assert_eq!(counter_vec.len(), 2);

        let item = counter_vec.as_mut().getmut(1);
        //let item2 = counter_vec.get_mut(context, 1);
        assert_eq!(item.counter.get(), 48);
        //assert_eq!(*item2.counter, 48);

        for _i in 0..10 {
            let _new_element = counter_vec.as_mut().push_zeroed();
        }

        let item = counter_vec.as_mut().getmut(1);
        assert_eq!(item.counter.get(), 48);
        assert_eq!(counter_vec.len(), 12);

        counter_vec.as_mut().shift_remove(1);
        assert_eq!(counter_vec.len(), 11);
        assert_eq!(counter_vec.as_mut().get(0).counter.get(), 47);

        counter_vec.as_mut().retain(|x| x.counter.get() == 0);
        assert_eq!(counter_vec.len(), 10);

        for i in 0..10 {
            assert_eq!(counter_vec.get(i).counter.get() as usize, 0);
        }
    })
    .unwrap();
}
#[test]
fn test_vec_undo() {
    let mut db: Database<DatabaseVec<CounterObject>> = Database::create_new(
        "test/vec_undo",
        true,
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings::default(),
        (),
    )
    .unwrap();

    {
        db.with_root_mut(|mut counter_vec| {
            NoatunContext.set_next_seqnr(SequenceNr::from_index(1));
            assert_eq!(counter_vec.len(), 0);

            let mut new_element = counter_vec.as_mut().push_zeroed();
            unsafe {
                new_element
                    .as_mut()
                    .map_unchecked_mut(|x| &mut x.counter)
                    .set(47);
                new_element
                    .as_mut()
                    .map_unchecked_mut(|x| &mut x.counter2)
                    .set(48);
            }

            NoatunContext.set_next_seqnr(SequenceNr::from_index(2));
            assert_eq!(counter_vec.len(), 1);
            NoatunContext.set_next_seqnr(SequenceNr::from_index(3));
        })
        .unwrap();
    }

    {
        db.with_root_mut(|counter_vec| {
            let mut counter = counter_vec.getmut(0);

            unsafe {
                counter
                    .as_mut()
                    .map_unchecked_mut(|x| &mut x.counter)
                    .set(50);
                NoatunContext.rewind(SequenceNr::from_index(2));
            }
            assert_eq!(counter.counter.get(), 47);
        })
        .unwrap();
    }

    db.force_rewind(SequenceNr::from_index(1));

    {
        db.with_root_mut(|counter_vec| {
            assert_eq!(counter_vec.len(), 0);
        })
        .unwrap();
    }
}

#[test]
fn test_object_macro() {
    use crate::data_types::DatabaseVec;
    noatun_object!(
        struct Kalle {
            pod hej:u32,
            pod tva:u32,
            object da: DatabaseVec<DatabaseCell<u32>>
        }
    );
    noatun_object!(
        struct Nalle {
            pod hej:u32,
            pod tva:u32,
            object da: DatabaseVec<DatabaseCell<u32>>
        }
    );
}

#[allow(clippy::assertions_on_constants)]
#[test]
fn test_id_generation_must_be_random() {
    assert!(!FOR_TEST_NON_RANDOM_ID);
}
