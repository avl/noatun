use bytemuck::AnyBitPattern;
use datetime_literal::datetime;
use noatun::data_types::{DatabaseCell, DatabaseObjectHandle, DatabaseVec};
use noatun::database::{Database, DatabaseSettings};
use noatun::{
    Application, CutOffDuration, Message, MessageHeader, MessageId, MessagePayload, NoatunContext,
    NoatunTime, Object, ThinPtr,
};
use savefile_derive::Savefile;
use std::io::{Cursor, Write};
use std::pin::Pin;

#[derive(Clone, Copy, AnyBitPattern)]
#[repr(C)]
struct CounterObject {
    counter: DatabaseCell<u32>,
    counter2: DatabaseVec<DatabaseObjectHandle<[DatabaseCell<u8>]>>,
}

impl Object for CounterObject {
    type Ptr = ThinPtr;
    type DetachedType = ();
    type DetachedOwnedType = ();

    fn detach(&self) -> Self::DetachedOwnedType {
        todo!()
    }

    fn clear(self: Pin<&mut Self>) {
        todo!()
    }

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {
        todo!()
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut this: Pin<&mut CounterObject> = NoatunContext.allocate_pod();
        this.as_mut().init_from_detached(detached);
        this
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_object(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> Pin<&'a mut Self> {
        unsafe { NoatunContext.access_object_mut(index) }
    }
}

#[derive(Debug, Savefile)]
struct CounterMessage {
    id: u32,
    counter: u8,
    delta: u32,
}

impl MessagePayload for CounterMessage {
    type Root = CounterObject;

    fn apply(&self, _time: NoatunTime, mut root: Pin<&mut Self::Root>) {
        let root_counter;
        unsafe {
            root_counter = root.as_mut().map_unchecked_mut(|x| &mut x.counter);
        }

        /*println!(
            "Applying message {} {} {}",
            self.id, self.counter, self.delta
        );*/

        let counter = root_counter.get();
        root_counter.set(counter + self.delta);

        unsafe {
            let root_counter2 = root.as_mut().map_unchecked_mut(|x| &mut x.counter2);
            root_counter2.push([self.delta as u8]);
        }
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(savefile::load_noschema(&mut Cursor::new(buf), 1)?)
    }

    fn serialize<W: Write>(&self, mut writer: W) -> anyhow::Result<()> {
        Ok(savefile::save_noschema(&mut writer, 1, self)?)
    }
}

impl Application for CounterObject {
    type Message = CounterMessage;
    type Params = ();

    fn initialize_root<'a>(_params: &()) -> Pin<&'a mut Self> {
        let ctr = NoatunContext.allocate_pod::<CounterObject>();
        ctr
    }
}

#[test]
fn test_counter_object_miri() {
    let mut db: Database<CounterObject> = Database::create_in_memory(
        10_000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(datetime!(2023-01-01 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();

    db.append_single(
        Message {
            header: MessageHeader {
                id: MessageId::new_debug(2),
                parents: vec![],
            },
            payload: CounterMessage {
                id: 2,
                counter: 0,
                delta: 42,
            },
        },
        true,
    )
    .unwrap();

    db.with_root(|root| {
        assert_eq!(root.counter.get(), 42);
    });

    db.append_single(
        Message {
            header: MessageHeader {
                id: MessageId::new_debug(1),
                parents: vec![],
            },
            payload: CounterMessage {
                id: 1,
                counter: 1,
                delta: 43,
            },
        },
        true,
    )
    .unwrap();

    db.with_root(|root| {
        assert_eq!(root.counter.get(), 85);
        assert_eq!(root.counter2.len(), 2);
        let vec_elem = root.counter2.get(0);
        let arr = vec_elem.get();
        let arr_item = &arr[0];
        assert_eq!(arr_item.get(), 43u8);
    });
}
