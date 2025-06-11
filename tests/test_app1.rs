use datetime_literal::datetime;
use noatun::data_types::{NoatunBox, NoatunCell, NoatunVec};
use noatun::database::{Database, DatabaseSettings};
use noatun::{
    Application, Message, MessageFrame, MessageHeader, MessageId, NoatunContext,
    NoatunStorable, NoatunTime, Object, ThinPtr,
};
use savefile_derive::Savefile;
use std::io::{Cursor, Write};
use std::pin::Pin;

#[repr(C)]
struct CounterObject {
    counter: NoatunCell<u32>,
    counter2: NoatunVec<NoatunBox<[NoatunCell<u8>]>>,
}

unsafe impl NoatunStorable for CounterObject {}

impl Object for CounterObject {
    type Ptr = ThinPtr;
    type DetachedType = ();
    type DetachedOwnedType = ();

    fn detach(&self) -> Self::DetachedOwnedType {
        unimplemented!()
    }

    fn clear(self: Pin<&mut Self>) {
        unimplemented!()
    }

    fn init_from_detached(self: Pin<&mut Self>, _detached: &Self::DetachedType) {
        unimplemented!()
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut this: Pin<&mut CounterObject> = NoatunContext.allocate();
        this.as_mut().init_from_detached(detached);
        this
    }
}

#[derive(Debug, Savefile)]
struct CounterMessage {
    id: u32,
    counter: u8,
    delta: u32,
}

impl Message for CounterMessage {
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
}

#[test]
fn test_counter_object_miri() {
    let mut db: Database<CounterObject> = Database::create_in_memory(
        10_000,
        DatabaseSettings {
            mock_time: Some(datetime!(2023-01-01 Z).into()),
            ..Default::default()
        },
        (),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();

    db.append_single(
        &MessageFrame {
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
        &MessageFrame {
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
        let vec_elem = root.counter2[0];
        let arr = vec_elem.get_inner();
        let arr_item = &arr[0];
        assert_eq!(arr_item.get(), 43u8);
    });
}
