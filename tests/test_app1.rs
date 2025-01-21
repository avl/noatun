use bytemuck::{Pod, Zeroable};
use datetime_literal::datetime;
use noatun::data_types::{DatabaseCell, DatabaseObjectHandle, DatabaseVec};
use noatun::database::Database;
use noatun::{Application, Message, MessageHeader, MessageId, MessagePayload, NoatunContext, NoatunTime, Object, ThinPtr};
use savefile_derive::Savefile;
use std::io::{Cursor, Write};
use std::pin::Pin;
use std::time::Duration;

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct CounterObject {
    counter: DatabaseCell<u32>,
    counter2: DatabaseVec<DatabaseObjectHandle<[DatabaseCell<u8>]>>,
}

impl Object for CounterObject {
    type Ptr = ThinPtr;
    type DetachedType = ();

    unsafe fn init_from_detached(&mut self, detached: Self::DetachedType) {
        todo!()
    }

    unsafe fn allocate_from_detached<'a>(detached: Self::DetachedType) -> &'a mut Self {
        let this: &mut CounterObject = NoatunContext.allocate_pod();
        this.init_from_detached(detached);
        this
    }

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> &'a mut Self {
        unsafe { NoatunContext.access_pod_mut(index) }
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
        println!(
            "Applying message {} {} {}",
            self.id, self.counter, self.delta
        );

        let counter = root.as_ref().counter.get();
        root.counter
            .set(counter + self.delta);

        unsafe { Pin::new_unchecked (&mut root.counter2).push(vec![self.delta as u8]); }
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

    fn initialize_root<'a>(_params: &()) -> &'a mut Self {
        let ctr = NoatunContext.allocate_pod::<CounterObject>();
        ctr
    }
}

#[test]
fn test_counter_object_miri() {
    let mut db: Database<CounterObject> = Database::create_in_memory(
        10_000,
        Duration::from_secs(1000),
        Some(datetime!(2023-01-01 Z)),
        None,
        ()
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


    db.with_root(|root|  {
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

    db.with_root(|root|{
        assert_eq!(root.counter.get(), 85);
        assert_eq!(root.counter2.len(), 2);
        let vec_elem = root.counter2.get( 0);
        let arr = vec_elem.get();
        let arr_item = &arr[0];
        assert_eq!(arr_item.get(), 43u8);
    });
}
