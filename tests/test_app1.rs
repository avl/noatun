use bytemuck::{Pod, Zeroable};
use datetime_literal::datetime;
use noatun::data_types::{DatabaseCell, DatabaseObjectHandle, DatabaseVec};
use noatun::database::Database;
use noatun::{Application, DatabaseContextData, Message, MessageHeader, MessageId, MessagePayload, NoatunContext, Object, ThinPtr};
use savefile_derive::Savefile;
use std::io::{Cursor, Write};
use std::time::Duration;

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct CounterObject {
    counter: DatabaseCell<u32>,
    counter2: DatabaseVec<DatabaseObjectHandle<[DatabaseCell<u8>]>>,
}

impl Object for CounterObject {
    type Ptr = ThinPtr;

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


    fn apply(&self, root: &mut Self::Root) {
        println!(
            "Applying message {} {} {}",
            self.id, self.counter, self.delta
        );

        root.counter
            .set(root.counter.get() + self.delta);
        let cell: &mut DatabaseCell<u32> = DatabaseCell::allocate();
        cell.set(self.delta);
        let cell_slice =
            unsafe { std::slice::from_raw_parts_mut(cell as *mut DatabaseCell<u32>, 1) };

        let handle = DatabaseObjectHandle::new(NoatunContext.index_of(cell_slice));
        root.counter2.push(handle);
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

    fn initialize_root(ctx: &mut DatabaseContextData) -> &mut Self {
        let ctr = ctx.allocate_pod::<CounterObject>();
        ctr
    }
}

#[test]
fn test_counter_object_miri() {
    let mut db: Database<CounterObject> = Database::create_in_memory(
        10_000,
        Duration::from_secs(1000),
        Some(datetime!(2023-01-01 Z)),
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

    /*
    TODO
    Finish big refactoring:

    1: Message is now a new type, that carries parents and id,
    message payload is now the only user-changable part.

    2: The way we store parents in the db has changed, see the smallvec-like new datastructure
    in the store

    3: We have prepared to store child-info in db. This is needed to be able to actually send
    'Message + all descendants!'

     */

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
