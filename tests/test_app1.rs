use datetime_literal::datetime;
use noatun::data_types::{NoatunBox, NoatunCell, NoatunVec};
use noatun::database::{Database, DatabaseSettings};
use noatun::{
    Message, MessageFrame, MessageHeader, MessageId, NoatunContext, NoatunStorable, Object,
    SavefileMessageSerializer, SchemaHasher, ThinPtr,
};
use savefile_derive::Savefile;
use std::pin::Pin;

#[repr(C)]
struct CounterObject {
    counter: NoatunCell<u32>,
    counter2: NoatunVec<NoatunBox<[NoatunCell<u8>]>>,
}

unsafe impl NoatunStorable for CounterObject {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("noatun::test_app1::CounterObject/1");
    }
}

impl Object for CounterObject {
    type Ptr = ThinPtr;
    type NativeType = ();
    type NativeOwnedType = ();

    fn export(&self) -> Self::NativeOwnedType {
        unimplemented!()
    }

    fn destroy(self: Pin<&mut Self>) {
        unimplemented!()
    }

    fn init_from(self: Pin<&mut Self>, _external: &Self::NativeType) {
        unimplemented!()
    }

    unsafe fn allocate_from<'a>(external: &Self::NativeType) -> Pin<&'a mut Self> {
        let mut this: Pin<&mut CounterObject> = NoatunContext.allocate();
        this.as_mut().init_from(external);
        this
    }

    fn hash_object_schema(hasher: &mut SchemaHasher) {
        Self::hash_schema(hasher);
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
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, mut root: Pin<&mut Self::Root>) {
        let root_counter;
        unsafe {
            root_counter = root.as_mut().map_unchecked_mut(|x| &mut x.counter);
        }

        let counter = root_counter.get();
        root_counter.set(counter + self.delta);

        unsafe {
            let root_counter2 = root.as_mut().map_unchecked_mut(|x| &mut x.counter2);
            root_counter2.push([self.delta as u8]);
        }
    }
}

#[test]
fn test_counter_object_miri() {
    let mut db: Database<CounterMessage> = Database::create_in_memory(
        10_000,
        DatabaseSettings {
            mock_time: Some(datetime!(2023-01-01 Z).into()),
            ..Default::default()
        },
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
        let vec_elem = &root.counter2[0];
        let arr = vec_elem.get_inner();
        let arr_item = &arr[0];
        assert_eq!(arr_item.get(), 43u8);
    });
}
