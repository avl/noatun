use bytemuck::{Pod, Zeroable};
use noatun::{
    Application, DatabaseCell, DatabaseContext, Message, MessageId, Object, SequenceNr,
    ThinPtr,
};
use std::io::Write;
use noatun::database::Database;

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct CounterObject {
    counter: DatabaseCell<u32>,
    counter2: DatabaseCell<u32>,
}

struct CounterApplication;

impl Object for CounterObject {
    type Ptr = ThinPtr;

    fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        context.access_pod(index)
    }

    fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        context.access_pod_mut(index)
    }
}

impl<'a> CounterObject {
    /*fn set_counter(&mut self, ctx: &mut DatabaseContext, value1: u32, value2: u32) {
        self.counter.set(ctx, value1);
        self.counter2.set(ctx, value2);
    }
    fn new(ctx: &mut DatabaseContext) -> &mut CounterObject {
        let counter: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
        assert_eq!(ctx.index_of(counter).0, 0);
        let counter2: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
        assert_eq!(ctx.index_of(counter2).0, 4);
        Self::access_mut(ctx, ThinPtr(0))
    }*/
}

#[derive(Debug)]
struct CounterMessage {
    _id: u32,
}

impl Message for CounterMessage {
    type Root = CounterObject;

    fn id(&self) -> MessageId {
        todo!()
    }

    fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
        std::iter::empty()
    }

    fn apply(&self, _context: &mut DatabaseContext, _root: &mut Self::Root) {
        todo!()
    }

    fn deserialize(_buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn serialize<W: Write>(&self, _writer: W) -> anyhow::Result<()> {
        todo!()
    }
}

impl Application for CounterApplication {
    type Root = CounterObject;
    type Message = CounterMessage;

    fn initialize_root(ctx: &mut DatabaseContext) -> ThinPtr {
        let ctr = ctx.allocate_pod::<CounterObject>();
        ctx.index_of(ctr)
    }
}

#[test]
fn test_counter_object() {
    let mut db: Database<CounterApplication> = Database::create_new(
        "test/integration/test_counter_object.bin",
        CounterApplication,
        true,
        10_000
    )
    .unwrap();
    db.with_root_mut(|counter, context, _| {
        context.set_next_seqnr(SequenceNr::from_index(1));
        assert_eq!(counter.counter.get(context), 0);
        counter.counter.set(context, 42);
        counter.counter2.set(context, 43);
        counter.counter.set(context, 44);

        assert_eq!(*counter.counter, 44);
    })
    .unwrap();
}
#[test]
fn test_counter_mayhem() {
    let mut db1: Database<CounterApplication> = Database::create_new(
        "test/integration/test_counter_mayhem.bin",
        CounterApplication,
        true,
        10_000
    )
    .unwrap();
    db1.with_root_mut(|_counter1, context1, _| {
        context1.set_next_seqnr(SequenceNr::from_index(1));
    })
    .unwrap();
    drop(db1);
    let mut db2: Database<CounterApplication> = Database::create_new(
        "test/integration/test_counter_mayhem2.bin",
        CounterApplication,
        true,
        10_000
    )
    .unwrap();

    db2.with_root_mut(|counter2, context2, _| {
        context2.set_next_seqnr(SequenceNr::from_index(1));
        counter2.counter.set(context2, 42);
    })
    .unwrap();
}
