use bytemuck::{Pod, Zeroable};
use noatun::database::Database;
use noatun::{Application, DatabaseContext, Message, MessageId, Object, ThinPtr};
use noatun::data_types::DatabaseCell;
use savefile_derive::Savefile;
use std::io::{Cursor, Write};

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct CounterObject {
    counter: DatabaseCell<u32>,
    counter2: DatabaseCell<u32>,
}

struct CounterApplication;

impl Object for CounterObject {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        context.access_pod(index)
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
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

#[derive(Debug, Savefile)]
struct CounterMessage {
    id: u32,
    counter: u8,
    delta: u32,
}

impl Message for CounterMessage {
    type Root = CounterObject;

    fn id(&self) -> MessageId {
        MessageId::new_debug(self.id)
    }

    fn parents(&self) -> impl ExactSizeIterator<Item = MessageId> {
        std::iter::empty()
    }

    fn apply(&self, context: &mut DatabaseContext, root: &mut Self::Root) {
        println!(
            "Applying message {} {} {}",
            self.id, self.counter, self.delta
        );
        println!(
            "  Prev values: {} {}",
            root.counter.get(context),
            root.counter2.get(context)
        );
        if self.counter == 0 {
            root.counter
                .set(context, root.counter.get(context) + self.delta);
        } else {
            root.counter2
                .set(context, root.counter2.get(context) + self.delta);
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
        10_000,
    )
    .unwrap();

    db.append_single(CounterMessage {
        id: 2,
        counter: 0,
        delta: 42,
    })
    .unwrap();

    let (root, context) = db.get_root();
    assert_eq!(root.counter.get(context), 42);

    db.append_single(CounterMessage {
        id: 1,
        counter: 1,
        delta: 43,
    })
    .unwrap();

    let (root, context) = db.get_root();
    assert_eq!(root.counter.get(context), 42);
    assert_eq!(root.counter2.get(context), 43);
}
