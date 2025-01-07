use bytemuck::{Pod, Zeroable};
use noatun::data_types::{DatabaseCell, DatabaseObjectHandle, DatabaseVec};
use noatun::database::Database;
use noatun::{Application, DatabaseContext, Message, MessageId, Object, ThinPtr};
use savefile_derive::Savefile;
use std::io::{Cursor, Write};

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct CounterObject {
    counter: DatabaseCell<u32>,
    counter2: DatabaseVec<DatabaseObjectHandle<[DatabaseCell<u8>]>>,
}

struct CounterApplication;

impl Object for CounterObject {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe {
            let a: &mut CounterObject = context.access_pod_mut(index);
            a
        }
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

        root.counter
            .set(context, root.counter.get(context) + self.delta);
        let cell: &mut DatabaseCell<u32> = DatabaseCell::allocate(context);
        cell.set(context, self.delta);
        let cell_slice =
            unsafe { std::slice::from_raw_parts_mut(cell as *mut DatabaseCell<u32>, 1) };

        let handle = DatabaseObjectHandle::new(context.index_of(cell_slice));
        root.counter2.push(context, handle);
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
fn test_counter_object_miri() {
    let mut db: Database<CounterApplication> =
        Database::create_in_memory(CounterApplication, 10_000).unwrap();

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
    assert_eq!(root.counter.get(context), 85);
    assert_eq!(root.counter2.len(context), 2);
    let vec_elem = root.counter2.get(context, 0);
    let arr = vec_elem.get(context);
    let arr_item = &arr[0];
    assert_eq!(
        arr_item.get(context),
        43u8
    );
}
