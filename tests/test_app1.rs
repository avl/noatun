use bytemuck::{Pod, Zeroable};
use noatun::{Application, Database, DatabaseCell, DatabaseContext, Object, SequenceNr, ThinPtr};

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
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

impl<'a> CounterObject {
    fn set_counter(&mut self, ctx: &mut DatabaseContext, value1: u32, value2: u32) {
        self.counter.set(ctx, value1);
        self.counter2.set(ctx, value2);
    }
    fn new(ctx: &mut DatabaseContext) -> &mut CounterObject {
        let counter: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
        assert_eq!(ctx.index_of(counter).0, 0);
        let counter2: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
        assert_eq!(ctx.index_of(counter2).0, 4);
        unsafe { Self::access_mut(ctx, ThinPtr(0)) }
    }
}

impl Application for CounterApplication {
    type Root = CounterObject;

    fn initialize_root(ctx: &mut DatabaseContext) -> &mut CounterObject {
        unsafe { ctx.allocate_pod::<CounterObject>() }
    }

    fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext, ptr: ThinPtr) -> &'a mut Self::Root {
        unsafe { ctx.access_pod_mut(ptr) }
    }
}

#[test]
fn test_counter_object() {
    let mut db: Database<CounterApplication> = Database::create_new("test/integration/test_counter_object.bin",CounterApplication).unwrap();
    let (counter, context) = db.get_root();
    context.set_next_seqnr(SequenceNr::from_index(1));
    assert_eq!(counter.counter.get(context), 0);
    counter.counter.set(context, 42);
    counter.counter2.set(context, 43);
    counter.counter.set(context, 44);

    assert_eq!(*counter.counter, 44);
}
#[test]
fn test_counter_mayhem() {
    let mut db1: Database<CounterApplication> = Database::create_new("test/integration/test_counter_mayhem.bin", CounterApplication).unwrap();
    let (_counter1, context1) = db1.get_root();
    context1.set_next_seqnr(SequenceNr::from_index(1));
    drop(db1);
    let mut db2: Database<CounterApplication> = Database::create_new("test/integration/test_counter_mayhem2.bin", CounterApplication).unwrap();

    let (counter2, context2) = db2.get_root();
    context2.set_next_seqnr(SequenceNr::from_index(1));
    counter2.counter.set(context2, 42);
}
