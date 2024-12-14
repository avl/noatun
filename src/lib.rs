use crate::backing_store::BackingStore;
use crate::buffer::DummyMemoryMappedBuffer;
use bytemuck::{Pod, bytes_of, Zeroable};
use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};
use std::slice::SliceIndex;

pub(crate) mod backing_store;

pub(crate) mod buffer;
pub(crate) mod undo_store;

trait Message<Base: Object> {
    fn apply(&self, database: Database<Base>);
}


trait Object {
    unsafe fn access<'a>(context: &DatabaseContext, index: usize) -> &'a Self;
    unsafe fn access_mut<'a>(context: &DatabaseContext, index: usize) -> &'a mut Self;
    fn index_of<'a>(&self, context: &'a DatabaseContext) -> usize;
}
trait FixedSizeObject : Object {
    const SIZE: usize;
    const ALIGN: usize;
}


#[derive(Default)]
struct DatabaseContext {
    data: DummyMemoryMappedBuffer,
}

trait Application {
    type Root: Object;
    fn initialize_root(ctx: &DatabaseContext);
    fn get_root<'a>(&mut self, ctx: &'a DatabaseContext) -> &'a mut Self::Root;
}

struct Database<Base> {
    context: DatabaseContext,
    base: PhantomData<Base>,
}

#[repr(transparent)]
struct DatabaseCell<T> {
    value: Cell<T>,
}

impl<'a, T: Pod> DatabaseCell<T> {
    fn get(&self) -> T {
        self.value.get()
    }
    fn set(&self, context: &DatabaseContext, new_value: T) {
        let index = self.index_of(context);
        context.write(index, bytes_of(&new_value));
        self.value.set(new_value);
    }
}

#[repr(C)]
struct DatabaseVec<T> {
    length: usize,
    capacity: usize,
    data: usize,
    phantom_data: PhantomData<T>,
}

unsafe impl<T> Zeroable for DatabaseVec<T> {}

impl<T> Copy for DatabaseVec<T> {}

impl<T> Clone for DatabaseVec<T> {
    fn clone(&self) -> Self {
        todo!()
    }
}

unsafe impl<T> Pod for DatabaseVec<T> where T:'static{

}

impl<T> DatabaseVec<T> where T:FixedSizeObject + 'static{

    fn new<'a>(ctx: &DatabaseContext) -> &'a DatabaseVec<T> {
        unsafe { ctx.data.allocate_pod::<DatabaseVec<T>>() }
    }
    fn len(&self) -> usize {
        self.length
    }
    fn get<'a>(&self, ctx: &'a DatabaseContext, index: usize) -> &'a T {
        let offset = self.data + index * T::SIZE;
        unsafe { T::access(ctx, offset) }
    }
    fn get_mut<'a>(&mut self, ctx: &'a DatabaseContext, index: usize) -> &'a mut T {
        let offset = self.data + index * T::SIZE;
        unsafe { T::access_mut(ctx, offset) }
    }
}


impl<T> FixedSizeObject for DatabaseVec<T> where
    T: FixedSizeObject,
{
    const SIZE: usize = 24;
    const ALIGN: usize = 8;
}

impl<T> Object for DatabaseVec<T>
where
    T: FixedSizeObject,
{
    unsafe fn access<'a>(context: &DatabaseContext, index: usize) -> &'a Self {
        unsafe { &*(context.start_ptr().wrapping_add(index) as *const DatabaseVec<T>) }
    }
    unsafe fn access_mut<'a>(context: &DatabaseContext, index: usize) -> &'a mut Self {
        unsafe { &mut *(context.start_ptr().wrapping_add(index) as *mut DatabaseVec<T>) }
    }

    fn index_of(&self, context: &DatabaseContext) -> usize {
        let data_offset = self as *const _ as *const u8 as usize;
        let start_offset = context.start_ptr() as usize;
        data_offset - start_offset
    }
}



#[repr(transparent)]
struct DatabaseObjectHandle<T: Object> {
    object_index: usize,
    phantom: PhantomData<T>,
}
impl<T:Object> FixedSizeObject for DatabaseObjectHandle<T> {
    const SIZE: usize = 8;
    const ALIGN: usize = 8;
}
impl<T: Object> Object for DatabaseObjectHandle<T> {

    unsafe fn access<'a>(context: &DatabaseContext, index: usize) -> &'a Self {
        unsafe { &*(context.start_ptr().wrapping_add(index) as *const _ as *const DatabaseObjectHandle<T>) }
    }
    unsafe fn access_mut<'a>(context: &DatabaseContext, index: usize) -> &'a mut Self {
        unsafe { &mut *(context.start_ptr().wrapping_add(index) as *const _ as *mut DatabaseObjectHandle<T>) }
    }

    fn index_of(&self, context: &DatabaseContext) -> usize {
        self.object_index
    }
}
impl<T:Object + Pod> FixedSizeObject for DatabaseCell<T> {
    const SIZE: usize = std::mem::size_of::<T>();
    const ALIGN: usize  = std::mem::align_of::<T>();
}

impl<T:Pod> DatabaseCell<T> {
    fn new(context: &DatabaseContext) -> &mut Self {
        let memory = context.data.allocate_dyn(
            std::mem::size_of::<DatabaseCell<T>>(),
            std::mem::align_of::<DatabaseCell<T>>(),
        );
        unsafe {
            &mut *(memory as *mut _ as *mut DatabaseCell<T>)
        }
    }
}

impl<T: Pod> Object for DatabaseCell<T> {

    unsafe fn access<'a>(context: &DatabaseContext, index: usize) -> &'a Self {
        let slice: &[u8] = context.data.access(index..index + 4).try_into().unwrap();
        unsafe {
            &*(slice as *const _ as *const _)
        }
    }

    unsafe fn access_mut<'a>(context: &DatabaseContext, index: usize) -> &'a mut Self {
        let slice: &mut [u8] = context.data.access_mut(index..index + 4).try_into().unwrap();
        unsafe {
            &mut *(slice as *const _ as *mut Self)
        }
    }

    fn index_of(&self, context: &DatabaseContext) -> usize {
        let val_offset = self as *const _ as *const u8 as usize;
        let start_offset = context.start_ptr() as *const u8 as usize;
        val_offset - start_offset
    }
}

impl DatabaseContext {

    fn start_ptr(&self) -> *const u8 {
        self.data.start_ptr()
    }

    fn index_of<T: Object>(&self, t: &T) -> usize {
        t.index_of(self)
    }

    fn write(&self, index: usize, data: &[u8]) {
        self.data.write(index, data);
    }

}

impl<APP: Application> Database<APP> {
    fn get_root<'a>(&'a mut self, app: &'a mut APP) -> (&mut APP::Root, &'a DatabaseContext) {
        (APP::get_root(app, &self.context), &self.context)
    }
    pub fn new() -> Database<APP> {
        let mut ctx = DatabaseContext::default();
        APP::initialize_root(&mut ctx);
        Database {
            context: ctx,
            base: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    impl FixedSizeObject for CounterObject {
        const SIZE: usize = 4;
        const ALIGN: usize = 4;
    }
    struct CounterObject {
        counter: DatabaseCell< u32>,
    }
    impl Object for CounterObject {

        unsafe fn access<'a>(context: &DatabaseContext, index: usize) -> &'a Self {
            unsafe { &*(context.start_ptr().wrapping_add(index) as *const _ as *const _) }
        }

        unsafe fn access_mut<'a>(context: &DatabaseContext, index: usize) -> &'a mut Self {
            unsafe { &mut *(context.start_ptr().wrapping_add(index)  as *mut _) }
        }

        fn index_of(&self, context: &DatabaseContext) -> usize {
            context.index_of(&self.counter)
        }
    }

    impl<'a> CounterObject {
        fn set_counter(&mut self, ctx: &DatabaseContext, value: u32) {
            self.counter.set(ctx, value);
        }
        fn new(ctx: &DatabaseContext) -> &CounterObject {
            let counter: &DatabaseCell<u32> = DatabaseCell::new(ctx);
            assert_eq!(ctx.index_of(counter), 0);
            unsafe { &*(counter as *const _ as *const CounterObject) }
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root = CounterObject;

        fn initialize_root(ctx: &DatabaseContext) {
            let new_obj = CounterObject::new(ctx);
            assert_eq!(ctx.index_of(&new_obj.counter), 0);
        }

        fn get_root<'a>(&mut self, ctx: &'a DatabaseContext) -> &'a mut Self::Root {
            unsafe { CounterObject::access_mut(ctx, 0) }
        }
    }

    struct IncrementMessage {
        increment_by: u32,
    }


    #[test]
    fn test() {
        let mut db: Database<CounterApplication> = Database::new();

        let context = &db.context;

        let mut app = CounterApplication;
        let (mut counter, context) = db.get_root(&mut app);
        assert_eq!(counter.counter.get(), 0);
        counter.counter.set(context, 42);
        assert_eq!(counter.counter.get(), 42);
    }

    #[test]
    fn test_vec() {

        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &DatabaseContext) {
                let obj:&DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                assert_eq!(ctx.index_of(obj), 0);

            }

            fn get_root<'a>(&mut self, ctx: &'a DatabaseContext) -> &'a mut Self::Root {
                unsafe { DatabaseVec::access_mut(ctx, 0) }
            }
        }

        let mut db: Database<CounterVecApplication> = Database::new();

        let context = &db.context;

        let mut app = CounterVecApplication;
        let (mut counter_vec, context) = db.get_root(&mut app);
        assert_eq!(counter_vec.length, 0);

    }

}
