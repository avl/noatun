use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::slice::SliceIndex;
use bytemuck::{bytes_of, Pod};
use crate::backing_store::{BackingStore};
use crate::buffer::DummyMemoryMappedBuffer;

pub(crate) mod backing_store;

pub(crate) mod undo_store;
pub(crate) mod buffer;

trait Message<Base:for<'a> Object<'a>> {
    fn apply(&self, database: Database<Base>);
}

trait Object<'a> {
    fn access(context: &'a DatabaseContext, index: usize) -> Self;
    fn index_of(&'a self, context: &'a DatabaseContext) -> usize;
}


#[derive(Default)]
struct DatabaseContext {
    data: DummyMemoryMappedBuffer
}

trait Application {
    type Root<'a>: Object<'a>;
    fn write_root(ctx: &DatabaseContext);
    fn get_root<'a>(ctx: &'a DatabaseContext) -> Self::Root<'a>;
}

struct Database<Base> {
    context: DatabaseContext,
    base: PhantomData<Base>
}

#[repr(transparent)]
struct DatabaseCell<'a, T> {
    value: &'a Cell<T>
}


impl<'a, T:Pod> DatabaseCell<'a, T> {
    fn get(&self) -> T {
        self.value.get()
    }
    fn set(&self, context: &DatabaseContext, value: T) {
        context.write(self, value);
        self.value.set(value);
    }
}
/*
struct ObjectHandle<'a, T:Object<'a>> {
    object_index: u64,

}*/


impl<'a, T: Pod> Object<'a> for DatabaseCell<'a, T> {
    fn access(context: &'a DatabaseContext, index: usize) -> Self {
        context.get_cell(index)
    }

    fn index_of(&'a self, context: &'a DatabaseContext) -> usize {
        let val_offset = self.value as *const _ as *const u8 as usize;
        let start_offset = context.start_ptr() as *const u8 as usize;
        val_offset - start_offset
    }
}

impl DatabaseContext {

    fn get_slice(&self, range: impl SliceIndex<[Cell<u8>], Output=[Cell<u8>]>) -> &[Cell<u8>] {
        &self.data.access(range)
    }

    fn start_ptr(&self) -> *const u8 {
        self.data.start_ptr()
    }

    fn get_array<const N:usize>(&self, index: usize) -> &[Cell<u8>;N] {
        self.data.access(index..index+4).try_into().unwrap()
    }
    fn write<'a, T:Pod>(&'a self,value: &DatabaseCell<'a, T>, new_value: T) {
        let index = self.data.index_of(value);
        println!("Index: {}", index);
        self.data.write(index, bytes_of(&new_value));
    }

    fn index_of<'a, T:Object<'a> + 'a>(&'a self, t:&'a T) -> usize {
        t.index_of(self)
    }

    fn get_cell<'a, T:Pod>(&'a self, index: usize) -> DatabaseCell<'a, T> {
        let slice : &[Cell<u8>] = self.data.access(index..index+4).try_into().unwrap();
        unsafe { DatabaseCell {
            value: &*(slice as *const _ as *const _)
        }}
    }

    fn allocate_cell<'a, T:Pod>(&'a self) -> DatabaseCell<'a, T> {
        let memory = self.data.allocate_dyn(
            std::mem::size_of::<DatabaseCell<T>>(),
            std::mem::align_of::<DatabaseCell<T>>()
        );
        unsafe {
            DatabaseCell {
                value: &*(memory as *const _ as *const _)
            }
        }
    }

}

impl<APP:Application> Database<APP> {
    fn get_root<'a>(&'a self) -> (APP::Root<'a>, &'a DatabaseContext) {
        (APP::get_root(&self.context), &self.context)
    }
    pub fn new() -> Database<APP>
    {
        let mut ctx = DatabaseContext::default();
        APP::write_root(&mut ctx);
        Database {
            context: ctx,
            base: PhantomData
        }
    }
}





#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use super::*;

    struct CounterObject<'a> {
        counter: DatabaseCell<'a, u32>,
    }
    impl<'a> Object<'a> for CounterObject<'a> {
        fn access(context: &'a DatabaseContext, index: usize) -> Self {
            CounterObject {
                counter: context.get_cell(index)
            }
        }

        fn index_of(&'a self, context: &'a DatabaseContext) -> usize {
            context.index_of(&self.counter)
        }
    }

    impl<'a> CounterObject<'a> {
        fn set_counter(&mut self, ctx: &DatabaseContext, value: u32) {
            self.counter.set(ctx, value);
        }
        fn new(ctx: &DatabaseContext) -> CounterObject {
            let counter = ctx.allocate_cell();
            assert_eq!(ctx.index_of(&counter), 0);
            CounterObject {
                counter
            }
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root<'a> = CounterObject<'a>;

        fn write_root(ctx: &DatabaseContext) {
            let new_obj = CounterObject::new(ctx);
            assert_eq!(ctx.index_of(&new_obj.counter), 0);
        }

        fn get_root<'a>(ctx: &'a DatabaseContext) -> Self::Root<'a> {
            CounterObject::access(ctx, 0)
        }
    }

    struct IncrementMessage {
        increment_by: u32,
    }



    #[test]
    fn test() {
        let mut db: Database<CounterApplication> = Database::new();

        let context = &db.context;

        let (mut counter, context) = db.get_root();
        assert_eq!(counter.counter.get(), 0);
        counter.counter.set(context, 42);
        assert_eq!(counter.counter.get(), 42);



    }

}
