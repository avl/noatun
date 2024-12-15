use crate::backing_store::BackingStore;
use crate::buffer::DatabaseContext;
use bytemuck::{Pod, Zeroable};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::slice::SliceIndex;

pub(crate) mod backing_store;

pub(crate) mod buffer;
pub(crate) mod undo_store;

trait Message<Base: Object> {
    fn apply(&self, database: Database<Base>);
}


trait Pointer: Copy + Debug {
    fn start(self) -> usize;
}

trait Object {
    type Ptr: Pointer;
    unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self;
    unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self;

}

trait FixedSizeObject : Object + Sized + Pod {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
    unsafe fn access<'a>(context: &DatabaseContext, index: usize) -> &'a Self { unsafe {
        context.access_pod(index)
    }}

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: usize) -> &'a mut Self { unsafe {
        context.access_pod_mut(index)
    }}
}

impl FixedSizeObject for u32 {
}

impl Object for u32 {
    type Ptr = ThinPtr;

    unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self { unsafe {
        context.access_pod(index.0)
    }}

    unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self { unsafe {
        context.access_pod_mut(index.0)
    }}

}
impl Object for u8 {
    type Ptr = ThinPtr;

    unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self { unsafe {
        context.access_pod(index.0)
    }}

    unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self { unsafe {
        context.access_pod_mut(index.0)
    }}
}
trait Application {
    type Root: Object;
    fn initialize_root(ctx: &mut DatabaseContext);
    fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext) -> &'a mut Self::Root;
}

struct Database<Base> {
    context: DatabaseContext,
    app: Base,
}

#[repr(transparent)]
#[derive(Copy, Clone)]
struct DatabaseCell<T:Copy> {
    value: T,
}

#[derive(Copy, Clone, Debug)]
struct FatPtr {
    start: usize,
    len: usize,
}
impl FatPtr {
    pub fn from(start: usize, len: usize) -> FatPtr {
        FatPtr {
            start,
            len,
        }
    }
}
#[derive(Copy, Clone, Debug)]
struct ThinPtr(usize);

unsafe impl Zeroable for FatPtr {}

unsafe impl Pod for FatPtr {

}
unsafe impl Zeroable for ThinPtr {}

unsafe impl Pod for ThinPtr {

}

impl Pointer for ThinPtr {
    fn start(self) -> usize {
        self.0
    }

}

impl Pointer for FatPtr {
    fn start(self) -> usize {
        self.start
    }
}

impl Object for [u8] {
    type Ptr = FatPtr;

    unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access(index) }
    }

    unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_mut(index) }
    }


}

unsafe impl<T> Zeroable for DatabaseCell<T> where T: Pod {

}

unsafe impl<T> Pod for DatabaseCell<T>  where T:Pod{

}
impl<T: Pod> DatabaseCell<T> {
    fn get(&self) -> T {
        self.value
    }
    fn set(&mut self, context: &mut DatabaseContext, new_value: T) {
        let index = context.index_of(self);
        //context.write(index, bytes_of(&new_value));
        self.value = new_value;
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
        *self
    }
}

unsafe impl<T> Pod for DatabaseVec<T> where T:'static{

}

impl<T> DatabaseVec<T> where T:FixedSizeObject + 'static{

    pub fn new<'a>(ctx: &DatabaseContext) -> &'a DatabaseVec<T> {
        unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn get<'a>(&self, ctx: &'a DatabaseContext, index: usize) -> &'a T {
        let offset = self.data + index * T::SIZE;
        unsafe { T::access(ctx, offset) }
    }
    pub fn get_mut<'a>(&mut self, ctx: &'a mut DatabaseContext, index: usize) -> &'a mut T {
        let offset = self.data + index * T::SIZE;
        unsafe { T::access_mut(ctx, offset) }
    }

    fn realloc(&mut self, ctx: &mut DatabaseContext, new_capacity: usize) {
        let dest = ctx.allocate_dyn(new_capacity * T::SIZE, T::ALIGN);
        let dest_index = ctx.index_of(&dest[0]);
        if self.length > 0 {
            let old_ptr = FatPtr::from(self.data,T::SIZE*self.length);
            ctx.copy(old_ptr,
                     dest_index.0
            );
        }
        self.data = dest_index.0;
        self.capacity = new_capacity;
    }

    pub fn push(&mut self, ctx: &mut DatabaseContext, value: T) {
        if self.length >= self.capacity {
            self.realloc(ctx, (self.capacity+1)*2);
        }
        *self.get_mut(ctx, self.length) = value;
    }

}


impl<T> FixedSizeObject for DatabaseVec<T> where
    T: FixedSizeObject+'static,
{
    const SIZE: usize = 3*std::mem::size_of::<usize>();
    const ALIGN: usize = std::mem::align_of::<usize>();
}

impl<T> Object for DatabaseVec<T>
where
    T: FixedSizeObject+'static,
{
    type Ptr = ThinPtr;

    unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { &*(context.start_ptr().wrapping_add(index.0) as *const DatabaseVec<T>) }
    }
    unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index.0) }
    }


}



#[repr(transparent)]
struct DatabaseObjectHandle<T: Object> {
    object_index: T::Ptr,
    phantom: PhantomData<T>,
}

unsafe impl<T:Object> Zeroable for DatabaseObjectHandle<T> {}

impl<T:Object> Copy for DatabaseObjectHandle<T> {}

impl<T:Object> Clone for DatabaseObjectHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T:Object+'static> Pod for DatabaseObjectHandle<T> {

}
impl<T:Object+'static> FixedSizeObject for DatabaseObjectHandle<T> {
}
impl<T: Object> Object for DatabaseObjectHandle<T> {
    type Ptr = ThinPtr;

    unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {

        unsafe { &*(context.start_ptr().wrapping_add(index.0) as *const _ as *const DatabaseObjectHandle<T>) }
    }
    unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { &mut *(context.start_ptr().wrapping_add(index.0) as *const _ as *mut DatabaseObjectHandle<T>) }
    }


}

impl<T:Object> DatabaseObjectHandle<T> {
    pub fn get<'a>(&'a self, context: &DatabaseContext) -> &T {
        println!("Handle index: {:?}", self.object_index);
        unsafe { T::access_unsized::<'a>(context, self.object_index) }
    }
    pub fn get_mut<'a>(&'a self, context: &mut DatabaseContext) -> &mut T {
        unsafe { T::access_unsized_mut::<'a>(context, self.object_index) }
    }
    pub fn new<'a>(context: &DatabaseContext, value: T) -> &'a mut Self where T: Object<Ptr=ThinPtr>, T: Pod {
        let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
        let target = unsafe { context.allocate_pod::<T>() };
        *target = value;
        println!("Target addr: {:?}, store ptr: {:?}",
            target as *const T,
            context.start_ptr()
        );
        println!("Index: {:x?}", context.index_of_ptr(target as *const _));
        this.object_index = context.index_of_ptr(target as *const _);
        this

    }

}

impl<T:Object + Pod> FixedSizeObject for DatabaseCell<T> {
    const SIZE: usize = std::mem::size_of::<T>();
    const ALIGN: usize  = std::mem::align_of::<T>();
}

impl<T:Pod+Object> DatabaseCell<T> {
    fn new(context: &DatabaseContext) -> &mut Self {
        let memory = unsafe { context.allocate_pod::<T>() };
        unsafe {
            &mut *(memory as *mut _ as *mut DatabaseCell<T>)
        }
    }
}

impl<T: Pod> Object for DatabaseCell<T> {
    type Ptr = ThinPtr;

    unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self { unsafe {

        context.access_pod(index.0)
    }}

    unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self { unsafe {

        context.access_pod_mut(index.0)
    }}


}


impl<APP: Application> Database<APP> {
    fn get_root<'a>(&'a mut self) -> (&mut APP::Root, &mut DatabaseContext) {
        let root = APP::get_root(&mut self.app, &mut self.context);
        (root, &mut self.context)
    }
    pub fn create(app: APP) -> Database<APP> {
        let mut ctx = DatabaseContext::default();
        APP::initialize_root(&mut ctx);
        Database {
            context: ctx,
            app,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    

    impl FixedSizeObject for CounterObject {
        const SIZE: usize = 8;
        const ALIGN: usize = 4;
    }
    #[derive(Clone,Copy)]
    #[repr(C)]
    struct CounterObject {
        counter: DatabaseCell< u32>,
        counter2: DatabaseCell< u32>,
    }

    unsafe impl Zeroable for CounterObject {}

    unsafe impl Pod for CounterObject {

    }

    impl Object for CounterObject {
        type Ptr = ThinPtr;

        unsafe fn access_unsized<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self { unsafe {
            context.access_pod(index.0)
        }}

        unsafe fn access_unsized_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self { unsafe {
            context.access_pod_mut(index.0)
        }}


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
            unsafe { Self::access_mut(ctx, 0) }
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root = CounterObject;

        fn initialize_root(ctx: &mut DatabaseContext) {
            let new_obj = CounterObject::new(ctx);
            //assert_eq!(ctx.index_of(&new_obj.counter), 0);
        }

        fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext) -> &'a mut Self::Root {
            unsafe { CounterObject::access_mut(ctx, 0) }
        }
    }

    struct IncrementMessage {
        increment_by: u32,
    }


    #[test]
    fn test1() {
        let mut db: Database<CounterApplication> = Database::create(CounterApplication);

        let context = &db.context;

        let (counter, context) = db.get_root();
        assert_eq!(counter.counter.get(), 0);
        counter.counter.set(context, 42);
        assert_eq!(counter.counter.get(), 42);
    }

    #[test]
    fn test_handle() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<u32>;

            fn initialize_root(ctx: &mut DatabaseContext) {
                DatabaseObjectHandle::new(ctx, 43u32);
            }

            fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext) -> &'a mut Self::Root {
                unsafe { DatabaseObjectHandle::access_mut(ctx, 0) }
            }
        }

        let mut db: Database<HandleApplication> = Database::create(HandleApplication);

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(*handle.get(context), 43);

    }

    #[test]
    fn test_vec() {

        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) {
                let obj:&DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                assert_eq!(ctx.index_of(obj).0, 0);

            }

            fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext) -> &'a mut Self::Root {
                unsafe { DatabaseVec::access_mut(ctx, 0) }
            }
        }

        let mut db: Database<CounterVecApplication> = Database::create(CounterVecApplication);

        let app = CounterVecApplication;
        let (counter_vec, context) = db.get_root();
        assert_eq!(counter_vec.length, 0);

    }

}
