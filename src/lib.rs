#![allow(unused)]
#![allow(dead_code)]

use bytemuck::{Pod, Zeroable};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{transmute, transmute_copy};
use std::ops::Deref;
use std::slice::SliceIndex;
use std::cell::Cell;
pub use buffer::DatabaseContext;
pub(crate) mod backing_store;

pub(crate) mod buffer;
pub(crate) mod undo_store;

trait Message<Root: Object> {
    fn time(&self) -> u64;
    fn apply(&self, context: &mut DatabaseContext, root: &mut Root);
}



struct MessageStore<APP:Application, M:Message<APP::Root>> {
    messages: Vec<M>,
    phantom_data: PhantomData<*const APP::Root>
}

impl<App:Application, M:Message<App::Root>> MessageStore<App, M> {
    pub fn new() -> MessageStore<App, M> {
        MessageStore { messages: Vec::new(), phantom_data: PhantomData }
    }
    pub fn apply(&self, database: &mut Database<App>) {
        let (root, context) = database.get_root();

        let mut cur_time = context.time();
        for msg in self.messages.iter().skip(context.next_seqnr()) {
            let msg_time = msg.time();
            if msg_time < cur_time {
                panic!("A later message had an earlier timestamp");
            } else if msg_time != cur_time {
                context.set_time(msg_time);
            }
            msg.apply(context, root);
        }
        context.set_next_seqnr(self.messages.len());
    }
}


pub enum GenPtr {
    Thin(ThinPtr),
    Fat(FatPtr)
}


pub trait Pointer: Copy + Debug + 'static {
    fn start(self) -> usize;
    fn create<T>(addr: &T, buffer_start: *const u8) -> Self;
    fn as_generic(&self) -> GenPtr;
}

pub trait Object {
    type Ptr: Pointer;
    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self;
    unsafe fn access_mut<'a>(
        context: &mut DatabaseContext,
        index: Self::Ptr,
    ) -> &'a mut Self;
}

pub trait FixedSizeObject: Object<Ptr=ThinPtr> + Sized + Pod {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();

}

impl<T:Object<Ptr=ThinPtr>+Sized+Copy+Pod> FixedSizeObject for T {
    const SIZE: usize = std::mem::size_of::<Self>();
    const ALIGN: usize = std::mem::align_of::<Self>();
}


impl Object for usize {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(context: &mut DatabaseContext, index: Self::Ptr) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}


impl Object for u32 {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(
        context: &mut DatabaseContext,
        index: Self::Ptr,
    ) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
impl Object for u8 {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(
        context: &mut DatabaseContext,
        index: Self::Ptr,
    ) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
pub trait Application {
    type Root: Object;
    fn initialize_root(ctx: &mut DatabaseContext) -> &mut Self::Root;
    fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext, root_ptr: <Self::Root as Object>::Ptr) -> &'a mut Self::Root;
}

pub struct Database<Base> {
    context: DatabaseContext,
    app: Base,
}

#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct DatabaseCell<T: Copy> {
    value: T,
}

impl<T:Copy> Deref for DatabaseCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Copy, Clone, Debug)]
pub struct FatPtr {
    start: usize,
    len: usize,
}
impl FatPtr {
    pub fn from(start: usize, len: usize) -> FatPtr {
        FatPtr { start, len }
    }
}
#[derive(Copy, Clone, Debug)]
pub struct ThinPtr(pub usize);

unsafe impl Zeroable for FatPtr {}

unsafe impl Pod for FatPtr {}
unsafe impl Zeroable for ThinPtr {}

unsafe impl Pod for ThinPtr {}

impl Pointer for ThinPtr {
    fn start(self) -> usize {
        self.0
    }

    fn create<T>(addr: &T, buffer_start: *const u8) -> Self {
        let index = (addr as *const T as *const u8 as usize) - (buffer_start as usize);
        ThinPtr(index)
    }

    fn as_generic(&self) -> GenPtr {
        GenPtr::Thin(*self)
    }
}

impl Pointer for FatPtr {
    fn start(self) -> usize {
        self.start
    }
    fn create<T>(addr: &T, buffer_start: *const u8) -> Self {
        let addr: *const T = addr as *const T;
        let dummy : (*const u8, usize);
        assert_eq!(std::mem::size_of::<*const T>(), 2 * std::mem::size_of::<usize>());
        dummy = unsafe { transmute_copy(&addr) };
        FatPtr {
            start: ((dummy.0 as usize) - (buffer_start as usize)),
            len: dummy.1,
        }
    }
    fn as_generic(&self) -> GenPtr {
        GenPtr::Fat(*self)
    }
}

impl Object for [u8] {
    type Ptr = FatPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access(index) }
    }

    unsafe fn access_mut<'a>(
        context: &mut DatabaseContext,
        index: Self::Ptr,
    ) -> &'a mut Self {
        unsafe { context.access_mut(index) }
    }
}

unsafe impl<T> Zeroable for DatabaseCell<T> where T: Pod {}

unsafe impl<T> Pod for DatabaseCell<T> where T: Pod {}
impl<T: Pod> DatabaseCell<T> {
    pub fn get(&self) -> T {
        self.value
    }
    pub fn set<'a>(&'a mut self, context: &'a mut DatabaseContext, new_value: T) {
        let index = context.index_of(self);
        //context.write(index, bytes_of(&new_value));
        context.write_pod(new_value, &mut self.value);
    }
}



#[repr(C)]
pub struct DatabaseVec<T> {
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

unsafe impl<T> Pod for DatabaseVec<T> where T: 'static {}

impl<T> DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    pub fn new<'a>(ctx: &mut DatabaseContext) -> &'a mut DatabaseVec<T> {
        unsafe { ctx.allocate_pod::<DatabaseVec<T>>() }
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn get(&self, ctx: &DatabaseContext, index: usize) -> &T {
        let offset = self.data + index * T::SIZE;
        unsafe { T::access(ctx, ThinPtr(offset)) }
    }
    pub fn get_mut(&mut self, ctx: &mut DatabaseContext, index: usize) -> &mut T {
        let offset = self.data + index * T::SIZE;
        let t = unsafe { T::access_mut(ctx, ThinPtr(offset)) };
        t
    }

    fn realloc_add(&mut self, ctx: &mut DatabaseContext, new_capacity: usize) {
        let dest = ctx.allocate_raw(new_capacity * T::SIZE, T::ALIGN);
        let dest_index = ctx.index_of_ptr(dest);

        if self.length > 0 {
            let old_ptr = FatPtr::from(self.data, T::SIZE * self.length);
            println!("Copy old");
            ctx.copy(old_ptr, dest_index.0);
        }

        let new_len = self.length+1;

        println!("Write-pod with db.vec");

        ctx.write_pod(DatabaseVec {
            length: new_len,
            capacity: new_capacity,
            data: dest_index.0,
            phantom_data: Default::default(),
        }, self)
    }

    pub fn push_new<'a>(&'a mut self, ctx: &mut DatabaseContext) -> &'a mut T {
        if self.length >= self.capacity {
            println!("Realloc-leg");
            self.realloc_add(ctx, (self.capacity + 1) * 2);
        } else {
            println!("no-Realloc-leg");
            ctx.write_pod(self.length + 1, &mut self.length);
        }

        self.get_mut(ctx, self.length - 1)
    }
}


impl<T> Object for DatabaseVec<T>
where
    T: FixedSizeObject + 'static,
{
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { &*(context.start_ptr().wrapping_add(index.0) as *const DatabaseVec<T>) }
    }
    unsafe fn access_mut<'a>(
        context: &mut DatabaseContext,
        index: Self::Ptr,
    ) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}

#[repr(transparent)]
struct DatabaseObjectHandle<T: Object> {
    object_index: T::Ptr,
    phantom: PhantomData<T>,
}

unsafe impl<T: Object> Zeroable for DatabaseObjectHandle<T> {}

impl<T: Object> Copy for DatabaseObjectHandle<T> {}

impl<T: Object> Clone for DatabaseObjectHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T: Object + 'static> Pod for DatabaseObjectHandle<T> {}
impl<T: Object> Object for DatabaseObjectHandle<T> {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe {
            &*(context.start_ptr().wrapping_add(index.0) as *const _
                as *const DatabaseObjectHandle<T>)
        }
    }
    unsafe fn access_mut<'a>(
        context: &mut DatabaseContext,
        index: Self::Ptr,
    ) -> &'a mut Self {
        unsafe {
            &mut *(context.start_ptr().wrapping_add(index.0) as *const _
                as *mut DatabaseObjectHandle<T>)
        }
    }
}

impl<T: Object> DatabaseObjectHandle<T> {
    pub fn get<'a>(&'a self, context: &DatabaseContext) -> &'a T {
        unsafe { T::access(context, self.object_index) }
    }
    pub fn get_mut<'a>(&'a self, context: &mut DatabaseContext) -> &'a mut T {
        unsafe { T::access_mut(context, self.object_index) }
    }
    pub fn new<'a>(context: &mut DatabaseContext, value: T) -> &'a mut Self
    where
        T: Object<Ptr = ThinPtr>,
        T: Pod,
    {
        let this = unsafe { context.allocate_pod::<DatabaseObjectHandle<T>>() };
        let target = unsafe { context.allocate_pod::<T>() };
        *target = value;
        this.object_index = context.index_of_ptr(target as *const _);
        this
    }
}


impl<T: Pod + Object> DatabaseCell<T> {
    pub fn new<'a>(context: &mut DatabaseContext) -> &'a mut Self {
        let memory = unsafe { context.allocate_pod::<T>() };
        unsafe { &mut *(memory as *mut _ as *mut DatabaseCell<T>) }
    }
}

impl<T: Pod> Object for DatabaseCell<T> {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
        unsafe { context.access_pod(index.0) }
    }

    unsafe fn access_mut<'a>(
        context: &mut DatabaseContext,
        index: Self::Ptr,
    ) -> &'a mut Self {
        unsafe { context.access_pod_mut(index) }
    }
}
thread_local! {
    static MULTI_INSTANCE_BLOCKER: Cell<bool> = Cell::new(false);
}

impl<APP: Application> Database<APP> {
    pub fn get_root<'a>(&'a mut self) -> (&'a mut APP::Root, &'a mut DatabaseContext) {
        let root_ptr = self.context.get_root_ptr::<<APP::Root as Object>::Ptr>();
        let root = APP::get_root(&mut self.app, &mut self.context, root_ptr);
        (root, &mut self.context)
    }
    pub fn create(app: APP) -> Database<APP> {
        if MULTI_INSTANCE_BLOCKER.get() {
            if !std::env::var("NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE").is_ok() {
                panic!("Noatun: Multiple active DB-roots in the same thread are not allowed.\
                        You can disable this warning by setting env-var NOATUN_UNSAFE_ALLOW_MULTI_INSTANCE.\
                        Note, unsoundness can occur if DatabaseContext from one instance is used by data \
                        for other.");
            }
        }
        MULTI_INSTANCE_BLOCKER.set(true);

        let mut ctx = DatabaseContext::default();
        let start_ptr = ctx.start_ptr();
        let root = APP::initialize_root(&mut ctx);
        let root_ptr = <APP::Root as Object>::Ptr::create(root, start_ptr);
        ctx.set_root_ptr(root_ptr.as_generic());
        ctx.set_time(0);
        Database { context: ctx, app }
    }

    pub fn rewind(&mut self, time: u64) {
        self.context.rewind(time as u64);
    }
}
impl<APP> Drop for Database<APP> {
    fn drop(&mut self) {
        MULTI_INSTANCE_BLOCKER.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Zeroable, Pod)]
    #[repr(C)]
    struct CounterObject {
        counter: DatabaseCell<u32>,
        counter2: DatabaseCell<u32>,
    }


    impl Object for CounterObject {
        type Ptr = ThinPtr;

        unsafe fn access<'a>(context: &DatabaseContext, index: Self::Ptr) -> &'a Self {
            unsafe { context.access_pod(index.0) }
        }

        unsafe fn access_mut<'a>(
            context: &mut DatabaseContext,
            index: Self::Ptr,
        ) -> &'a mut Self {
            unsafe { context.access_pod_mut(index) }
        }
    }

    impl CounterObject {
        fn set_counter(&mut self, ctx: &mut DatabaseContext, value1: u32, value2: u32) {
            self.counter.set(ctx, value1);
            self.counter2.set(ctx, value2);
        }
        fn new<'a,'b:'a>(ctx: &'b mut DatabaseContext) -> &'a mut CounterObject {
            let counter: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
            assert_eq!(ctx.index_of(counter).0, 0);
            let counter2: &mut DatabaseCell<u32> = DatabaseCell::new(ctx);
            assert_eq!(ctx.index_of(counter2).0, 4);
            unsafe { Self::access_mut(ctx, ThinPtr(0)) }
        }
    }

    struct CounterApplication;

    impl Application for CounterApplication {
        type Root = CounterObject;

        fn initialize_root(ctx: &mut DatabaseContext) -> &mut CounterObject {
            let new_obj = CounterObject::new(ctx);
            //assert_eq!(ctx.index_of(&new_obj.counter), 0);
            new_obj
        }

        fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext, ptr: <CounterObject as Object>::Ptr) -> &'a mut Self::Root {
            unsafe { CounterObject::access_mut(ctx, ptr) }
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
        counter.counter2.set(context, 43);
        counter.counter.set(context, 44);

        assert_eq!(*counter.counter, 44);
        assert_eq!(counter.counter.get(), 44);
        assert_eq!(counter.counter2.get(), 43);
    }

    struct CounterMessage {
        time: u64,
        inc1: i32,
        inc2: i32,
    }
    impl Message<CounterObject> for CounterMessage {
        fn time(&self) -> u64 {
            self.time
        }

        fn apply(&self, context: &mut DatabaseContext, root: &mut CounterObject) {
            root.counter.set(context, root.counter.get().saturating_add_signed(self.inc1));
            root.counter2.set(context, root.counter2.get().saturating_add_signed(self.inc2));
        }
    }

    #[test]
    fn test_msg_store() {
        let mut db: Database<CounterApplication> = Database::create(CounterApplication);
        let mut messages = MessageStore::new();
        messages.messages.push(CounterMessage {
            time: 1,
            inc1: 2,
            inc2: 1,
        });

        messages.apply(&mut db);

        let (counter, context) = db.get_root();
        assert_eq!(counter.counter.get(), 2);


    }

    #[test]
    fn test_handle() {
        struct HandleApplication;

        impl Application for HandleApplication {
            type Root = DatabaseObjectHandle<u32>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut DatabaseObjectHandle<u32> {
                DatabaseObjectHandle::new(ctx, 43u32)
            }

            fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext, ptr: <DatabaseObjectHandle<u32> as Object>::Ptr) -> &'a mut Self::Root {
                unsafe { DatabaseObjectHandle::access_mut(ctx, ptr) }
            }
        }

        let mut db: Database<HandleApplication> = Database::create(HandleApplication);

        let app = HandleApplication;
        let (handle, context) = db.get_root();
        assert_eq!(*handle.get(context), 43);
    }

    #[test]
    fn test_vec0() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut DatabaseVec<CounterObject> {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                obj
            }

            fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext, ptr: <DatabaseVec<CounterObject> as Object>::Ptr) -> &'a mut Self::Root {
                unsafe { DatabaseVec::access_mut(ctx, ThinPtr(0)) }
            }
        }

        let mut db: Database<CounterVecApplication> = Database::create(CounterVecApplication);
        let (counter_vec, context) = db.get_root();
        assert_eq!(counter_vec.len(), 0);

        println!("1");
        let new_element = counter_vec.push_new(context);
        let new_element = counter_vec.get_mut(context, 0);
        println!("2");

        new_element.counter.set(context, 47);
        let new_element = counter_vec.push_new(context);
        new_element.counter.set(context, 48);
        println!("3");

        assert_eq!(counter_vec.len(), 2);

        let item = counter_vec.get_mut(context, 1);
        assert_eq!(*item.counter, 48);

        println!("4");

        for _ in 0..10 {
            let new_element = counter_vec.push_new(context);
            println!("5");

        }
        println!("6");

        let item = counter_vec.get_mut(context, 1);
        assert_eq!(*item.counter, 48);
    }

    #[test]
    fn test_vec_undo() {
        struct CounterVecApplication;

        impl Application for CounterVecApplication {
            type Root = DatabaseVec<CounterObject>;

            fn initialize_root(ctx: &mut DatabaseContext) -> &mut DatabaseVec<CounterObject> {
                let obj: &mut DatabaseVec<CounterObject> = DatabaseVec::new(ctx);
                obj
            }

            fn get_root<'a>(&'a mut self, ctx: &mut DatabaseContext, ptr: <DatabaseVec<CounterObject> as Object>::Ptr) -> &'a mut Self::Root {
                unsafe { DatabaseVec::access_mut(ctx, ThinPtr(0)) }
            }
        }

        let mut db: Database<CounterVecApplication> = Database::create(CounterVecApplication);

        {
            let (counter_vec, context) = db.get_root();
            context.set_time(1);
            assert_eq!(counter_vec.len(), 0);
            println!("Pre-push");
            let new_element = counter_vec.push_new(context);
            new_element.counter.set(context, 47);
            new_element.counter2.set(context, 48);
            println!("Pre-set-time");
            context.set_time(2);
            assert_eq!(counter_vec.len(), 1);
            context.set_time(3);
        }

        {
            let (counter_vec, context) = db.get_root();
            let counter = counter_vec.get_mut(context, 0);
            counter.counter.set(context, 50);
            context.rewind(2);
            assert_eq!(counter.counter.get(), 47);
        }

        println!("Rewind!");
        db.rewind(1);

        {
            let (counter_vec, context) = db.get_root();
            assert_eq!(counter_vec.len(), 0);
        }
    }
}
