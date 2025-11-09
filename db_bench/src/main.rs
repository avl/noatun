//! Simple benchmark application, comparing sqlite with noatun for a simple warehouse problem
//#![deny(missing_docs)] //TODO: enable warning again
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;
use tracing_subscriber::Layer;
use crate::key_value_store::key_value_model::KeyValOperation;
use crate::noatun_helper::NoatunImpl;
use crate::warehouse::model::{ArticleId, BoxId, WarehouseTask};

use crate::warehouse::sqlite_bench::SqliteImpl;

#[macro_use]
extern crate noatun;

pub mod noatun_helper;

mod warehouse;
mod key_value_store;

trait BenchmarkTask : Sized {
    type Query;
    fn generate_tasks(rng: SmallRng) -> impl Iterator<Item = Self>;

    fn generate_queries(rng: SmallRng) -> impl Iterator<Item=Self::Query>;

    fn name() -> &'static str;

    fn generate_tasks_in_transaction(rng: SmallRng) -> impl Iterator<Item = TasksInTransaction<Self>> {
        let mut rng2 = rng.clone();
        let mut gen = Self::generate_tasks(rng);
        std::iter::from_fn(move || {
            let count = rng2.gen_range(1..1000);
            let mut temp = Vec::with_capacity(count);
            for _cnt in 0..count {
                temp.push(gen.next().unwrap());
            }
            Some(TasksInTransaction(temp))
        })
    }
}


/// A set of tasks performed in one database transaction
pub struct TasksInTransaction<Task>(pub Vec<Task>);

/// Trait implemented by all benchmark backends.
///
/// This program implements this for sqlite and noatun
pub trait Benchmark {

    /// The type of each task that the benchmark should carry out
    type Task : BenchmarkTask;

    /// Apply all the given transactions to the db
    fn run(tasks: Vec<TasksInTransaction<Self::Task>>) -> Self;
    /// Run a query on the db.
    ///
    /// Returns a hash that depends on the query output
    fn single_query(&self, query: <Self::Task as BenchmarkTask>::Query) -> u64;

    /// Number of bytes of disk space used
    fn space_used(&self) -> f64;

    /// Return the number of events added
    fn event_count(&self) -> usize;
    /// Return the number of transactions that we performed
    fn transaction_count(&self) -> usize;
    /// The name of the impl
    fn name() -> &'static str;
}

/// Configure tracing
fn setup_tracing() {
    let stdout_log = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .pretty()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(stdout_log);
    _ = tracing::subscriber::set_global_default(subscriber);
}

fn run_test<BT: BenchmarkTask, T>(seed: u64, count: usize) -> u64 where T: Benchmark<Task=BT> {

    let tasks = T::Task::generate_tasks_in_transaction(SmallRng::seed_from_u64(seed)).take(count).collect::<Vec<_>>();

    let bef = Instant::now();
    let db = T::run(tasks);
    let t = bef.elapsed();
    let mut tot_sum = 0;
    println!(
        "{}/{}: {} inserts in {:?} (in {} transactions, {} ops/s)",
        T::Task::name(),
        T::name(),
        db.event_count(),
        t,
        db.transaction_count(),
        db.event_count() as f64 / t.as_secs_f64()
    );
    let bef = Instant::now();
    let mut query_count = 0;
    for query in BT::generate_queries(SmallRng::seed_from_u64(seed)) {
        let hash = db.single_query(query);
        query_count += 1;
        tot_sum ^= hash;

    }
    
    let t = bef.elapsed();
    println!(
        "{}/{}: {} queries in {:?} ({} per second)",
        T::Task::name(),
        T::name(),
        query_count,
        t,
        query_count as f32 / t.as_secs_f32()
    );
    println!("{}: Space used: {} MB", T::name(), db.space_used()/1e6);
    tot_sum
}

fn main() {
    setup_tracing();
    let args = std::env::args().skip(1).collect::<Vec<String>>();
    let args = args.iter().map(|a| a.as_str()).collect::<Vec<_>>();
    let all = args.is_empty();

    let transaction_count: usize = 10;
    if all || args.contains(&"warehouse") {
        let noatun_sum = run_test::<WarehouseTask, NoatunImpl<WarehouseTask>>(1, transaction_count);
        let sqlite_sum = run_test::<WarehouseTask, SqliteImpl>(1, transaction_count);
        assert_eq!(noatun_sum, sqlite_sum);
    }
    if all || args.contains(&"keyval") {
        let sqlite_sum = run_test::<KeyValOperation, key_value_store::key_value_sqlite::SqliteKeyValueImpl>(1, transaction_count);
        let noatun_sum = run_test::<KeyValOperation, NoatunImpl<KeyValOperation>>(1, transaction_count);
        assert_eq!(noatun_sum, sqlite_sum);
    }

}
