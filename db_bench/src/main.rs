//! Simple benchmark application, comparing sqlite with noatun for a simple warehouse problem
#![deny(missing_docs)]
use crate::model::{generate_sequence, ArticleId, BoxId, TasksInTransaction};
use crate::noatun_bench::NoatunImpl;
use crate::sqlite_bench::SqliteImpl;
use rand::prelude::SmallRng;
use rand::SeedableRng;
use std::time::Instant;
use tracing_subscriber::Layer;

#[macro_use]
extern crate noatun;

pub mod model;
pub mod noatun_bench;
pub mod sqlite_bench;
#[cfg(test)]
mod tests;


/// Trait implemented by all benchmark backends.
///
/// This program implements this for sqlite and noatun
pub trait Benchmark {
    /// Apply all the given transactions to the db
    fn run(tasks: Vec<TasksInTransaction>) -> impl Benchmark;
    /// Run a query on the db.
    ///
    /// This must return the total number of pieces of 'article_id' in box `box_id`,
    /// or any of its inner boxes (recursively).
    fn single_query(&self, box_id: BoxId, article_id: ArticleId) -> u32;

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

fn run_test<T: Benchmark>(seed: u64, count: usize) -> u32 {
    let gen_task = || generate_sequence(SmallRng::seed_from_u64(seed)).take(count);
    let tasks = gen_task().collect::<Vec<_>>();

    let bef = Instant::now();
    let db = T::run(tasks);
    let t = bef.elapsed();
    let mut tot_sum = 0;
    println!(
        "{}: {} inserts in {:?} (in {} transactions, {} ops/s)",
        T::name(),
        db.event_count(),
        t,
        db.transaction_count(),
        db.event_count() as f64 / t.as_secs_f64()
    );
    let bef = Instant::now();
    let mut query_count = 0;
    for boxid in 0..4 {
        for article_id in 0..4 {
            let sum = db.single_query(BoxId(boxid), ArticleId(article_id));
            query_count += 1;
            tot_sum += sum;
        }
    }
    let t = bef.elapsed();
    println!(
        "{}: {} queries in {:?} ({} per second)",
        T::name(),
        query_count,
        t,
        query_count as f32 / t.as_secs_f32()
    );
    tot_sum
}

fn main() {
    setup_tracing();

    let transaction_count: usize = 500;

    let noatun_sum = run_test::<NoatunImpl>(1, transaction_count);
    let sqlite_sum = run_test::<SqliteImpl>(1, transaction_count);

    assert_eq!(noatun_sum, sqlite_sum);
}
