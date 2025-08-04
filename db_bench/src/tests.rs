use crate::model::Task;
use crate::{noatun_bench, ArticleId, Benchmark, BoxId, TasksInTransaction};

#[test]
fn simple_smoketest() {
    let tasks = TasksInTransaction(vec![Task::CreateBox { id: BoxId(0) }]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true);
    assert_eq!(db.single_query(BoxId(0), ArticleId(0)), 0);
}
#[test]
fn simple_smoketest2() {
    let tasks = TasksInTransaction(vec![
        Task::CreateBox { id: BoxId(0) },
        Task::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
    ]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true);
    assert_eq!(db.single_query(BoxId(0), ArticleId(3)), 42);
}
#[test]
fn simple_smoketest3() {
    let tasks = TasksInTransaction(vec![
        Task::CreateBox { id: BoxId(0) },
        Task::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
    ]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true);
    assert_eq!(db.single_query(BoxId(0), ArticleId(3)), 42);
}

#[test]
fn simple_smoketest4() {
    let tasks = TasksInTransaction(vec![
        Task::CreateBox { id: BoxId(0) },
        Task::CreateBox { id: BoxId(1) },
        Task::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
        Task::AddArticle {
            id: BoxId(1),
            article_id: 3,
            quantity: 43,
        },
    ]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true);
    assert_eq!(db.single_query(BoxId(0), ArticleId(3)), 42);
}

#[test]
fn simple_smoketest5() {
    // Place box 1 in box 0.
    let tasks = TasksInTransaction(vec![
        Task::CreateBox { id: BoxId(0) },
        Task::CreateBox { id: BoxId(1) },
        Task::MoveBox {
            id: BoxId(1),
            to: Some(BoxId(0)),
        },
        Task::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
        Task::AddArticle {
            id: BoxId(1),
            article_id: 3,
            quantity: 43,
        },
    ]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true);
    // Sum of quantities in box 0 should now be 42 + 43, since box 1 is inside box 0.
    assert_eq!(db.single_query(BoxId(0), ArticleId(3)), 85);
}
