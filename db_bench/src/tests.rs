use crate::noatun_bench::single_query;
use crate::{noatun_bench, ArticleId, BoxId, Task, TasksInTransaction};

#[test]
fn simple_smoketest() {
    let mut tasks = TasksInTransaction(vec![Task::CreateBox { id: BoxId(0) }]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true).0;
    assert_eq!(single_query(&db, BoxId(0), ArticleId(0)), 0);
}
#[test]
fn simple_smoketest2() {
    let mut tasks = TasksInTransaction(vec![
        Task::CreateBox { id: BoxId(0) },
        Task::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
    ]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true).0;
    assert_eq!(single_query(&db, BoxId(0), ArticleId(3)), 42);
}
#[test]
fn simple_smoketest3() {
    let mut tasks = TasksInTransaction(vec![
        Task::CreateBox { id: BoxId(0) },
        Task::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
    ]);
    let db = noatun_bench::run_test(std::iter::once(tasks), true).0;
    assert_eq!(single_query(&db, BoxId(0), ArticleId(3)), 42);
}

#[test]
fn simple_smoketest4() {
    let mut tasks = TasksInTransaction(vec![
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
    let db = noatun_bench::run_test(std::iter::once(tasks), true).0;
    assert_eq!(single_query(&db, BoxId(0), ArticleId(3)), 42);
}

#[test]
fn simple_smoketest5() {
    // Place box 1 in box 0.
    let mut tasks = TasksInTransaction(vec![
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
    let db = noatun_bench::run_test(std::iter::once(tasks), true).0;
    // Sum of quantities in box 0 should now be 42 + 43, since box 1 is inside box 0.
    assert_eq!(single_query(&db, BoxId(0), ArticleId(3)), 85);
}
