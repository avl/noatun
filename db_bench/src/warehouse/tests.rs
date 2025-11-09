use crate::{Benchmark, TasksInTransaction};
use crate::noatun_helper::run_test;
use crate::warehouse::model::{ArticleId, BoxId, WarehouseTask, Query};
use crate::warehouse::noatun_bench;

#[test]
fn simple_smoketest() {
    let tasks = TasksInTransaction(vec![WarehouseTask::CreateBox { id: BoxId(0) }]);
    let db = run_test(vec!(tasks), true);
    assert_eq!(db.single_query(Query {
        box_id: BoxId(0),
        article_id: ArticleId(0),
    }), 0);
}
#[test]
fn simple_smoketest2() {
    let tasks = TasksInTransaction(vec![
        WarehouseTask::CreateBox { id: BoxId(0) },
        WarehouseTask::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
    ]);
    let db = run_test(vec!(tasks), true);
    assert_eq!(db.single_query(
        Query {
            box_id: BoxId(0),
            article_id: ArticleId(3)
        }
    ), 42);
}
#[test]
fn simple_smoketest3() {
    let tasks = TasksInTransaction(vec![
        WarehouseTask::CreateBox { id: BoxId(0) },
        WarehouseTask::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
    ]);
    let db = run_test(vec!(tasks), true);
    assert_eq!(db.single_query(
        Query {
            box_id: BoxId(0),
            article_id: ArticleId(3)
        }
    ), 42);
}

#[test]
fn simple_smoketest4() {
    let tasks = TasksInTransaction(vec![
        WarehouseTask::CreateBox { id: BoxId(0) },
        WarehouseTask::CreateBox { id: BoxId(1) },
        WarehouseTask::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
        WarehouseTask::AddArticle {
            id: BoxId(1),
            article_id: 3,
            quantity: 43,
        },
    ]);
    let db = run_test(vec!(tasks), true);
    assert_eq!(db.single_query(
        Query {
            box_id: BoxId(0),
            article_id: ArticleId(3)
        }
    ), 42);
}

#[test]
fn simple_smoketest5() {
    // Place box 1 in box 0.
    let tasks = TasksInTransaction(vec![
        WarehouseTask::CreateBox { id: BoxId(0) },
        WarehouseTask::CreateBox { id: BoxId(1) },
        WarehouseTask::MoveBox {
            id: BoxId(1),
            to: Some(BoxId(0)),
        },
        WarehouseTask::AddArticle {
            id: BoxId(0),
            article_id: 3,
            quantity: 42,
        },
        WarehouseTask::AddArticle {
            id: BoxId(1),
            article_id: 3,
            quantity: 43,
        },
    ]);
    let db = run_test(vec!(tasks), true);
    // Sum of quantities in box 0 should now be 42 + 43, since box 1 is inside box 0.
    assert_eq!(db.single_query(
        Query {
            box_id: BoxId(0),
            article_id: ArticleId(3)
        }
    ), 85);
}
