//! Implementation of the warehouse benchmark for sqlite
//! use crate::model::Task;
use crate::{ Benchmark, TasksInTransaction};
use rusqlite::Connection;
use std::path::Path;
use crate::warehouse::model::{ArticleId, BoxId, Query, WarehouseTask};

/// Implementation of the warehouse benchmark for sqlite
pub struct SqliteImpl {
    conn: Connection,
    event_count: usize,
    transaction_count: usize,
}

fn run(transactions: impl IntoIterator<Item = TasksInTransaction<WarehouseTask>>) -> SqliteImpl {
    if std::path::Path::exists(Path::new("test.bin")) {
        std::fs::remove_file("test.bin").unwrap();
    }
    let mut conn = Connection::open("test.bin").unwrap();
    let mut event_count = 0;
    let mut transaction_count = 0;

    conn.execute(
        "
         CREATE TABLE boxes (
             id INTEGER PRIMARY KEY,
             parent INTEGER NULL
         );",
        (), // empty list of parameters.
    )
    .unwrap();
    conn.execute(
        "
         CREATE TABLE quantity (
             box_id INTEGER,
             article_id INTEGER,
             quantity INTEGER,
             PRIMARY KEY(box_id, article_id)
         );
",
        (),
    )
    .unwrap();

    conn.execute(
        "
        CREATE INDEX parent_index
            ON boxes(parent);
    ",
        (),
    )
    .unwrap();

    for tasks in transactions {
        let sqlite_trans = conn.transaction().unwrap();
        for task in tasks.0 {
            event_count += 1;
            match task {
                WarehouseTask::CreateBox { id } => {
                    sqlite_trans
                        .execute("INSERT INTO boxes VALUES(?, NULL)", (id.0,))
                        .unwrap();
                }
                WarehouseTask::MoveBox { id, to } => {
                    sqlite_trans
                        .execute(
                            "UPDATE boxes SET parent=? WHERE id=?",
                            (to.map(|x| x.0), id.0),
                        )
                        .unwrap();
                }
                WarehouseTask::AddArticle {
                    id,
                    article_id,
                    quantity,
                } => {
                    sqlite_trans.execute("INSERT INTO quantity(box_id,article_id,quantity) VALUES (?,?,?) ON CONFLICT(box_id,article_id) DO UPDATE SET quantity = quantity + ?", (id.0,article_id,quantity,quantity)).unwrap();
                }
                WarehouseTask::RemoveArticle {
                    id,
                    article_id,
                    quantity,
                } => {
                    sqlite_trans.execute("UPDATE quantity SET quantity = quantity - ? WHERE box_id=? AND article_id = ?", (quantity, id.0, article_id)).unwrap();
                }
            }
        }
        transaction_count += 1;
        sqlite_trans.commit().unwrap();
    }

    SqliteImpl {
        conn,
        event_count,
        transaction_count,
    }
}

impl Benchmark for SqliteImpl {
    type Task = WarehouseTask;
    fn run(tasks: Vec<TasksInTransaction<Self::Task>>) -> Self {
        run(tasks)
    }

    fn single_query(&self, query: Query) -> u64 {
        single_query(&self.conn, query.box_id, query.article_id)
    }

    fn space_used(&self) -> f64 {
        std::fs::metadata("test.bin").unwrap().len() as f64
    }

    fn event_count(&self) -> usize {
        self.event_count
    }

    fn transaction_count(&self) -> usize {
        self.transaction_count
    }

    fn name() -> &'static str {
        "Sqlite"
    }
}

fn single_query(conn: &Connection, boxid: BoxId, article_id: ArticleId) -> u64 {
    let mut stmt = conn.prepare("
WITH RECURSIVE boxclosure(x) AS (
SELECT ?
UNION
SELECT boxes.id FROM boxclosure,boxes WHERE boxes.parent=boxclosure.x
)
select IFNULL(SUM(quantity.quantity),0) from boxclosure bt,quantity WHERE quantity.box_id=x AND quantity.article_id=?;

    ").unwrap();
    let mut result = stmt.query((boxid.0, article_id.0)).unwrap();
    let scalar = result.next().unwrap().unwrap();
    scalar.get(0).unwrap()
}
