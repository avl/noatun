use std::path::Path;
use rusqlite::Connection;
use crate::{Benchmark, TasksInTransaction};
use crate::key_value_store::key_value_model::{KeyValOperation, KeyValQuery};
use crate::warehouse::model::{ArticleId, BoxId, Query, WarehouseTask};

/// Implementation of the warehouse benchmark for sqlite
pub struct SqliteKeyValueImpl {
    conn: Connection,
    event_count: usize,
    transaction_count: usize,
}
fn run(transactions: impl IntoIterator<Item = TasksInTransaction<KeyValOperation>>) -> SqliteKeyValueImpl {
    if std::path::Path::exists(Path::new("test.bin")) {
        std::fs::remove_file("test.bin").unwrap();
    }
    let mut conn = Connection::open("test.bin").unwrap();
    let mut event_count = 0;
    let mut transaction_count = 0;

    use super::key_value_model::VALUE_SIZE;
    conn.execute(
     &format!("CREATE TABLE mapping (
         key varchar(25) PRIMARY KEY,
         val varchar({VALUE_SIZE})
     );"),
        (), // empty list of parameters.
    )
        .unwrap();

    for tasks in transactions {
        let sqlite_trans = conn.transaction().unwrap();
        for task in tasks.0 {
            event_count += 1;
            match task {
                KeyValOperation::AddKeyVal(key,val) => {
                    sqlite_trans
                        .execute("INSERT INTO mapping VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET val = ?", (key, val.clone(), val))
                        .unwrap();
                }
                KeyValOperation::RemoveKey(key) => {
                    sqlite_trans
                        .execute("DELETE FROM mapping WHERE key = ?", (key, ))
                        .unwrap();
                }
            }
        }
        transaction_count += 1;
        sqlite_trans.commit().unwrap();
    }

    SqliteKeyValueImpl {
        conn,
        event_count,
        transaction_count,
    }
}

impl Benchmark for SqliteKeyValueImpl {
    type Task = KeyValOperation;
    fn run(tasks: Vec<TasksInTransaction<Self::Task>>) -> Self {
        run(tasks)
    }

    fn single_query(&self, query: KeyValQuery) -> u64 {
        single_query(&self.conn, query.key)
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

fn single_query(conn: &Connection, key: String) -> u64 {
    let mut stmt = conn.prepare("
        select val from mapping where key = ?
        ").unwrap();

    let mut result = stmt.query((&key,)).unwrap();
    if let Ok(Some(scalar)) = result.next() {
        if let Some(val) = scalar.get(0).unwrap() {
            let val: String = val;
            key.parse().unwrap()
        } else {
            0
        }
    } else {
        0
    }
}

