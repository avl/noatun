use noatun::{Database, DatabaseSettings, Message, NoatunTime, OpenMode};
use crate::TasksInTransaction;

/// Implementation of the warehouse benchmark for noatun
pub struct NoatunImpl<MSG:Message> {
    /// The noatun db instance.
    ///
    /// This contains all data in the db, including the materialized view and
    /// the message store.
    pub db: Database<MSG>,
    pub event_count: usize,
    pub transaction_count: usize,
}

pub(crate) fn run_test<MSG:Message>(input: Vec<TasksInTransaction<MSG>>, in_memory: bool) -> NoatunImpl<MSG> {
    let noatun_time = NoatunTime::now();

    let mut db;
    if in_memory {
        db = Database::<MSG>::create_in_memory(
            50_000,
            DatabaseSettings {
                auto_prune: false,
                ..DatabaseSettings::default()
            },
        )
            .unwrap();
    } else {
        db = Database::<MSG>::create_new(
            "test_noatun.bin",
            OpenMode::Overwrite,
            DatabaseSettings {
                auto_prune: true,

                max_file_size: 100_000_000_000,
                ..DatabaseSettings::default()
            },
        )
            .unwrap();
    }

    db.set_abort_on_panic();

    let mut transaction_count = 0;
    let mut tot_events = 0;
    let mut sess = db.begin_session_mut().unwrap();
    //sess.disable_filesystem_sync().unwrap();
    for trans in input {
        tot_events += trans.0.len();
        sess.append_many_local_at(noatun_time, trans.0.into_iter())
            .unwrap();
        transaction_count += 1;
    }

    sess.commit().unwrap();

    NoatunImpl {
        db,
        event_count: tot_events,
        transaction_count,
    }
}

