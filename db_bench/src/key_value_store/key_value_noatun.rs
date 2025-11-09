use std::pin::Pin;
use rand::prelude::SmallRng;
use noatun::{Database, Message, MessageId, SavefileMessageSerializer};
use noatun::prelude::{NoatunHashMap, NoatunString};
use crate::{Benchmark, BenchmarkTask, TasksInTransaction};
use crate::key_value_store::key_value_model::{KeyValQuery, KeyValOperation};
use crate::noatun_helper::{run_test, NoatunImpl};
use crate::warehouse::model::WarehouseTask;

noatun_object!(
    struct MainDoc {
        object mapping: NoatunHashMap<NoatunString, NoatunString>
    }
);

impl Message for KeyValOperation {
    type Root = MainDoc;
    type Serializer = SavefileMessageSerializer<KeyValOperation>;

    fn apply(&self, id: MessageId, root: Pin<&mut Self::Root>) {
        match self {
            KeyValOperation::AddKeyVal(k, v) => {
                let this = root.pin_project();
                this.mapping.insert(k.as_str(), v);
            }
            KeyValOperation::RemoveKey(k) => {
                let this = root.pin_project();
                this.mapping.remove(k);
            }
        }
    }
}

impl Benchmark for NoatunImpl<KeyValOperation> {
    type Task = KeyValOperation;

    fn run(tasks: Vec<TasksInTransaction<Self::Task>>) -> Self {
        run_test(tasks, false)
    }

    fn single_query(&self, query: KeyValQuery) -> u64 {
        self.db.begin_session().unwrap().with_root(|root| {
            let val = root.mapping.get(&query.key);

            if val.is_some() {
                query.key.parse::<u64>().unwrap()
            } else {
                0
            }
        })
    }

    fn space_used(&self) -> f64 {
        self.db.disk_space_used_bytes() as f64
    }

    fn event_count(&self) -> usize {

        self.event_count
    }

    fn transaction_count(&self) -> usize {
        self.transaction_count

    }

    fn name() -> &'static str {
        "Noatun"
    }
}


