//! Implementation of the warehouse benchmark for noatun
use crate::{ArticleId, Benchmark, BoxId, TasksInTransaction};
use noatun::data_types::{NoatunHashMap, NoatunOption, NoatunVec};
use noatun::database::DatabaseSettings;
use noatun::{
    noatun_object, Database, Message, MessageId, NoatunCell, NoatunTime, OpenMode,
    SavefileMessageSerializer,
};

use crate::model::Task;
use std::pin::Pin;

noatun_object!(
    /// Define the schema for a box in the warehouse
    struct StorageBox {
        /// The box's id
        pod id: u32,
        /// A mapping from ArticleId to count
        object articles: NoatunHashMap<u32, NoatunCell<u32>>,
        /// Parent BoxId, or None
        pod parent: NoatunOption<u32>,
        /// List of boxes that are (currently) inside this box.
        object children: NoatunVec<NoatunCell<u32>>,
    }
);


noatun_object!(
    /// Define the schema for the main document
    struct MainDoc {
        /// Boxes, keyed by id
        object boxes: NoatunHashMap<u32, StorageBox>
    }
);

impl Message for Task {
    type Root = MainDoc;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {

        let mut project = root.pin_project();
        
        match self {
            Task::CreateBox { id } => {
                
                project.boxes.insert(
                    id.0,
                    &StorageBoxNative {
                        id: id.0,
                        articles: Default::default(),
                        parent: None.into(),
                        children: vec![],
                    },
                );
            }
            Task::MoveBox { id, to } => {
                let mut prev_parent = None;
                if let Some(abox) = project.boxes.as_mut().get_mut_val(&id.0) {
                    let abox = abox.pin_project();
                    prev_parent = abox.parent.into_option();
                    abox.parent.set((to.map(|x| x.0)).into());
                }
                if let Some(parent) = to {
                    if let Some(parent_obj) = project.boxes.as_mut().get_mut_val(&parent.0) {
                        let mut parent_proj = parent_obj.pin_project();
                        parent_proj.children.as_mut().push(id.0);
                    }
                }

                if let Some(prev_parent) = prev_parent {
                    if let Some(parent_obj) = project.boxes.as_mut().get_mut_val(&prev_parent) {
                        let parent_proj = parent_obj.pin_project();
                        let index = parent_proj.children.iter().position(|x| x.get() == id.0);
                        if let Some(index) = index {
                            parent_proj.children.swap_remove(index);
                        }
                    }
                }
            }
            t @ Task::AddArticle {
                id,
                article_id,
                quantity,
            }
            | t @ Task::RemoveArticle {
                id,
                article_id,
                quantity,
            } => {
                if let Some(abox) = project.boxes.get_mut_val(&id.0) {
                    let mut abox = abox.pin_project();
                    let subtract = matches!(t, Task::RemoveArticle { .. });
                    if let Some(art) = abox.articles.as_mut().get_mut_val(article_id) {
                        let cur = art.get();
                        let new = if subtract {
                            cur.saturating_sub(*quantity)
                        } else {
                            cur + *quantity
                        };
                        art.set(new);
                    } else {
                        if !subtract {
                            abox.articles.insert(*article_id, quantity);
                        }
                    }
                }
            }
        }
    }
}

/// Implementatino of the warehouse benchmark for noatun
pub struct NoatunImpl {
    /// The noatun db instance.
    ///
    /// This contains all data in the db, including the materialized view and
    /// the message store.
    pub db: Database<Task>,
    event_count: usize,
    transaction_count: usize,
}

impl Benchmark for NoatunImpl {
    fn run(tasks: Vec<TasksInTransaction>) -> impl Benchmark {
        run_test(tasks.into_iter(), false)
    }

    fn single_query(&self, box_id: BoxId, article_id: ArticleId) -> u32 {
        let sess = self.db.begin_session().unwrap();

        sess.with_root(|root| {
            let mut temp = Vec::with_capacity(20);
            temp.push(box_id.0);
            let mut temp2 = Vec::with_capacity(20);
            let mut sum = 0;
            while !temp.is_empty() {
                for x in temp.drain(..) {
                    if let Some(abox) = root.boxes.get(&x) {
                        sum += abox
                            .articles
                            .get(&article_id.0)
                            .map(|x| x.get())
                            .unwrap_or(0);

                        for child in abox.children.iter() {
                            temp2.push(child.get());
                        }
                    }
                }
                std::mem::swap(&mut temp, &mut temp2);
            }
            sum
        })
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

pub(crate) fn run_test(input: impl Iterator<Item = TasksInTransaction>, in_memory: bool) -> NoatunImpl {
    let noatun_time = NoatunTime::now();

    let mut db;
    if in_memory {
        db = Database::<Task>::create_in_memory(
            50_000,
            DatabaseSettings {
                auto_prune: false,
                ..DatabaseSettings::default()
            },
        )
        .unwrap();
    } else {
        db = Database::<Task>::create_new(
            "test_noatun.bin",
            OpenMode::Overwrite,
            DatabaseSettings {
                auto_prune: true,

                max_file_size: 1000_000_000,
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

    drop(sess);

    NoatunImpl {
        db,
        event_count: tot_events,
        transaction_count,
    }
}
