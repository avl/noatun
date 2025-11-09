//! Implementation of the warehouse benchmark for noatun
use crate::{Benchmark, TasksInTransaction};
use noatun::data_types::{NoatunHashMap, NoatunOption, NoatunVec};
use noatun::database::DatabaseSettings;
use noatun::{
    noatun_object, Database, Message, MessageId, NoatunCell, NoatunTime, OpenMode,
    SavefileMessageSerializer,
};


use std::pin::Pin;
use crate::noatun_helper::{run_test, NoatunImpl};
use crate::warehouse::model::{ArticleId, BoxId, Query, WarehouseTask};

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

impl Message for WarehouseTask {
    type Root = MainDoc;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {

        let mut project = root.pin_project();
        
        match self {
            WarehouseTask::CreateBox { id } => {
                
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
            WarehouseTask::MoveBox { id, to } => {
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
            t @ WarehouseTask::AddArticle {
                id,
                article_id,
                quantity,
            }
            | t @ WarehouseTask::RemoveArticle {
                id,
                article_id,
                quantity,
            } => {
                if let Some(abox) = project.boxes.get_mut_val(&id.0) {
                    let mut abox = abox.pin_project();
                    let subtract = matches!(t, WarehouseTask::RemoveArticle { .. });
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




impl Benchmark for NoatunImpl<WarehouseTask> {
    type Task = WarehouseTask;

    fn run(tasks: Vec<TasksInTransaction<Self::Task>>) -> Self {
        run_test(tasks, false)
    }

    fn single_query(&self, query: Query) -> u64 {
        let sess = self.db.begin_session().unwrap();

        sess.with_root(|root| {
            let mut temp = Vec::with_capacity(20);
            temp.push(query.box_id.0);
            let mut temp2 = Vec::with_capacity(20);
            let mut sum = 0;
            while !temp.is_empty() {
                for x in temp.drain(..) {
                    if let Some(abox) = root.boxes.get(&x) {
                        sum += abox
                            .articles
                            .get(&query.article_id.0)
                            .map(|x| x.get())
                            .unwrap_or(0) as u64;

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