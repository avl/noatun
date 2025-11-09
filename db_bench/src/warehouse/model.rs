//! A simplified model of a warehouse, with boxes stored in boxes
use indexmap::IndexMap;
use rand::prelude::{IteratorRandom, SmallRng};
use rand::Rng;
use crate::{BenchmarkTask, TasksInTransaction};

/// Identifier for a box (or container, pallet, shelf whatever)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Savefile)]
pub struct BoxId(pub u32);

/// Identifier for an article (you know, bolts, pipes, washers or whatever)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Savefile)]
pub struct ArticleId(pub u32);

/// An operation performed in the warehouse
#[derive(Debug, Savefile)]
pub enum WarehouseTask {
    /// A new box is created
    CreateBox {
        /// The id of the box.
        ///
        /// This just has to be unique, no other requirements
        id: BoxId,
    },
    /// A box is moved to be inside another box, or if `to` is empty, is moved
    /// out of any box.
    MoveBox {
        /// The id of the box.
        id: BoxId,
        /// The box that this box has been moved into.
        ///
        /// If this is None, it means the box has been moved out to the top level.
        to: Option<BoxId>,
    },
    /// A quantity of the given article is added to the box
    AddArticle {
        /// The box to which the quantity is added
        id: BoxId,
        /// The article that is added
        article_id: u32,
        /// The number of pieces
        quantity: u32,
    },
    /// A quantity of the given article is removed from the box
    RemoveArticle {
        /// The box from which the quantity is removed
        id: BoxId,
        /// The article id of the removed items
        article_id: u32,
        /// The number of pieces removed
        quantity: u32,
    },
}

pub struct Query {
    pub box_id: BoxId,
    pub article_id: ArticleId,
}
impl BenchmarkTask for WarehouseTask {
    type Query = Query;

    fn generate_tasks(rng: SmallRng) -> impl Iterator<Item=Self> {
        generate_sequence(rng)
    }

    fn generate_queries(rng: SmallRng) -> impl Iterator<Item=Self::Query> {
        let mut temp: Vec<Query> = Vec::new();
        for boxid in 0..4 {
            for article_id in 0..4 {
                temp.push(Query {
                    box_id: BoxId(boxid),
                    article_id: ArticleId(article_id),
                });
            }
        }
        temp.into_iter()
    }

    fn name() -> &'static str {
        "warehouse"
    }
}


/// Generate random test-sequence of events for the warehouse
pub fn generate_sequence(mut rng: SmallRng) -> impl Iterator<Item = WarehouseTask> {
    const MAX_BOXES: usize = 10_000;

    let mut boxes: IndexMap<BoxId, /*parent*/ Option<BoxId>> = IndexMap::new();
    let qtys: IndexMap<(BoxId, ArticleId), u32> = IndexMap::new();

    fn any_parent_contains(
        boxes: &mut IndexMap<BoxId, Option<BoxId>>,
        thebox: BoxId,
        sought_parent: BoxId,
    ) -> bool {
        if thebox == sought_parent {
            return true;
        }
        if let Some(Some(parent)) = boxes.get(&thebox) {
            return any_parent_contains(boxes, *parent, sought_parent);
        }
        false
    }
    let mut next_box_id = 0;
    std::iter::from_fn(move || {

        /*let count = rng.gen_range(1..1000);
        for _cnt in 0..count {*/
        if boxes.is_empty() {
            boxes.insert(BoxId(0), None);
            next_box_id = 1;
            return Some(WarehouseTask::CreateBox { id: BoxId(0) });
        }
        match rng.gen_range(0..2) {
            0 if boxes.len() < MAX_BOXES => {
                let id = next_box_id;
                next_box_id += 1;
                boxes.insert(BoxId(id), None);
                return Some(WarehouseTask::CreateBox { id: BoxId(id) });
            }
            1 if boxes.len() > 2 => {
                let box1 = *boxes.keys().choose(&mut rng).unwrap();
                let box2 = *boxes.keys().choose(&mut rng).unwrap();
                if box1 != box2 && !any_parent_contains(&mut boxes, box2, box1) {
                    boxes.insert(box1, Some(box2));
                    return Some(WarehouseTask::MoveBox {
                        id: box1,
                        to: Some(box2),
                    });
                }
            }
            _ => {}
        }
        let box_id = *boxes.keys().choose(&mut rng).unwrap();
        let article = rng.gen_range(0..10);
        let qty = rng.gen_range(0..10);
        match rng.gen_range(0..2) {
            0 if *qtys.get(&(box_id, ArticleId(article))).unwrap_or(&0) >= qty => {
                return Some(WarehouseTask::RemoveArticle {
                    id: box_id,
                    article_id: article,
                    quantity: qty,
                });
            }
            _ => return Some(WarehouseTask::AddArticle {
                id: box_id,
                article_id: article,
                quantity: qty,
            }),
        }
        //}
        //Some(ret)
    })
}
