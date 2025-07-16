use crate::noatun_bench::single_query;
use indexmap::IndexMap;
use noatun::set_test_epoch;
use rand::prelude::{IteratorRandom, SmallRng};
use rand::{Rng, RngCore, SeedableRng};
use std::hint::black_box;
use std::time::Instant;
use tracing_subscriber::Layer;

#[macro_use]
extern crate noatun;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Savefile)]
struct BoxId(u32);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Savefile)]
struct ArticleId(u32);

#[derive(Debug, Savefile)]
pub enum Task {
    CreateBox {
        id: BoxId,
    },
    MoveBox {
        id: BoxId,
        to: Option<BoxId>,
    },
    AddArticle {
        id: BoxId,
        article_id: u32,
        quantity: u32,
    },
    RemoveArticle {
        id: BoxId,
        article_id: u32,
        quantity: u32,
    },
}

struct TasksInTransaction(Vec<Task>);

fn generate_sequence(mut rng: SmallRng) -> impl Iterator<Item = TasksInTransaction> {
    const MAX_BOXES: usize = 100;

    let mut boxes: IndexMap<BoxId, /*parent*/ Option<BoxId>> = IndexMap::new();
    let mut qtys: IndexMap<(BoxId, ArticleId), u32> = IndexMap::new();

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
        let mut ret = Vec::new();
        let count = rng.gen_range(1..1000);
        for _cnt in 0..count {
            if boxes.is_empty() {
                boxes.insert(BoxId(0), None);
                next_box_id = 1;
                ret.push(Task::CreateBox { id: BoxId(0) });
                continue;
            }
            match rng.gen_range(0..2) {
                0 if boxes.len() < MAX_BOXES => {
                    let id = next_box_id;
                    next_box_id += 1;
                    boxes.insert(BoxId(id), None);
                    ret.push(Task::CreateBox { id: BoxId(id) });
                    continue;
                }
                1 if boxes.len() > 2 => {
                    let box1 = *boxes.keys().choose(&mut rng).unwrap();
                    let box2 = *boxes.keys().choose(&mut rng).unwrap();
                    if box1 != box2 && !any_parent_contains(&mut boxes, box2, box1) {
                        boxes.insert(box1, Some(box2));
                        ret.push(Task::MoveBox {
                            id: box1,
                            to: Some(box2),
                        });
                        continue;
                    }
                }
                _ => {}
            }
            let box_id = *boxes.keys().choose(&mut rng).unwrap();
            let article = rng.gen_range(0..10);
            let qty = rng.gen_range(0..10);
            match rng.gen_range(0..2) {
                0 if *qtys.get(&(box_id, ArticleId(article))).unwrap_or(&0) >= qty => {
                    ret.push(Task::RemoveArticle {
                        id: box_id,
                        article_id: article,
                        quantity: qty,
                    })
                }
                _ => ret.push(Task::AddArticle {
                    id: box_id,
                    article_id: article,
                    quantity: qty,
                }),
            }
        }
        Some(TasksInTransaction(ret))
    })
}

pub mod sqlite_bench {
    use crate::{ArticleId, BoxId, Task, TasksInTransaction};
    use rusqlite::{params, Connection};
    use std::path::Path;

    pub fn run(transactions: impl IntoIterator<Item = TasksInTransaction>) -> (Connection, usize) {
        if std::path::Path::exists(Path::new("test.bin")) {
            std::fs::remove_file("test.bin").unwrap();
        }
        let mut conn = Connection::open("test.bin").unwrap();

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

        let mut ops = 0;
        for tasks in transactions {
            let mut sqlite_trans = conn.transaction().unwrap();
            for task in tasks.0 {
                ops += 1;
                match task {
                    Task::CreateBox { id } => {
                        sqlite_trans
                            .execute("INSERT INTO boxes VALUES(?, NULL)", (id.0,))
                            .unwrap();
                    }
                    Task::MoveBox { id, to } => {
                        sqlite_trans
                            .execute(
                                "UPDATE boxes SET parent=? WHERE id=?",
                                (to.map(|x| x.0), id.0),
                            )
                            .unwrap();
                    }
                    Task::AddArticle {
                        id,
                        article_id,
                        quantity,
                    } => {
                        sqlite_trans.execute("INSERT INTO quantity(box_id,article_id,quantity) VALUES (?,?,?) ON CONFLICT(box_id,article_id) DO UPDATE SET quantity = quantity + ?", (id.0,article_id,quantity,quantity)).unwrap();
                    }
                    Task::RemoveArticle {
                        id,
                        article_id,
                        quantity,
                    } => {
                        sqlite_trans.execute("UPDATE quantity SET quantity = quantity - ? WHERE box_id=? AND article_id = ?", (quantity, id.0, article_id)).unwrap();
                    }
                }
            }
            sqlite_trans.commit().unwrap();
        }

        (conn, ops)
    }
    pub fn single_query(conn: &mut Connection, boxid: BoxId, article_id: ArticleId) -> u32 {
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
}

//TODO: Rename this module
pub mod noatun_bench {
    use crate::{ArticleId, BoxId, Task, TasksInTransaction};
    use noatun::data_types::{NoatunHashMap, NoatunOption, NoatunVec};
    use noatun::database::DatabaseSettings;
    use noatun::{
        msg_deserialize, msg_serialize, noatun_object, DatabaseRoot, Database, Message,
        MessageFrame, NoatunCell, NoatunTime,
    };
    use std::io::Write;
    use std::pin::Pin;

    noatun_object!(
        struct StorageBox {
            pod id: u32,
            object articles: NoatunHashMap<u32, NoatunCell<u32>>,
            pod parent: NoatunOption<u32>,
            object children: NoatunVec<NoatunCell<u32>>,
        }
    );

    noatun_object!(
        struct MainDoc {
            object boxes: NoatunHashMap<u32, StorageBox>
        }
    );

    impl DatabaseRoot for MainDoc {
        type Message = Task;
        type Params = ();
    }
    impl Message for Task {
        type Root = MainDoc;

        fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
            let mut project = root.pin_project();

            match self {
                Task::CreateBox { id } => {
                    project.boxes.insert(
                        id.0,
                        &StorageBoxDetached {
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
                        let mut abox = abox.pin_project();
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
                            let mut parent_proj = parent_obj.pin_project();
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
                        if let Some(mut art) = abox.articles.as_mut().get_mut_val(article_id) {
                            let mut cur = art.get();
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

        fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
        where
            Self: Sized,
        {
            msg_deserialize(buf)
        }

        fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
            msg_serialize(self, writer)
        }
    }

    pub fn single_query(database: &Database<MainDoc>, boxid: BoxId, article_id: ArticleId) -> u32 {
        let sess = database.begin_session().unwrap();

        sess.with_root(|root| {
            let mut temp = Vec::with_capacity(20);
            temp.push(boxid.0);
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

    pub fn run_test(
        input: impl Iterator<Item = TasksInTransaction>,
        in_memory: bool,
    ) -> (Database<MainDoc>, usize) {
        let noatun_time = NoatunTime::now();

        let mut db;
        if in_memory {
            db = Database::<MainDoc>::create_in_memory(
                50_000,
                DatabaseSettings {
                    auto_delete: false,
                    ..DatabaseSettings::default()
                },
                (),
            )
            .unwrap();
        } else {
            db = Database::<MainDoc>::create_new(
                "test_noatun.bin",
                true,
                DatabaseSettings {
                    auto_delete: true,
                    max_file_size: 1000_000_000,
                    ..DatabaseSettings::default()
                },
                (),
            )
            .unwrap();
        }



        let mut tot_events = 0;
        let mut sess = db.begin_session_mut().unwrap();
        //sess.disable_filesystem_sync().unwrap();
        for trans in input {
            tot_events += trans.0.len();
            sess.append_many_local_at(noatun_time, trans.0.into_iter())
                .unwrap();
        }
        drop(sess);

        (db, tot_events)
    }
}

pub fn setup_tracing() {
    let stdout_log = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .pretty()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(stdout_log);
    _ = tracing::subscriber::set_global_default(subscriber);
}

fn main() {
    setup_tracing();

    //TODO: Clean up this "benchmark"
    
    let TRANSACTION_COUNT: usize = 1000;
    let gen_task = || generate_sequence(SmallRng::seed_from_u64(1)).take(TRANSACTION_COUNT);

    let mut tot_noatun_sum = 0;
    let mut tot_sqlite_sum = 0;

    let task1 = gen_task().collect::<Vec<_>>();
    let task2 = gen_task().collect::<Vec<_>>();

    let bef = Instant::now();
    let (mut db, event_count) = noatun_bench::run_test(task1.into_iter(), false);
    let t = bef.elapsed();
    println!(
        "Inserts using noatun: {:?} ({} events in {} transactions, {} ops/s)",
        t,
        event_count,
        TRANSACTION_COUNT,
        event_count as f64 / t.as_secs_f64()
    );
    let bef = Instant::now();
    for boxid in 0..4 {
        for article_id in 0..4 {
            let sum = noatun_bench::single_query(&db, BoxId(boxid), ArticleId(article_id));
            tot_noatun_sum += sum;
        }
    }
    let t = bef.elapsed();
    println!(
        "Queries using noatun: {:?} ({} per second)",
        t,
        16.0 / t.as_secs_f32()
    );

    let bef = Instant::now();
    let (mut conn, op_count) = sqlite_bench::run(task2.into_iter());
    let t = bef.elapsed();
    println!(
        "Inserts using sqlite: {:?} ({} ops in {} transactions, {} ops/s)",
        t,
        op_count,
        TRANSACTION_COUNT,
        op_count as f64 / t.as_secs_f64()
    );

    let bef = Instant::now();
    for boxid in 0..4 {
        for article_id in 0..4 {
            let sum = sqlite_bench::single_query(&mut conn, BoxId(boxid), ArticleId(article_id));
            tot_sqlite_sum += sum;
        }
    }
    let t = bef.elapsed();
    println!(
        "Queries using sqlite: {:?} ({} per second))",
        t,
        16.0 / t.as_secs_f32()
    );

    assert_eq!(tot_noatun_sum, tot_sqlite_sum);
}
