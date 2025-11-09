use rand::prelude::SmallRng;
use rand::Rng;
use crate::{BenchmarkTask, TasksInTransaction};

#[derive(Savefile, Debug)]
pub enum KeyValOperation {
    AddKeyVal(String, String),
    RemoveKey(String),
}
pub struct KeyValQuery {
    pub key: String,
}
pub const VALUE_SIZE: usize = 256;
fn random_string(rng: &mut SmallRng) -> String {
    let mut temp = String::with_capacity(VALUE_SIZE);
    for _ in 0..VALUE_SIZE {
        temp.push(char::from(65+rng.gen_range(0..25)));
    }

    temp
}
impl BenchmarkTask for KeyValOperation {
    type Query = KeyValQuery;

    fn generate_tasks(mut rng: SmallRng) -> impl Iterator<Item=Self> {
        std::iter::from_fn(move || {
            match rng.gen_range(0..100) {
                0 => {
                    let key = rng.gen_range(0..u32::MAX);
                    Some(KeyValOperation::RemoveKey(key.to_string()))
                }
                _ =>  {
                    let key = rng.gen_range(0..u32::MAX);
                    let val = random_string(&mut rng);
                    Some(KeyValOperation::AddKeyVal(key.to_string(), val))
                }
            }
        })
    }

    fn generate_queries(mut rng: SmallRng) -> impl Iterator<Item=Self::Query> {
        std::iter::from_fn(move || {
            let key = rng.gen_range(0..u32::MAX).to_string();
            Some(KeyValQuery {
                key,
            })
        }).take(1000)
    }

    fn name() -> &'static str {
        "key_value_store"
    }
}

