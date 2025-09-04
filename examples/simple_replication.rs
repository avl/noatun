//! Minimal app that replicates text messages
//!
//! To use, start the program, enter text, and press enter.
//! A list of all texts added (on any node) will be shown.
use noatun::communication::udp::TokioUdpDriver;
use noatun::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::data_types::{NoatunHashMap, NoatunString};
use noatun::{
    noatun_object, Database, DatabaseSettings, Message, MessageId, Object,
    SavefileMessageSerializer,
};
use savefile_derive::Savefile;
use std::error::Error;
use std::fmt::Debug;
use std::io::stdin;
use std::pin::Pin;

noatun_object!(
    struct TodoItem {

        object text: NoatunString,
    }
);

noatun_object!(
    struct TodoList {
        object items: NoatunHashMap<MessageId, NoatunString>,
    }
);

#[derive(Savefile, Debug)]
pub enum TodoMessage {
    AddText(String),
    RemoveText(MessageId),
}

impl Message for TodoMessage {
    type Root = TodoList;
    type Serializer = SavefileMessageSerializer<TodoMessage>;

    fn apply(&self, id: MessageId, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        match self {
            TodoMessage::AddText(text) => {
                root.items.insert(id, text.as_str());
            }
            TodoMessage::RemoveText(id) => {
                root.items.remove(id);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db: Database<TodoMessage> =
        Database::create_in_memory(10_000_000, DatabaseSettings::default()).unwrap();

    let distributed_db = DatabaseCommunication::new_custom(
        &mut TokioUdpDriver,
        db,
        DatabaseCommunicationConfig::default(),
    )
    .await
    .unwrap();

    loop {
        let mut buffer = String::new();

        stdin().read_line(&mut buffer)?;

        if !buffer.is_empty() {
            distributed_db
                .add_message(TodoMessage::AddText(buffer.trim().to_string()))
                .await?;
        }
        distributed_db.with_root(|root| {
            let mut items: Vec<_> = root.items.iter().map(|(k, v)| (*k, v.export())).collect();
            items.sort();
            println!("All messages:");
            for (_msg_id, item) in items {
                println!("{item}");
            }
        })
    }
}
