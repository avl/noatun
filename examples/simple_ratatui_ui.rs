//! Simple example-application that demonstrates using the ratatui-feature
//! to show diagnostics information.
//!
//! The app creates a single message, without first waiting for any neighbors, and then
//! distributes this message. This is not really a meaningful way to use noatun,
//! but this simple example is only intended to demonstrate the ratatui ui.
//!
//! This app does not persist anything to disk.
use noatun::communication::udp::TokioUdpDriver;
use noatun::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::{
    noatun_object, ratatui_inspector, Database, DatabaseSettings, Message, MessageId,
    SavefileMessageSerializer,
};
use savefile_derive::Savefile;
use std::error::Error;
use std::fmt::Debug;
use std::pin::Pin;

noatun_object!(
    struct Counter {
        pod value: u32
    }
);

#[derive(Savefile, Debug)]
pub struct IncrementMessage;

impl Message for IncrementMessage {
    type Root = Counter;
    type Serializer = SavefileMessageSerializer<IncrementMessage>;

    fn apply(&self, _id: MessageId, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        root.value += 1;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db: Database<IncrementMessage> =
        Database::create_in_memory(10_000_000, DatabaseSettings::default()).unwrap();

    let distributed_db = DatabaseCommunication::new_custom(
        &mut TokioUdpDriver,
        db,
        DatabaseCommunicationConfig {
            enable_diagnostics: true,
            ..DatabaseCommunicationConfig::default()
        },
    )
    .await
    .unwrap();

    distributed_db.add_message(IncrementMessage).await.unwrap();

    ratatui_inspector::run_inspector(None, &distributed_db).unwrap();
    Ok(())
}
