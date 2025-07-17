# Noatun

Welcome to Noatun! Noatun is an in-process, multi master, distributed event sourced database with automatic
garbage collection. It's suitable for unreliable networks and can be used in embedded applications.

Features:

 * Multi master distribution - writes can be made on any node, even offline
 * Decentralized - nodes in the network do not need to be assigned unique ids
 * Data model is 100% event based. Current database state is a function only of current events.
 * Works in any network - does not require unique network addresses
 * Deterministic replay and time travel for easy debugging
 * Robust persistent store optimized for availability

## How it works

At the base of Noatun is an event log. Everything that happens in a Noatun database happens
because of an event. The only way to affect the state of a Noatun database is to create and
add an event.

Events are applied to the database in timestamp order. As a user of Noatun, you need to
define a function that applies an event to the database. Noatun then ensures that the
database is always just the result of applying all events in order. Noatun will, under the hood,
efficiently roll back and reapply events if they arrive over the network out of order.

However, events don't necessarily remain in the database forever. If all the effects of an
event have been overwritten by later events, Noatun will prune the first event. This means
that a Noatun application can work for an indefinite time period without growing indefinitely
in size (given that previous events are actually logically subsumed by later events).

## How to use

Let's say you wanted to track the number of bolts in a warehouse. Bolts are added
to the warehouse, removed, and occasionally an inventory is performed where the number
of bolts are counted to make sure the tally is correct.

```rust
use noatun::{Database, OpenMode, NoatunTime, noatun_object, Message, DatabaseSettings, PostcardMessageSerializer};
use noatun::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::communication::udp::TokioUdpDriver;
use serde_derive::{Serialize, Deserialize};
use tokio::time::Duration;
use std::pin::Pin;
use std::io::Write;

/// Define our events
/// We use serde + postcard in this example, but noatun isn't tied to
/// serde in any way (see further below). 
#[derive(Debug, Serialize, Deserialize)]
pub enum WarehouseEvent {
    Add(u32),
    Remove(u32),
    Inventory(u32)
}

/// Define our root database object. Here we have a single pod (plain old data) field of type u32.
/// See docs for what types are supported by the noatun_object macro.
/// It is also possible to implement completely custom types by implementing the [`noatun::Object`]
/// macro manually.
noatun_object!(
    struct Warehouse {
        pod quantity: u32
    }
);

/// Implement Message for our WarehouseEvent, to tell Noatun how to apply these events to the db
impl Message for WarehouseEvent {
    /// The type of database root this event must be used with
    type Root = Warehouse;
    
    /// The on-disk/on-wire format of messages is customizable.
    /// Here we use the serde-based "postcard" serializer.
    type Serializer = PostcardMessageSerializer<Self>;
    
    /// A function which applies an event to a database with `Warehouse`
    /// as its root object.
    fn apply(&self, _time: NoatunTime, root: Pin<&mut Warehouse>) {
        let mut root = root.pin_project();
        match self {
            WarehouseEvent::Add(delta) => {
                root.quantity += *delta;
            }
            WarehouseEvent::Remove(delta) => {
                root.quantity -= *delta;
            }
            WarehouseEvent::Inventory(qty) => {
                root.quantity.set(*qty);
            }
        }
    }
}

/// Open a database, add some events, and then synchronize with any
/// other reachable noatun nodes.
async fn example() {
    
    /// Create the database on disk
    /// Note, this example creates a purely local database. See further examples
    /// for how to setup synchronization.
    let mut db: Database<WarehouseEvent> = Database::create_new(
        "warehouse_db",
        OpenMode::OpenCreate,
        DatabaseSettings::default(),
    ).unwrap();

    /// Arrange for the database to be distributed
    /// We use a standard UDP driver here, but anything implementing the trait
    /// [`noatun::CommunicationDriver`] can be used.
    let distributed_db = DatabaseCommunication::new_custom(
        &mut TokioUdpDriver, 
        db,
        DatabaseCommunicationConfig::default())
        .await
        .unwrap();
    
    // Add two events, adding a quantity of 43, and then subtracting 1
    distributed_db.add_message(WarehouseEvent::Add(43)).await.unwrap();
    distributed_db.add_message(WarehouseEvent::Remove(1)).await.unwrap();


    distributed_db.with_root(|root|{
        // The current quantity in the database should now be 42.
        assert_eq!(*root.quantity, 42);
    });
    
    // ... run application.
    // Noatun shuts down when `distributed_db` is dropped.
    tokio::time::sleep(Duration::from_secs(30)).await;
}


```
