# Introduction to Noatun

Welcome to Noatun! Noatun is an in-process, multi master, distributed event sourced database with automatic
garbage collection and an materialized view support. It's suitable for unreliable networks and can be 
used in embedded/edge applications (though std required). Noatun is written in 100% Rust.

Unique selling points:
 * Robust, completely automatic non-centralized multi-master replication
 * Full functionality even offline, even for long offline periods.
 * Faked linear history: your application doesn't have to consider concurrency. You're an  
   expert in your domain, you shouldn't have to solve distributed computing just because your customers 
   have offline requirements. 
 * Perfectly suited for when internet connectivity cannot be guaranteed.

Additional features:
 * 100% decentralized - nodes in the network do not need to be assigned unique ids - all they need
   to agree on is the event format and definition. 
 * Data model is 100% event based. Current database state is a function only of current events.
 * Works in any network (and does not require unique network addresses)
 * Deterministic replay and time travel for easy debugging
 * Robust persistent store optimized for availability
 * Automatic pruning of stale events
 * Excellent read performance. Reading from Noatun is almost as fast as reading from regular pure 
   in-memory rust data structures.
 * Good write performance. Writing events to disk is very fast, and projecting them
   to the materialized view is often fast too (but depends on the user logic).

## Functional overview

At the base of Noatun is an event log. Everything that happens in a Noatun database happens
because of an event. The only way to affect the state of a Noatun database is to create an event.

Each Noatun database has two parts: 
 * Event store: contains all events in the database
 * Materialized view: maintained by "applying" all events in order


<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -201 835.4687500000001 402" style="max-width: 835.4687500000001px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(412.734375, -175)" id="U" class="node default default flowchart-label"><rect height="42" width="825.46875" y="-21" x="-412.734375" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-39.140625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="78.28125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Application</span></div></foreignObject></g></g><g transform="translate(412.734375, -75)" id="NoatunApi" class="node default default flowchart-label"><rect height="42" width="825.46875" y="-21" x="-412.734375" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-37.359375, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="74.71875"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">NoatunApi</span></div></foreignObject></g></g><g transform="translate(412.734375, 75)" id="ID" class="node default default flowchart-label"><rect height="42" width="825.46875" y="-21" x="-412.734375" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(85.746875, 75)" id="E" class="node default default flowchart-label"><rect height="26" width="155.49375" y="-13" x="-77.746875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-41.8046875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="83.609375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event Store</span></div></foreignObject></g></g><g transform="translate(412.734375, 75)" id="Projector" class="node default default flowchart-label"><rect height="26" width="155.49375" y="-13" x="-77.746875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-32.015625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="64.03125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Projector</span></div></foreignObject></g></g><g transform="translate(739.7218750000001, 75)" id="M" class="node default default flowchart-label"><rect height="26" width="155.49375" y="-13" x="-77.746875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-62.546875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="125.09375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Materialized View</span></div></foreignObject></g></g><g transform="translate(412.734375, 175)" id="Disk" class="node default default flowchart-label"><rect height="42" width="825.46875" y="-21" x="-412.734375" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-15.5546875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="31.109375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Disk</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-U-NoatunApi" d="M412.734,-154L412.734,-149.167C412.734,-144.333,412.734,-134.667,412.734,-125.667C412.734,-116.667,412.734,-108.333,412.734,-104.167L412.734,-100"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-NoatunApi-E" d="M366.956,-54L347.337,-45C327.718,-36,288.479,-18,246.94,1.055C205.401,20.111,161.561,40.221,139.641,50.277L117.722,60.332"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-M-NoatunApi" d="M711.383,62L688.857,51.667C666.331,41.333,621.28,20.667,579.741,1.611C538.202,-17.444,500.175,-34.888,481.162,-43.61L462.148,-52.332"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-E-Projector" d="M163.494,75L177.785,75C192.076,75,220.658,75,248.574,75C276.49,75,303.739,75,317.363,75L330.988,75"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Projector-M" d="M490.481,75L504.772,75C519.064,75,547.646,75,575.561,75C603.477,75,630.726,75,644.351,75L657.975,75"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-M-Projector" d="M661.975,75L647.684,75C633.393,75,604.81,75,576.895,75C548.979,75,521.73,75,508.106,75L494.481,75"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-E-Disk" d="M128.255,88L148.419,94.167C168.584,100.333,208.912,112.667,244.243,123.472C279.574,134.277,309.908,143.553,325.075,148.192L340.242,152.83"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-M-Disk" d="M697.214,88L677.049,94.167C656.885,100.333,616.557,112.667,581.225,123.472C545.894,134.277,515.561,143.553,500.394,148.192L485.227,152.83"></path><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Disk-M" d="M481.402,154L497.206,149.167C513.011,144.333,544.619,134.667,579.95,123.862C615.282,113.057,654.335,101.113,673.862,95.142L693.388,89.17"></path></g></svg>


_Information flow (in operation)_

Events are applied to the projection in timestamp order. As a user of Noatun, you need to
implement a method that applies an event to the database (method `Message::apply` of trait `Message`). 
Noatun then ensures that the materialized view is always the result of applying all events 
in order. Noatun will, under the hood, efficiently roll back and reapply events if they 
arrive over the network out of order.

However, events don't necessarily remain in the database forever. If all the effects of an
event have been overwritten by later events, Noatun will prune the first event. This means
that a Noatun application can work for an indefinite time period without growing indefinitely
in size (given that previous events are actually logically subsumed by later events).

## Complete example

Let's say you wanted to track the number of bolts in a warehouse. Bolts are added
to the warehouse, removed, and occasionally an inventory is performed where the number
of bolts are counted to make sure the tally is correct.

Example code:

```rust
use noatun::{Database, OpenMode, MessageId, noatun_object, Message, DatabaseSettings, PostcardMessageSerializer};
use noatun::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::communication::udp::TokioUdpDriver;
use serde_derive::{Serialize, Deserialize};
use tokio::time::Duration;
use std::pin::Pin;
use std::io::Write;

/// Define our events
/// 
/// For serialization of events (to disk and on network), we use 
/// serde + postcard. However, Noatun itself isn't tied to serde in 
/// any way (see further below). 
#[derive(Debug, Serialize, Deserialize)]
pub enum WarehouseEvent {
    Add(u32),
    Remove(u32),
    Inventory(u32)
}

/// Define our root database object. Here we have a single pod (plain old data) field of type u32.
/// (This is an oversimplified example.)
/// See docs for what types are supported by the noatun_object macro.
/// It is also possible to implement completely custom types by implementing the [`crate::prelude::Object`]
/// macro manually.
noatun_object!(
    struct Warehouse {
        pod quantity: u32
    }
);

/// Implement Message for our WarehouseEvent, to tell Noatun how to apply these events to the db
impl Message for WarehouseEvent {
    /// The type of database materialized view root this event must be used with
    type Root = Warehouse;
    
    /// The on-disk/on-wire format of messages is customizable.
    /// Here we use the serde-based "postcard" serializer.
    type Serializer = PostcardMessageSerializer<Self>;
    
    /// A function which applies an event to a database with `Warehouse`
    /// as its root object.
    fn apply(&self, _id: MessageId, root: Pin<&mut Warehouse>) {
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

    // As an example, read from the database
    distributed_db.with_root(|root|{
        // This will print 42 (unless another node managed to connect
        // and inserted more messages):
        println!("Quantity: {}", root.quantity.get());
    });
    
    // ... run application.
    // Noatun shuts down when `distributed_db` is dropped.
    tokio::time::sleep(Duration::from_secs(30)).await;
}


```
# Message model

Noatun messages consist of three parts:
 * A 16 byte message id (of which 48 bits are a timestamp)
 * A list of parents (message ids)
 * A user-defined payload

<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -43 348.84375 86" style="max-width: 348.84375px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}#div .BT&gt;*{stroke:transparent!important;fill:transparent!important;}#div .BT span{stroke:transparent!important;fill:transparent!important;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(169.421875, 0)" id="id-sbi8qboa77q-9" class="node default default flowchart-label"><rect height="76" width="338.84375" y="-38" x="-169.421875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(59.140625, -17)" id="uct1" class="node default BT flowchart-label"><rect height="26" width="102.28125" y="-13" x="-51.140625" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-32.4609375, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="64.921875"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Message</span></div></foreignObject></g></g><g transform="translate(59.140625, 17)" id="t" class="node default default flowchart-label"><rect height="26" width="102.28125" y="-13" x="-51.140625" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-39.1328125, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="78.265625"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">MessageId</span></div></foreignObject></g></g><g transform="translate(169.421875, 17)" id="b" class="node default default flowchart-label"><rect height="26" width="102.28125" y="-13" x="-51.140625" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-35.5703125, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="71.140625"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Parent list</span></div></foreignObject></g></g><g transform="translate(279.703125, 17)" id="c" class="node default default flowchart-label"><rect height="26" width="102.28125" y="-13" x="-51.140625" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-47.140625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="94.28125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">User payload</span></div></foreignObject></g></g></g></svg>

_Message layout_

Message parents are handled automatically by Noatun. See chapter on Internals for more information. 
The user payload serialization format is user-defined. By default Noatun uses serde postcard, but
any serialization mechanism can be used.

# Data types supported by Noatun

Noatun is completely unopinionated when it comes to message formats.

However, when it comes to the materialized view, noatun ships with a set of standard types. The user
can define their own types to extend this standard set of types.

Some basic types:
 * NoatunCell  - wrapper around primitives and many other Copy-types
 * NoatunHashMap - hash map for Noatun
 * NoatunString - Noatun equivalent to std::string::String
 * Struct types defined using `noatun_object!`-macro.
 * Pods defined using `noatun_pod!`-macro.

## Defining custom types

The `noatun_object!` macro can be used to define a custom object types like this:

```rust
noatun_object!{
    /// Documentation for struct
    struct MyType {
        /// Documentation for field
        pod foo: u32,
        /// Documentation for other field
        object bar: NoatunHashMap<u32, NoautunString>,
    }
}
```
Map-keys must implement `NoatunKey`. Map-values must be objects. Pod-types can
be turned into objects by wrapping in `NoatunCell`.

The `noatun_pod!`-macro can be used to define custom pod types:

```rust
noatun_pod! {
    /// Documentation for struct
    struct MyPodType {
        foo: u32,
        bar: u16
    }
}

noatun_pod!{
    struct MyTuplePodType(u32,u16)
}
```
All fields of a pod type must also be pod-types.



## Objects vs NoatunPods

Data stored in Noatun must implement the `Object` trait. Implementors of Object
know to import/export, allocate, initialize and destroy themselves. Objects also
implement read/write dependency tracking through use of `Tracker`s.

Naturally, Noatun supports storing primitives (u8, u16, u32 etc). These types do not implement `Object`,
but must be wrapped in a type that does. The types `NoatunCell` and `OpaqueNoatunCell` serve this function.
These cell types (`NoatunCell` and `OpaqueNoatunCell`) add read/write dependency tracking. 

In addition to rust's standard primitives, any type implementing `NoatunPod` can be used inside such a cell.
In order to implement `NoatunPod`, a type must have a stable memory layout and must be Copy.
It must also implement the `NoatunStorable` trait.  See `NoatunPod` and `NoatunStorable` docs for a complete list of 
requirements.

## Key types
Hashmaps are a central datatype, that is well suited for use with noatun (see type `NoatunHashmap`).
Hashmap keys must be pods. In addition, they must implement the `NoatunKey`-trait. This trait provides
predictable (non-randomized) hashing. The regular rust hash functions can't be used since the random
seed is different for each execution, making on-disk hashmaps invalid after restart.


## Native types

As noted in the previous section, arbitrary objects cannot be stored directly in a Noatun-database. Instead,
special types need to be used. For convenience, these types can be "exported" into "native" types. For example,
the native type of NoatunString is simply `std::string::String`. 

The reason that native types can't be used directly in noatun is that they do not have a guaranteed memory layout.
If they were stored directly in a noatun database, there would be no guarantee that persisted data would remain
valid if the application was recompiled.

To retrieve an instance of native type, use the `Object::export` method. To write a native type to a noatun object,
use `Object::init_from`.

## Schema hash

All types storable in a noatun materialized view have a concept of a schema. Noatun persisted the hash
of the complete db schema, and rebuild the materialized view if this hash doesn't match between the data types
in memory and the format on disk. 


# Features

## Automatic pruning

### Introduction

Messages are automatically removed from the database when they are no longer needed.

The basic approach is that Noatun tracks exactly what information a message's `apply`-method writes.
Once all that information has been overwritten, the message can be removed (with some caveats).

<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -111 630.375 222" style="max-width: 630.375px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}#div .BT&gt;*{stroke:transparent!important;fill:transparent!important;}#div .BT span{stroke:transparent!important;fill:transparent!important;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(74.546875, 0)" id="id-9hv3im2806-12" class="node default default flowchart-label"><rect height="212" width="149.09375" y="-106" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(74.546875, -85)" id="uct1" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-41.8046875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="83.609375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event Store</span></div></foreignObject></g></g><g transform="translate(74.546875, -51)" id="Event1" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event1</span></div></foreignObject></g></g><g transform="translate(74.546875, 17)" id="Event2" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event2</span></div></foreignObject></g></g><g transform="translate(74.546875, 85)" id="Event3" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event3</span></div></foreignObject></g></g><g transform="translate(545.828125, -17)" id="id-2ntbtpvzu6e-17" class="node default default flowchart-label"><rect height="178" width="149.09375" y="-89" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(545.828125, -85)" id="uct2" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-62.546875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="125.09375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Materialized View</span></div></foreignObject></g></g><g transform="translate(545.828125, -51)" id="FieldA" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldA</span></div></foreignObject></g></g><g transform="translate(545.828125, 51)" id="FieldB" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldB</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event1-FieldA" d="M141.094,-51L169.276,-51C197.458,-51,253.823,-51,309.521,-51C365.219,-51,420.25,-51,447.766,-51L475.281,-51"></path><g transform="translate(310.1875, -51)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event1-FieldB" d="M134.612,-38L163.875,-31.667C193.137,-25.333,251.662,-12.667,309.536,-0.141C367.409,12.385,424.631,24.769,453.242,30.962L481.853,37.154"></path><g transform="translate(310.1875, 0)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event2-FieldB" d="M141.094,21.801L169.276,23.834C197.458,25.867,253.823,29.934,309.523,33.952C365.222,37.97,420.257,41.941,447.774,43.926L475.292,45.911"></path><g transform="translate(310.1875, 34)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event3-FieldA" d="M119.596,72L151.361,62.833C183.126,53.667,246.657,35.333,309.547,17.185C372.437,-0.964,434.687,-18.927,465.811,-27.909L496.936,-36.891"></path><g transform="translate(310.1875, 17)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g></g></svg>


_Basic Example_

Event 1 writes both fields. After Event2 has been written, Event1 still needs to be retained, since
it wrote the most recent value to "FieldB". However, after Event3 has been written, none of what
Event1 wrote is still in the database, and Event1 will now be automatically pruned (note this is not always 100% true, 
please continue reading).

However, consider what happens if messages (actually, the `apply` method of Message impls) also read from the database:


<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -111 630.375 222" style="max-width: 630.375px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}#div .BT&gt;*{stroke:transparent!important;fill:transparent!important;}#div .BT span{stroke:transparent!important;fill:transparent!important;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(74.546875, 0)" id="id-k9p2eahc9k-20" class="node default default flowchart-label"><rect height="212" width="149.09375" y="-106" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(74.546875, -85)" id="uct1" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-41.8046875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="83.609375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event Store</span></div></foreignObject></g></g><g transform="translate(74.546875, -51)" id="Event1" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event1</span></div></foreignObject></g></g><g transform="translate(74.546875, 17)" id="Event2" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event2</span></div></foreignObject></g></g><g transform="translate(74.546875, 85)" id="Event3" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event3</span></div></foreignObject></g></g><g transform="translate(545.828125, -17)" id="id-kb81qqx6keg-25" class="node default default flowchart-label"><rect height="178" width="149.09375" y="-89" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(545.828125, -85)" id="uct2" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-62.546875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="125.09375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Materialized View</span></div></foreignObject></g></g><g transform="translate(545.828125, -51)" id="FieldA" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldA</span></div></foreignObject></g></g><g transform="translate(545.828125, 51)" id="FieldB" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldB</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event1-FieldA" d="M141.094,-51L169.276,-51C197.458,-51,253.823,-51,309.521,-51C365.219,-51,420.25,-51,447.766,-51L475.281,-51"></path><g transform="translate(310.1875, -51)" class="edgeLabel"><g transform="translate(-23.5625, -9)" class="label"><foreignObject height="18" width="47.125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write:1</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event1-FieldB" d="M134.612,-38L163.875,-31.667C193.137,-25.333,251.662,-12.667,309.536,-0.141C367.409,12.385,424.631,24.769,453.242,30.962L481.853,37.154"></path><g transform="translate(310.1875, 0)" class="edgeLabel"><g transform="translate(-23.5625, -9)" class="label"><foreignObject height="18" width="47.125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write:1</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-FieldA-Event2" d="M479.281,-41.398L451.099,-37.332C422.917,-33.265,366.552,-25.133,310.847,-17.095C255.143,-9.058,200.098,-1.115,172.575,2.856L145.053,6.827"></path><g transform="translate(310.1875, -17)" class="edgeLabel"><g transform="translate(-22.6875, -9)" class="label"><foreignObject height="18" width="45.375"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">read:1</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event2-FieldB" d="M141.094,21.801L169.276,23.834C197.458,25.867,253.823,29.934,309.523,33.952C365.222,37.97,420.257,41.941,447.774,43.926L475.292,45.911"></path><g transform="translate(310.1875, 34)" class="edgeLabel"><g transform="translate(-23.5625, -9)" class="label"><foreignObject height="18" width="47.125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write:2</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event3-FieldA" d="M119.596,72L151.361,62.833C183.126,53.667,246.657,35.333,309.547,17.185C372.437,-0.964,434.687,-18.927,465.811,-27.909L496.936,-36.891"></path><g transform="translate(310.1875, 17)" class="edgeLabel"><g transform="translate(-23.5625, -9)" class="label"><foreignObject height="18" width="47.125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write:3</span></div></foreignObject></g></g></g></svg>

_Messages with dependencies_


In this example, none of the messages can be deleted. Even though none of the values written by Event1 remain in the 
database, the value "1", written to field A, was later read by Event2. Any value subsequently written by Event2
(i.e, the write to Field B) might depend on the value read from field A. In fact, it is highly likely that the
value written to field B depends on what wa read from field A. Otherwise, the implementation of Event2 should just
be changed to eliminate an apparently useless read. 

Noatun tracks this type of information flow dependency between events, and will thus _not_ prune Event1 in this case.

### Automatic Pruning details

As we saw in the previous section, reads introduce dependencies between events that may inhibit automatic
pruning. This is generally a good thing. Without this, messages couldn't safely build upon data in
the materialized view that was written by earlier messages. Doing so would cause unexpected effects
if/when those earlier messages were completely overwritten. 

For example, consider a simple counter, which registers the number of clicks on a button. Each
message would read the previous counter value, increment it, and write it back to the counter.
If dependencies were not tracked, the counter value would never increment far, since every message
would cause the previous message to be pruned.

This sort of dependency tracking is not without problems (though Noatun solves them for you).

### Actual reads vs potential reads, and the cutoff interval

As we saw in the previous section, when a message apply reads from the materialized view, this creates
a read dependency. However, messages can arrive to a node out-of-order. This means that even if no readers
currently exist locally, they could exist elsewhere in the distributed system. 

Let's look at a simple example:

<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -94 630.375 188" style="max-width: 630.375px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}#div .BT&gt;*{stroke:transparent!important;fill:transparent!important;}#div .BT span{stroke:transparent!important;fill:transparent!important;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(74.546875, 0)" id="id-wzetfs94yrs-27" class="node default default flowchart-label"><rect height="178" width="149.09375" y="-89" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(74.546875, -63.75)" id="uct1" class="node default BT flowchart-label"><rect height="34.5" width="133.09375" y="-17.25" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-41.8046875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="83.609375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event Store</span></div></foreignObject></g></g><g transform="translate(74.546875, -21.25)" id="Event1" class="node default default flowchart-label"><rect height="34.5" width="133.09375" y="-17.25" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event1</span></div></foreignObject></g></g><g transform="translate(74.546875, 63.75)" id="Event2" class="node default default flowchart-label"><rect height="34.5" width="133.09375" y="-17.25" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event2</span></div></foreignObject></g></g><g transform="translate(545.828125, 0)" id="id-p3nr6w5sgoo-32" class="node default default flowchart-label"><rect height="178" width="149.09375" y="-89" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(545.828125, -68)" id="uct2" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-62.546875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="125.09375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Materialized View</span></div></foreignObject></g></g><g transform="translate(545.828125, -34)" id="FieldA" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldA</span></div></foreignObject></g></g><g transform="translate(545.828125, 68)" id="FieldB" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldB</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event1-FieldA" d="M141.094,-23.05L169.276,-23.813C197.458,-24.575,253.823,-26.1,309.521,-27.607C365.219,-29.114,420.251,-30.603,447.767,-31.347L475.283,-32.091"></path><g transform="translate(310.1875, -27.625)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event2-FieldA" d="M141.094,49.947L169.276,44.102C197.458,38.257,253.823,26.566,310.18,14.877C366.537,3.187,422.886,-8.5,451.06,-14.344L479.235,-20.188"></path><g transform="translate(310.1875, 14.875)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g></g></svg>


Event2 completely overwrites everything created by Event1. So it may seem we could always prune Event1.

However, this is not the case. It's possible that, sometime after Event1 was created, but before Event2 was created,
on a different node, there may be an Event1.5:


<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -111 630.375 222" style="max-width: 630.375px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}#div .BT&gt;*{stroke:transparent!important;fill:transparent!important;}#div .BT span{stroke:transparent!important;fill:transparent!important;}#div .D&gt;*{stroke-dasharray:5!important;}#div .D span{stroke-dasharray:5!important;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(74.546875, 0)" id="id-tjqpps1ut9f-35" class="node default default flowchart-label"><rect height="212" width="149.09375" y="-106" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(74.546875, -85)" id="uct1" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-41.8046875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="83.609375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event Store</span></div></foreignObject></g></g><g transform="translate(74.546875, -51)" id="Event1" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event1</span></div></foreignObject></g></g><g transform="translate(74.546875, 17)" id="Event15" class="node default D flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-31.578125, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="63.15625"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event1.5</span></div></foreignObject></g></g><g transform="translate(74.546875, 85)" id="Event2" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event2</span></div></foreignObject></g></g><g transform="translate(545.828125, -17)" id="id-vb333g341em-40" class="node default default flowchart-label"><rect height="178" width="149.09375" y="-89" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(545.828125, -85)" id="uct2" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-62.546875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="125.09375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Materialized View</span></div></foreignObject></g></g><g transform="translate(545.828125, -51)" id="FieldA" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldA</span></div></foreignObject></g></g><g transform="translate(545.828125, 51)" id="FieldB" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldB</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event1-FieldA" d="M141.094,-51L169.276,-51C197.458,-51,253.823,-51,309.521,-51C365.219,-51,420.25,-51,447.766,-51L475.281,-51"></path><g transform="translate(310.1875, -51)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-FieldA-Event15" d="M479.281,-41.398L451.099,-37.332C422.917,-33.265,366.552,-25.133,310.847,-17.095C255.143,-9.058,200.098,-1.115,172.575,2.856L145.053,6.827"></path><g transform="translate(310.1875, -17)" class="edgeLabel"><g transform="translate(-16.015625, -9)" class="label"><foreignObject height="18" width="32.03125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">read</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event15-FieldB" d="M141.094,21.801L169.276,23.834C197.458,25.867,253.823,29.934,309.523,33.952C365.222,37.97,420.257,41.941,447.774,43.926L475.292,45.911"></path><g transform="translate(310.1875, 34)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event2-FieldA" d="M119.596,72L151.361,62.833C183.126,53.667,246.657,35.333,309.547,17.185C372.437,-0.964,434.687,-18.927,465.811,-27.909L496.936,-36.891"></path><g transform="translate(310.1875, 17)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g></g></svg>


Since we cannot know if such an event will arrive, we cannot immediately prune event 1.

However, if we know the worst case network propagation time T, we can prune events that have been
unobservable for a time of at least T. In the original example above (before receiving Event 1.5), once time T has passed since the timestamp
of Event2, we know that there can't exist an Event 1.5, because it would have reached us already (by definition).

Noatun exposes this concept as the `cutoff_interval`. The value is configurable in the `DatabaseSettings` struct.

Note, in the example above, it is the timestamp of Event2 (the message that overwrote the last visible piece of
Event1) that the cutoff_interval is relative to.

Note, Noatun verifies that nodes always agree on the set of events with timestamps before `now - cutoff_interval`
(this time is known as the "cutoff_time"). A hash of all messages timestamped before the cutoff time is maintained
and periodically sent to all neighbors. The cutoff_time advances periodically by a the "cutoff stride". When
nodes detect that peers have cutoff intervals in the near future, they immediately advance to be in sync.
Large clock drift is detected and flagged as an error. Noatun requires approximate clock synchronization.


### Avoiding read dependencies in complex apps

For some applications, message pruning is simply not necessary. Consider a distributed bug tracker for
a small team. Noatun will function well with millions of events in the store, and a small team
may never reach this amount of data.

Even if pruning is needed, it is likely to be okay that updates to a specific bug aren't pruned until
the bug is deleted.

But for some applications, this is not enough. Consider a support application for delivery trucks.

Each truck may update its current position once a second. With thousands of trucks, the number of position
updates will soon grow large. However, if we only need the most recent position update, we would like
previous messages to be pruned.

This is easily supported by Noatun. However, a complication to be aware of is that
navigating the materialized view can cause unintended observations. See next section. 


### Early pruning with opaque data

Noatun can sometimes prune data even before the cut-off interval has elapsed. This is possible when
a message has only written "opaque" data to the database. Opaque data is data that cannot be read
by other messages. That is, information that cannot be read while executing in [`crate::prelude::Message::apply`], but only
from [`crate::prelude::DatabaseSession::with_root`]. 

Let's return to a variation of our earlier example:

<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -94 630.375 188" style="max-width: 630.375px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}#div .BT&gt;*{stroke:transparent!important;fill:transparent!important;}#div .BT span{stroke:transparent!important;fill:transparent!important;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(74.546875, 0)" id="id-2477l9m2d6a-42" class="node default default flowchart-label"><rect height="178" width="149.09375" y="-89" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(74.546875, -63.75)" id="uct1" class="node default BT flowchart-label"><rect height="34.5" width="133.09375" y="-17.25" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-41.8046875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="83.609375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event Store</span></div></foreignObject></g></g><g transform="translate(74.546875, -21.25)" id="Event1" class="node default default flowchart-label"><rect height="34.5" width="133.09375" y="-17.25" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event1</span></div></foreignObject></g></g><g transform="translate(74.546875, 63.75)" id="Event2" class="node default default flowchart-label"><rect height="34.5" width="133.09375" y="-17.25" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-24.90625, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="49.8125"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Event2</span></div></foreignObject></g></g><g transform="translate(545.828125, 0)" id="id-9keiirezhzs-47" class="node default default flowchart-label"><rect height="178" width="149.09375" y="-89" x="-74.546875" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(545.828125, -68)" id="uct2" class="node default BT flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-62.546875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="125.09375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">Materialized View</span></div></foreignObject></g></g><g transform="translate(545.828125, -34)" id="FieldA" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldA</span></div></foreignObject></g></g><g transform="translate(545.828125, 68)" id="FieldB" class="node default default flowchart-label"><rect height="26" width="133.09375" y="-13" x="-66.546875" ry="0" rx="0" style="" class="basic label-container"></rect><g transform="translate(-22.6796875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="45.359375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">FieldB</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event1-FieldA" d="M141.094,-23.05L169.276,-23.813C197.458,-24.575,253.823,-26.1,309.521,-27.607C365.219,-29.114,420.251,-30.603,447.767,-31.347L475.283,-32.091"></path><g transform="translate(310.1875, -27.625)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g><path marker-end="url(#div_block-pointEnd)" class="edge-thickness-normal edge-pattern-solid flowchart-link LS-a1 LE-b1" id="1-Event2-FieldA" d="M141.094,49.947L169.276,44.102C197.458,38.257,253.823,26.566,310.18,14.877C366.537,3.187,422.886,-8.5,451.06,-14.344L479.235,-20.188"></path><g transform="translate(310.1875, 14.875)" class="edgeLabel"><g transform="translate(-16.890625, -9)" class="label"><foreignObject height="18" width="33.78125"><div xmlns="http://www.w3.org/1999/xhtml" style="stroke: rgb(51, 51, 51); stroke-width: 1.5px; display: inline-block; white-space: nowrap;"><span class="edgeLabel" style="stroke: #333; stroke-width: 1.5px;color:none;">write</span></div></foreignObject></g></g></g></svg>


If FieldA is an opaque field, we know no message can ever read it. This means that we can be certain that 
the value that Event1 wrote can never be accessed by other messages. Thus, messages that only write opaque data 
can be pruned as soon as all their information has been completely overwritten.


### Collections

Collections offer a challenge. To illustrate this, consider vectors.

It may seem that pushing a new item at the end of a vector [`crate::prelude::NoatunVec`] should not introduce any read dependency.
But actually, the result of such a push depends on the previous contents of, and thus all previous writes
to the vector. The reason for this is that later messages may use the length of the vector in calculations.

Pruning any messages that wrote to a [`crate::prelude::NoatunVec`] would change the later return value of [`crate::prelude::NoatunVec::len`], and
this could change the final materialized state. Because of this [`crate::prelude::NoatunVec`] *does* record a read dependency
on previous messages when pushing to a [`crate::prelude::NoatunVec`].

To work around this, [`crate::prelude::OpaqueNoatunVec`] exists. It works like [`crate::prelude::NoatunVec`], but does not record read dependencies
when pushing new elements. The downside is that it does not support a regular `len` operation. This way,
pruning an element from a `crate::prelude::OpaqueNoatunVec` is not observable to any message. Remember that we never prune a message
if information it wrote could be read by a later message. So if the message we're about to prune wrote an item
that is actually read itself by a message, the pruning will not occur.


### Tombstones

Tombstones are markers that certain information no longer exists. Intuitively, it may seem that information that
no longer exists shouldn't require any information to be stored at all. However, in a distributed system this
isn't always true. The reason is that a node that is not up-to-date could still have information that *should* 
have been deleted. Other nodes thus need to maintain just enough information to be able to communicate that
the deleted information is, in fact, deleted,

Noatun marks messages that delete elements from collections as 'tombstone' messages. These are never pruned
until the cutoff interval has elapsed, even if the message only wrote opaque data. 

Emitting tombstones can be costly, so it can make sense for applications to take care to avoid doing so.

Noatun has a tool for avoiding tombstones in some situations: the `clear` method.

[`crate::prelude::NoatunVec`], [`crate::prelude::OpaqueNoatunVec`] and [`crate::prelude::NoatunHashMap`] all have such a `clear`-method. This method, unsurprisingly,
removes all elements from the collection. But additionally, and crucially, it does this without marking the 
message as a tombstone. Instead, it records itself as the writer of a special 'clear' marker in the collection. 
This write is recorded just like the write to any field. Future calls to 'clear' will overwrite the marker, and 
allow the previous message to be pruned. This is in contrast with tombstone messages, that are never pruned
before cutoff.

### Trackers

The way dependencies are handled in Noatun is through "Trackers". The struct `Tracker` is used to 
record ownership of a piece of data. All tracked data types in Noatun contain a `Tracker` instance.
The tracker simply records the identity of the most recent message that wrote the piece of data. Note,
the data must be completely overwritten, or not written at all. The noatun datatypes ensure this invariant is 
maintained.

When data is read (while building the materialized view), a read dependency is created between the currently
materialized message, and the owner of the tracker that owns the data which was read.

Types with the word `Opaque` in their name still have trackers, but since their data cannot be read
by the `Message::apply`-methods while building the materialized view, such trackers never participate in
establishing read dependencies between messages.


## Validation

Interactive applications often have a need to validate messages before emitting them.

In these situations, applications can use [`crate::prelude::DatabaseSessionMut::with_root_preview`] to
apply a message temporarily, and give the application access to the resulting
materialized view. An application can then run validation on the actual
state resulting from applying the message.

If message application has complex application logic, this can be useful for
reducing code duplication in validators.

After `with_root_preview` returns, the database is restored to the previous
state.

## Undo

There are a few possibilities for undoing events in Noatun:

### Deleting the event

Events can be deleted using [`crate::prelude::DatabaseSessionMut::remove_message`]. Note, however,
that this is a low level operation that should not be used for events that have been
(or may have been) transmitted to other nodes. Noatun still guarantees eventual consistency,
but this will only occur after the cutoff interval has passed, and will be accomplished by
(potentially) transmitting the entire database state (all messages). It is thus strongly
recommended to not remove messages from a database in this way.

### Adding a new event that undoes the previous event

The most straightforward way to handle undo is to create an event that just does
the reverse of the event that is to be undone. This will overwrite all data written
by the original message, and it can then hopefully be automatically pruned.

### Inhibiting a message from being applied

Since messages have access to their id when being applied, it is possible to
maintain a set of 'inhibited' messages. A [`crate::prelude::Message::apply`] implementation can
then check if it has been inhibited before executing the bulk of its body.

Separate 'inhibit' messages can then be defined, that add to the set of inhibited
messages. This way, a message can be inhibited, effectively undoing it. Or to be precise,
it will be as if the message never happened.

The inhibit messages can be created with a MessageId that sorts immediately before
the original message (but still on the same timestamp). See method
[`crate::prelude::MessageId::unique_predecessor`].



# Details and limitations

## Numerical limitations

The size of Noatun databases is, in practice, only bounded by available disk storage and
virtual memory size. The max number of messages stored in Noatun is bounded at 2^32. However, 
many applications never approach this number of simultaneously live messages. 

There is no hard limit to how many nodes a Noatun deployment can contain. In fact, 
the total number of nodes in the network doesn't even have to be known. That said,
due to the way Noatun works, it's recommended to keep the number of active neighbors
(nodes that can speak directly over the network) to below 100 nodes. The number of
actively updated nodes should probably not exceed a few hundred (or the update head
will become too big).

## Noatun requires correct time

In distributed systems, a decision often has to be made whether nodes are required to have
correct time or not. Not all hardware has a battery-backed real time clock, and in an 
offline scenario such systems may have no ability to determine correct time. It can
thus be beneficial for a distributed system not to rely on the correct time being available.

That said, Noatun makes the decision that all nodes must have the correct time. This is
at the heart of the Noatun model. Even without real time clocks, nodes can always
persist the last known correct time. By doing this, nodes can make sure that they either
have the correct time, or have a slow clock. Since all Noatun messages are timestamped,
on receiving a message that appears to be in the future, a node can know to adjust
its clock. No such automatic adjustment mechanism is provided by Noatun itself, it
has to be supplied by the user.

While requiring correct time is a limitation, it is often the case that IT systems
need correct time anyway for other purposes, such as validating certificates, 
correctly timestamping logs, achieving freshness conditions in cryptography, and many more.

The noatun type representing time, [`crate::prelude::NoatunTime`], has a range from the year 1970 to 
the year 10000.


## Logical conflicts during Message::apply

Noatun guarantees that all messages are applied in order. I.e, Noatun will call
the `apply` method of the users `Message` type in timestamp order. If messages arrive 
out-of-order, Noatun will rewind time as needed and re-apply messages. The user
does not have to think about this.

The user code never sees an out-of-order message.

That said, it is possible for different nodes to issue events that logically conflict.
Noatun has no built-in conflict resolution, but since messages are always applied
in order, it is easy to implement "last write wins".

## Philosophy of event applications

As long as all messages represent "an event that actually happened in the real world"
things often turn out fine. 

To illustrate this, consider a naive distributed system that keeps track of a bunch of ice cream carts on
a beach. Each cart is a noatun node. Every time an ice cream is sold, each cart/node records 
the sale in a database:

```ignore
enum Event {
    IceCreamSold(u32)
}

noatun_object!(
    struct SalesStatistics {
        pod total_ice_cream_sold: u32
    }
);

impl Message for Event {
    ..
    fn apply(&self, ..) {
        match self {
            Event::IceCreamSold(sold) => {
                root.total_ice_cream_sold.set(sold);
            }
        }
    }
}
```

Ice cream cart #1 sells 2 cones, and records an event `Event::IceCreamSold(2)`.
This sets the total number of sold cones to 2. So far so good.

Now, ice cream cart #2 sells 3 cones, and records `Event::IceCreamSold(3)`.
This sets the total number to 3.

With the above `apply` definition, this will result in total_ice_cream_sold equal to 3, 
instead of the correct 5. 

The correct apply method should increment `total_ice_cream_sold`, not assign it.

The trouble in the original naive implementation was that IceCreamSold was interpreted
as a global count of sold icecream, something that each ice cream cart did not actually
have information about.

If events only encode actual ground truth information, and no derived information, 
it is often relatively straightforward to correctly implement the [`crate::prelude::Message::apply`] method.

In this example, the information available to each cart was just that a sale had been made
locally, and that was all the information that should be encoded in the message. Deriving
the total count of sold cones could only be done in the `apply`-method.

In general, Noatun events should contain events that exactly reflect what has happened
in the real world, with the timestamp of the actual event, without any extra information.
However, see below for cases where this may be hard to achieve.

## Event design pitfalls

Here we list a few classes of event design pitfalls.

### Including derived information

Let's say you're building a road toll system. The system consists of a number of cameras.
The cameras photograph cars, and register the passage of each car as an event in Noatun.

What's wrong with the following event?

```rust
use noatun::data_types::NoatunString;
enum TollEvent {
    CarPassed {
        license_plate_number: NoatunString,
        owner: NoatunString,
        // .. billing information ..
    }
}
```

The system photographs cars, and extracts the license plate number. It then looks up
the numbers in the vehicle registry, and fills in owner and billing information.

The error here is that vehicle ownership changes are not immediate. Thus,
a car that passed the camera may have changed owner just the minute before (or earlier). Thus,
we should not be including 'owner' in the event, only the license plate number. That is actually
the only information that the camera is sure about.

### Issuing events with the wrong time stamp
Let's say we're building an application to support repair technicians keeping track
of spare parts kits. Each day, every technician randomly grabs a kit before heading out,
then consumes spare parts from this kit during the day. Every such consumption event is
entered into the system. When back at base, the kits are inventoried.

```rust
enum SparePartEvent {
    InventoryKit {
        kit_name: String,
        spare_part_count: u32,
    },
    ConsumeSparePart {
        kit_name: String,
        technician_name: String,
    },
}
```

Imagine a situation where a technician consumed a spare part, but forgot to enter it
into the computer system. The next day, the technician realizes their mistake, and
enters a `ConsumeSparePart` event in into the system.

The problem here is that the kit might already have been inventoried (and the missing
quantity presumably noted), and might now physically be out with some other technician.
Entering the missing `ConsumeSparePart` after-the-fact is only correct if the event
is backdated to the correct time. Usually, such a 'correct' timestamp can be found.

Such backdating is easy in this example, but we'll see in the next chapter a situation where
it's a bit trickier.


### Events with unclear natural time stamps
Let's say we're building a truck fleet management application. The application
manages a fleet of trucks, and keeps track of their maintenance schedules.

Different types of trucks need different maintenance schedules, and these can be changed,
so are kept in the database as separate objects.

Our event model:


```rust
enum MaintenanceEvent {
    NewMaintenancePlan {
        plan_name: String,
        oil_change_interval_days: u32,
        brake_inspection_interval_days: u32,
    },
    AddTruck {
        truck_license_plate: String,
        maintenance_plan: String,
    },
    RecordMaintenance {
        truck: String,
    },
}
```

We initially set up a new plan, say "standard maintenance" with oil change interval
of 180 days and brake inspection interval 360 days. Let's say we set this up on
January 1st 2025. We make sure to timestamp this `NewMaintenancePlan` event before
any `AddTruck` events.

However, after having the system in operation for a while, we expand our fleet
with a new truck. However, it's used, and its last maintenance was on December 1st 2024.

When we enter this event into the system, we notice a problem. We must backdate
the `RecordMaintenance` event to december 1st 2024. But then [`crate::prelude::Message::apply`]
fails, because the maintenance plan doesn't exist yet. It isn't created until 
January 1st 2025. 

We claimed earlier that events should always be entered into the system with their
"natural" timestamp. However, in the situation described here, there isn't really
a natural timestamp for the `NewMaintenancePlan` event. Users are often not accustomed
to event sourced architectures, and might assume that any change to the maintenance plans
affects also data established by events timestamped in the distant past.

Generally, there are two options:

 * Stick with the "events have natural timestamps" idea. In this case there are a few options:
   * Create a new `NewMaintenancePlan` element backdated to December 1st 2024. 
   * Create a new `NewMaintenancePlan` element backdated to January 1st 1970 UTC
     (the earliest supported NoatunTime), and figure out a strategy for when the element
     needs to be updated: Potentially using `Message::unique_successor`.
 * Use 'data entry' timestamps for all elements. That is, timestamp all events with the
   time at which they were entered into the system. This loses some benefits
   of a timestamped event source, but may be the right choice in this particular example.
   In this case, all calculations of maintenance timers has to be done after each message
   application that changes the maintenance plans. Doing this can work, but it reduces 
   the benefit of Noatun, and if such a pattern is prevalent, Noatun may be the wrong choice.  

# Internals

This chapter goes into some of the internals of Noatun. While it can be of interest to users,
the aspiration is that users should not need to know of these details.

## MessageId

<svg aria-roledescription="block" role="graphics-document document" viewBox="-5 -43 418.171875 86" style="max-width: 418.171875px;" xmlns="http://www.w3.org/2000/svg" width="100%" id="div"><style>#div{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;fill:#333;}#div .error-icon{fill:#552222;}#div .error-text{fill:#552222;stroke:#552222;}#div .edge-thickness-normal{stroke-width:1px;}#div .edge-thickness-thick{stroke-width:3.5px;}#div .edge-pattern-solid{stroke-dasharray:0;}#div .edge-thickness-invisible{stroke-width:0;fill:none;}#div .edge-pattern-dashed{stroke-dasharray:3;}#div .edge-pattern-dotted{stroke-dasharray:2;}#div .marker{fill:#333333;stroke:#333333;}#div .marker.cross{stroke:#333333;}#div svg{font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:16px;}#div p{margin:0;}#div .label{font-family:"trebuchet ms",verdana,arial,sans-serif;color:#333;}#div .cluster-label text{fill:#333;}#div .cluster-label span,#div p{color:#333;}#div .label text,#div span,#div p{fill:#333;color:#333;}#div .node rect,#div .node circle,#div .node ellipse,#div .node polygon,#div .node path{fill:#ECECFF;stroke:#9370DB;stroke-width:1px;}#div .flowchart-label text{text-anchor:middle;}#div .node .label{text-align:center;}#div .node.clickable{cursor:pointer;}#div .arrowheadPath{fill:#333333;}#div .edgePath .path{stroke:#333333;stroke-width:2.0px;}#div .flowchart-link{stroke:#333333;fill:none;}#div .edgeLabel{background-color:rgba(232,232,232, 0.8);text-align:center;}#div .edgeLabel rect{opacity:0.5;background-color:rgba(232,232,232, 0.8);fill:rgba(232,232,232, 0.8);}#div .labelBkg{background-color:rgba(232, 232, 232, 0.5);}#div .node .cluster{fill:rgba(255, 255, 222, 0.5);stroke:rgba(170, 170, 51, 0.2);box-shadow:rgba(50, 50, 93, 0.25) 0px 13px 27px -5px,rgba(0, 0, 0, 0.3) 0px 8px 16px -8px;stroke-width:1px;}#div .cluster text{fill:#333;}#div .cluster span,#div p{color:#333;}#div div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:"trebuchet ms",verdana,arial,sans-serif;font-size:12px;background:hsl(80, 100%, 96.2745098039%);border:1px solid #aaaa33;border-radius:2px;pointer-events:none;z-index:100;}#div .flowchartTitleText{text-anchor:middle;font-size:18px;fill:#333;}#div :root{--mermaid-font-family:"trebuchet ms",verdana,arial,sans-serif;}#div .BT&gt;*{stroke:transparent!important;fill:transparent!important;}#div .BT span{stroke:transparent!important;fill:transparent!important;}</style><g></g><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="6" viewBox="0 0 10 10" class="marker block" id="div_block-pointEnd"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 0 L 10 5 L 0 10 z"></path></marker><marker orient="auto" markerHeight="12" markerWidth="12" markerUnits="userSpaceOnUse" refY="5" refX="4.5" viewBox="0 0 10 10" class="marker block" id="div_block-pointStart"><path style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 0 5 L 10 10 L 10 0 z"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="11" viewBox="0 0 10 10" class="marker block" id="div_block-circleEnd"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5" refX="-1" viewBox="0 0 10 10" class="marker block" id="div_block-circleStart"><circle style="stroke-width: 1; stroke-dasharray: 1, 0;" class="arrowMarkerPath" r="5" cy="5" cx="5"></circle></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="12" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossEnd"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><marker orient="auto" markerHeight="11" markerWidth="11" markerUnits="userSpaceOnUse" refY="5.2" refX="-1" viewBox="0 0 11 11" class="marker cross block" id="div_block-crossStart"><path style="stroke-width: 2; stroke-dasharray: 1, 0;" class="arrowMarkerPath" d="M 1,1 l 9,9 M 10,1 l -9,9"></path></marker><g class="block"><g transform="translate(204.0859375, 0)" id="id-s6q7n5agnj9-49" class="node default default flowchart-label"><rect height="76" width="408.171875" y="-38" x="-204.0859375" ry="0" rx="0" style="" class="basic cluster composite label-container"></rect><g transform="translate(0, 0)" style="" class="label"><rect></rect><foreignObject height="0" width="0"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel"></span></div></foreignObject></g></g><g transform="translate(70.6953125, -17)" id="uct1" class="node default BT flowchart-label"><rect height="26" width="125.390625" y="-13" x="-62.6953125" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-39.1328125, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="78.265625"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">MessageId</span></div></foreignObject></g></g><g transform="translate(70.6953125, 17)" id="t" class="node default default flowchart-label"><rect height="26" width="125.390625" y="-13" x="-62.6953125" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-58.6953125, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="117.390625"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">48 bit timestamp</span></div></foreignObject></g></g><g transform="translate(204.0859375, 17)" id="b" class="node default default flowchart-label"><rect height="26" width="125.390625" y="-13" x="-62.6953125" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-46.25, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="92.5"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">2 special bits</span></div></foreignObject></g></g><g transform="translate(337.4765625, 17)" id="c" class="node default default flowchart-label"><rect height="26" width="125.390625" y="-13" x="-62.6953125" ry="5" rx="5" style="" class="basic label-container"></rect><g transform="translate(-52.921875, -9)" style="" class="label"><rect></rect><foreignObject height="18" width="105.84375"><div style="display: inline-block; white-space: nowrap;" xmlns="http://www.w3.org/1999/xhtml"><span class="nodeLabel">78 bits random</span></div></foreignObject></g></g></g></svg>

_MessageId layout_

The message id consists of 3 parts:
* A 48 bit timestamp (with millisecond precision and a range of more than 10000 years)
* 2 "special" bits used to provide 16384 'successor' and 'predecessor' values for each original value.
  For newly generated message ids, these two bits always have the value `01`or `10`.
  This ensures that there is always room to create new Message-id values before and after
  any other id, and that these ids will have the same timestamp as the original.
  These can be used to generate a message id that occurs "immediately before" some other message.
* A 78 bit random part

With 16 bytes of entropy, accidental collisions between MessageId instances are astronomically
unlikely.

In each Noatun database, the set of messages that are not the parent of any other message, is known
as the "update heads" set. Knowing the message-id of every message in the update heads set is enough
to know the complete state of the database.

Every message lists as its parents, the set of update-heads when the message was created.
The newly added message then becomes the new update-heads (which will thus then have only a single entry).
If only a single noatun instance exists, there is only ever one update head, and all messages become linked
in a single long linked list.

With more than one node, the messages and their parents form a DAG (directed acyclic graph). It is
a Noatun-invariant that a message is never stored in a Noatun database unless all the parents of the message
also are, with one caveat (see further below).

The upshot of all this is that knowing the set of update-heads of a Noatun database is enough to
know the entire database state (with one caveat, which we'll get to).

This allows Noatun to easily detect if two nodes are in sync or not.

However, Noatun has the concept of "cutoff_time". See


### Pruning

When a message is deemed to no longer have any possible effect on the database state, it is pruned.
Its children will have the message removed from their parent lists. This means that, at any given time,
the same message (by message id) may have different parents on different nodes. However, it is always
the case that such a pruned message will be pruned on every node, eventually.

## Data storage

Noatun stores data on disk by memory-mapping several files:

### Message store files
Messages are stored in files `data0.bin` and `data1.bin`. One of these is always active, and the other passive.
All new writes occur in the active file. The other is slowly being copied over to the active one, a little bit with
every write to the active file. This means that the passive file eventually becomes empty, at which point the
files switch purpose (active becoming passive, and vice versa). When messages are deleted, they're just marked
as deleted and no compaction occurs. However, naturally, deleted files are never migrated from active to passive,
so over time the on-disk structure remains compact.

### Update heads-file

The file `update_heads.bin` contains the current list of messages without parents, that exist after the cutoff
time. Messages with timestamps before the cutoff time are never in the update heads file. Update heads
are used to quickly compare the state of two nodes. If the update heads are the same, and the cutoff hash is the same,
two nodes are synchronized.

### Index file

The file `index.bin` contains a single linear sorted index of all messages. Each entry contains information on which data-file
the messsage is in (data0 or data1), as well as offset and size. Since the index is sorted, and memory mapped
into the process, searching for a specific message by id is very fast.

### Main database file

The file `maindb.bin` contains the materialized view.

### Undo file

The file `undo.bin` contains undo information. This information allows us to "rewind time" in the main database file.
This is used to effectively implement reception of messages out-of-order.


## Memory allocation and re-use of memory

All Noatun files grow serve new allocations by growing the file.

For the main database file, whenever Noatun needs to allocate memory for the root object, a NoatunBox, or any of the 
collection types, memory is simply allocated at the end. At time of writing, Noatun never reuses memory, even when 
doing so would be possible. For example, when a vector is grown and needs to be reallocated, this leaves behind
an unused memory block that could potentially be reused. 

When the database file is grown, it is extended by all-zeros. Noatun guarantees that new allocated memory is always 
all-zero. Since memory is never reused, user implemented data-types don't need to worry about zeroing memory
after it is no longer needed.

## Tracking writes

As described in earlier chapters, Noatun keeps track of what information each NoatunMessage updated.

It does this by maintaining a vector of "write counts" for each Message in the database. It also keeps track,
for each piece of data in the materialized view, what message wrote that piece of data. This piece of
tracking information is known as a "Tracker" (since it registers who wrote to the particular data). Each tracker 
is 32 bits; simply the ordinal number of the Message that updated it.

Whenever a message writes data, its write counter is incremented. Whenever a message overwrites tracked data
previously written by another message(identified by a Tracker), that other message's write counter is decremented.

When a message write counter reaches 0, the message is added to a list of tentatively 'unused' messages.

Unused messages can be pruned, if either: 
 * They haven't been transmitted to another node
 * They only wrote opaque data
 * All their overwriters are timestamped before the cutoff time.

Note, there is one caveat to the above. Messages that deleted individual items from collections are
marked as "tombstone" messages. Tombstone messages can only be deleted once the cutoff time advances
past all their overwriters.

## Tracking reads

Noatun also tracks reads. For each message, a list of readers is maintained. This allows
maintaining a dependency graph for read-dependencies between all messages in the database.

# Communication

Noatun contains built-in support for communication over UDP Multicast. However, the central communication
logic is completely independent of the chosen network technology. Noatun poses very few requirements on 
the underlying network. Specifically:
 * The network can have high latency (multiple seconds is ok)
 * The network can be lossy (though performance will suffer if packet loss is frequent) 
   Noatun does not do forward error correction. The network is expected to provide this, if needed. 
 * The network capacity can be low. Noatun has a configurable max bandwidth cap that it will respect,
   to avoid overloading the network. 
 * The network MTU can be small (down to ~200 bytes is ok)
 * The network doesn't need to have the concept of addresses
 * If it has addresses, not all addresses need be globally unique (this can be useful in a very decentralized system,
   where reliably guaranteeing uniqueness of ip-addresses for all nodes may be hard).
 * The nodes don't need to know their own address

The only requirement of addresses is that if the network has addresses, nodes may not change addresses too frequently.
Note that on linux you may need to disable `rp_filter`, if there are address duplicates in your network. 







