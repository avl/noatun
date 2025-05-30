use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io::{Cursor, Read, Write};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use eframe::{App, Frame};
use egui::{vec2, Color32, Context, CornerRadius, Pos2, Rect, Shape, StrokeKind, TextBuffer, TextEdit, Vec2};
use egui::epaint::RectShape;
use noatun::{msg_deserialize, msg_serialize, noatun_object, Application, CutOffDuration, Database, Message, MessageFrame, MessageId, NoatunCell, NoatunTime, Savefile};
use anyhow::Result;
use arcshift::ArcShift;
use bytes::{Buf, BufMut};
use datetime_literal::datetime;
use savefile::{Deserializer, LittleEndian};
use savefile::prelude::ReadBytesExt;

use noatun::data_types::{NoatunHashMap, NoatunString};
use noatun::database::DatabaseSettings;
use noatun::distributor::{Address, Distributor, DistributorMessage, EphemeralNodeId, PeerSummaryInfo};

#[derive(Debug, Savefile)]
pub enum KeyUpdateMessage {
    Set(String, i32),
    Change(String, i32)
}

noatun_object!{
    struct Document {
        object key_values: NoatunHashMap<NoatunString, NoatunCell<i32>>
    }
}

impl Message for KeyUpdateMessage {
    type Root = Document;

    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        match self {
            KeyUpdateMessage::Set(key, val) => {
                root.key_values.insert(key.as_str(), val);
            }
            KeyUpdateMessage::Change(key, msg_val) => {
                if let Some(val) = root.key_values.get_mut_val(key) {
                    let prev = val.get();
                    let new = prev.saturating_add(*msg_val);
                    val.set(new);
                }
            }
        }
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        msg_serialize(self, writer)
    }
}

impl Application for Document {
    type Message = KeyUpdateMessage;
    type Params = ();
}

struct InflightPacket {
    from: u8,
    to: u8,
    start_time: Instant,
    velocity: f32,
    arrive_time: Instant,
    data: DistributorMessage,
    arrived: bool,
}

#[derive(Debug)]
struct DecodedPacket {
    distributor_message: DistributorMessage,
    user_message: Option<MessageFrame<KeyUpdateMessage>>,
}

impl InflightPacket {
    pub fn decode(&self) -> DecodedPacket {
        let mut user_message = None;
        if let DistributorMessage::Message { source, message, demand_ack, origin } = &self.data {
            user_message = Some(message.to_message_from_ref().unwrap());
        }
        DecodedPacket {
            distributor_message: self.data.clone(),
            user_message,
        }
    }
}

#[derive(Debug)]
struct Node {
    whoami: u8,
    db: Database<Document>,
    comm: Distributor,
    last_periodic: Instant,
}

impl Node {
    pub fn new(id: u8, now: Instant,) -> Node {

        let peer_info = ArcShift::new(PeerSummaryInfo::default());
        let db = Database::create_in_memory(1_000_000, DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 T 00:00:00 Z).into()),
            projection_time_limit: None,
            auto_delete: false,
            max_file_size: 100_000_000,
            cutoff_interval: CutOffDuration::from_minutes(5),
        }, ()).unwrap();

        Node {
            whoami: id,
            db,
            comm: Distributor::new(Duration::from_secs(5), ArcShift::new(EphemeralNodeId::new(id as u16)),peer_info, now),
            last_periodic: now,
        }
    }
    fn receive_message(&mut self, message: DistributorMessage, src: u8, now: Instant) {
        self.comm.receive_message(&mut self.db, std::iter::once((Address::from(src), message)), now).unwrap();
    }
    fn step(&mut self, now: Instant, actual_ether: &mut ActualEther) {
        if now >= self.last_periodic + self.comm.periodic_message_interval() {
            let session = self.db.begin_session().unwrap();
            let msgs = self.comm.get_periodic_message(&session).unwrap();
            println!("Node {} get periodic: {:?}", self.whoami, msgs);
            for msg in msgs {
                actual_ether.send_to(self.whoami, msg, now).unwrap();
            }
            self.last_periodic = now;
        }
    }
}


impl ActualEther {

    fn send_to(&mut self, src: u8, data: DistributorMessage, now: Instant) -> std::io::Result<()> {

        let own_node = &self.node_metadata[src as usize];
        for (index, dest) in self.node_metadata.iter().enumerate() {
            if index as u8 != src {
                //println!("{} sending to {}", src, index);
                let velocity = 1.0;
                let dist_time = (dest.pos - own_node.pos).length() / velocity;

                let mut ifp = InflightPacket {
                    from: src,
                    to: index as u8,
                    start_time: now,
                    velocity,
                    arrive_time: now + Duration::from_secs_f32(dist_time),
                    data: data.clone(),
                    arrived: false,
                };
                self.packets.push(ifp);
            }
        }
        Ok(())
    }
}
impl Ether {
    fn step(&mut self, step_time: Duration) {

        while let Some((msg, from, to)) = self.recv() {
            self.nodes[to as usize].receive_message(msg, from, self.now);
        }

        for node in &mut self.nodes {
            node.step(self.now, &mut self.actual_ether);
        }


        self.now += step_time;
    }

    fn recv(&mut self) -> Option<(DistributorMessage, /*from*/u8, /*to*/ u8)> {

        let mut result = None;
        {

            self.actual_ether.packets.retain_mut(|x|{
                let retained = if !x.arrived {
                    if x.arrive_time >= self.now && result.is_none() {
                        x.arrived = true;
                        result = Some((x.data.clone(), x.from, x.to));
                    }
                    true
                } else {
                    x.arrive_time.elapsed().as_secs() <= 5
                };
                retained
            });
        }
        return result;
    }
}

struct NodeMetaData {
    whoami: u8,
    pos: Vec2,

}

#[derive(Default)]
struct ActualEther {
    node_metadata: Vec<NodeMetaData>,
    packets: Vec<InflightPacket>,
}

struct Ether {
    actual_ether: ActualEther,
    nodes: Vec<Node>,
    start: Instant,
    now: Instant,
}
impl Default for Ether {
    fn default() -> Ether  {
        let now = Instant::now();
        Ether {
            actual_ether: Default::default(),
            nodes: vec![],
            start: now,
            now,
        }
    }
}
impl Ether {
    pub fn elapsed(&self) -> Duration {
        self.now.saturating_duration_since(self.start)
    }
    pub fn add_node(&mut self, pos: Vec2) {
        let node = Node::new(self.actual_ether.node_metadata.len().try_into().unwrap(),self.now);
        self.actual_ether.node_metadata.push(NodeMetaData {
            whoami: node.whoami,
            pos,
        });
        self.nodes.push(node);
    }
}

struct Visualizer {
    ether: Ether,
    last_update: Instant,
}

impl Default for Visualizer {
    fn default() -> Self {
        Self {
            ether: Default::default(),
            last_update: Instant::now(),
        }
    }
}

impl App for Visualizer {
    fn update(&mut self, ctx: &Context, frame: &mut Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ctx.set_pixels_per_point(2.0);

            ui.horizontal(|ui| {

                if ui.button("Play").clicked() {

                }
                if ui.button("Stop").clicked() {

                }

                ui.label("Time:");
                let mut t = format!("{:?}", self.ether.elapsed());
                use egui::Widget;
                TextEdit::singleline(&mut t).interactive(false).ui(ui);
            });


            let square_available = ui.available_width().min(ui.available_height() );


            let desired_size = vec2(
                square_available,
                square_available
            );
            let (id, canvas_rect) = ui.allocate_space(desired_size);

            let mut shapes = vec![];


            for (node, node_meta) in self.ether.nodes.iter().zip(self.ether.actual_ether.node_metadata.iter()) {

                let mut pos_x1 = (canvas_rect.width()* (node_meta.pos.x-0.025));
                let mut pos_y1 = (canvas_rect.height()*(node_meta.pos.y-0.025));
                let mut pos_x2 = (canvas_rect.width()* (node_meta.pos.x+0.025));
                let mut pos_y2 = (canvas_rect.height()*(node_meta.pos.y+0.025));

                let mut rect = Rect {
                    min: Pos2::new(pos_x1, pos_y1),
                    max: Pos2::new(pos_x2, pos_y2),
                };

                shapes.push(Shape::Rect(RectShape {
                    rect,
                    corner_radius: CornerRadius::from(5.0),
                    fill: Color32::LIGHT_RED,
                    stroke: Default::default(),
                    stroke_kind: StrokeKind::Inside,
                    round_to_pixels: None,
                    blur_width: 0.0,
                    brush: None,
                }));

            }

            for packet in self.ether.actual_ether.packets.iter() {
                let from = &self.ether.actual_ether.node_metadata[packet.from as usize].pos;
                let to = &self.ether.actual_ether.node_metadata[packet.to as usize].pos;
                let journey_time = (packet.arrive_time - packet.start_time).as_secs_f32();
                let elapsed = (Instant::now() - packet.start_time).as_secs_f32();
                let along = elapsed/journey_time;

                let pos = *from + (*to-*from)*along;

                let mut pos_x1 = (canvas_rect.width()* (pos.x-0.015));
                let mut pos_y1 = (canvas_rect.height()*(pos.y-0.015));
                let mut pos_x2 = (canvas_rect.width()* (pos.x+0.015));
                let mut pos_y2 = (canvas_rect.height()*(pos.y+0.015));

                let mut rect = Rect {
                    min: Pos2::new(pos_x1, pos_y1),
                    max: Pos2::new(pos_x2, pos_y2),
                };

                shapes.push(Shape::Rect(RectShape {
                    rect,
                    corner_radius: CornerRadius::from(3.0),
                    fill: Color32::LIGHT_BLUE,
                    stroke: Default::default(),
                    stroke_kind: StrokeKind::Inside,
                    round_to_pixels: None,
                    blur_width: 0.0,
                    brush: None,
                }));

            }

            if !self.ether.actual_ether.packets.is_empty() {
                ui.ctx().request_repaint();
            }  else {
                ui.ctx().request_repaint_after(Duration::from_millis(50));
            }

            ui.painter().with_clip_rect(canvas_rect).extend(shapes);

            let now = Instant::now();
            let elapsed = now.saturating_duration_since(self.last_update);
            self.last_update = now;
            self.ether.step(elapsed);

        });
    }
}

fn main() {

    let mut visualizer = Visualizer::default();

    visualizer.ether.add_node(Vec2::new(0.2,0.2));
    visualizer.ether.add_node(Vec2::new(0.7,0.1));
    visualizer.ether.add_node(Vec2::new(0.3,0.7));
    println!("Nodes: {:#?}", visualizer.ether.nodes);
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([800.0, 800.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Visualizer",
        options,
        Box::new(|cc| {
            Ok(Box::new(visualizer))
        }),
    ).unwrap()


}