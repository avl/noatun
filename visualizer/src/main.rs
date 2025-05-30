use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::hash::Hasher;
use std::io::{Cursor, Read, Write};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use eframe::{App, Frame};
use egui::{vec2, Button, Color32, Context, CornerRadius, Pos2, Rect, RichText, Sense, Shape, Stroke, StrokeKind, TextBuffer, TextEdit, Vec2, Widget};
use egui::epaint::RectShape;
use noatun::{msg_deserialize, msg_serialize, noatun_object, Application, CutOffDuration, Database, Message, MessageFrame, MessageId, NoatunCell, NoatunStorable, NoatunTime, Object, Savefile};
use anyhow::Result;
use arcshift::ArcShift;
use bytes::{Buf, BufMut};
use datetime_literal::datetime;
use egui::scroll_area::ScrollBarVisibility;
use savefile::{Deserializer, LittleEndian};
use savefile::prelude::ReadBytesExt;

use noatun::data_types::{NoatunHashMap, NoatunKey, NoatunString};
use noatun::database::DatabaseSettings;
use noatun::distributor::{Address, Distributor, DistributorMessage, EphemeralNodeId, PeerSummaryInfo, SerializedMessage};



#[derive(PartialEq, Eq, Hash, Clone, Copy, Savefile, Debug)]
#[repr(u8)]
pub enum Rgb {
    RED,
    GREEN,
    BLUE
}

impl Rgb {
    fn color(&self) -> Color32 {
        (*self).into()
    }
}

impl From<Rgb> for Color32 {
    fn from(value: Rgb) -> Self {
        match value {
            Rgb::RED => Color32::from_rgb(255,170,150),
            Rgb::GREEN => Color32::from_rgb(120,255,190),
            Rgb::BLUE => Color32::from_rgb(140,160,255),
        }
    }
}

/// SAFETY:
/// This is safe only because we never persist anything, so there's no
/// risk of loading a file with invalid values.
unsafe impl NoatunStorable for Rgb {}

impl NoatunKey for Rgb {
    type DetachedType = Rgb;
    type DetachedOwnedType = Rgb;

    fn hash<H>(tself: &Self::DetachedType, state: &mut H)
    where
        H: Hasher
    {
        use std::hash::Hash;
        (*tself).hash(state);
    }

    fn detach_key_ref(&self) -> &Self::DetachedType {
        self
    }

    fn detach_key(&self) -> Self::DetachedOwnedType {
        *self
    }

    fn eq(a: &Self::DetachedType, b: &Self::DetachedType) -> bool {
        a == b
    }

    fn init_from_detached(mut self: Pin<&mut Self>, detached: &Self::DetachedType) {
        self.set(*detached);
    }
}

#[derive(Debug, Savefile, Clone)]
pub enum KeyUpdateMessage {
    Set(Rgb, i32),
    Change(Rgb, i32)
}

noatun_object!{
    struct Document {
        object key_values: NoatunHashMap<Rgb, NoatunCell<i32>>
    }
}

impl Message for KeyUpdateMessage {
    type Root = Document;

    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        match self {
            KeyUpdateMessage::Set(key, val) => {
                root.key_values.insert(key, val);
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
    packet_number: usize,
}

#[derive(Debug)]
struct DecodedPacket {
    distributor_message: DistributorMessage,
    user_message: Option<MessageFrame<KeyUpdateMessage>>,
}

impl InflightPacket {
    pub fn color(&self) -> Color32 {
        match &self.data {
            DistributorMessage::ReportHeads { .. } => Color32::GRAY,
            DistributorMessage::SyncAllQuery(_) => Color32::GRAY,
            DistributorMessage::SyncAllRequest(_) => Color32::GRAY,
            DistributorMessage::SyncAllAck(_) => Color32::GRAY,
            DistributorMessage::RequestUpstream { .. } => Color32::GRAY,
            DistributorMessage::UpstreamResponse { .. } => Color32::GRAY,
            DistributorMessage::SendMessageAndAllDescendants { .. } => Color32::GRAY,
            DistributorMessage::Message { source, message, demand_ack, origin } => {
                match message.to_message_from_ref::<KeyUpdateMessage>().unwrap().payload {
                    KeyUpdateMessage::Set(rgb, _) => {(rgb).into()}
                    KeyUpdateMessage::Change(rgb, _) => {(rgb).into()}
                }
            }
            DistributorMessage::Forwarding { .. } => {Color32::GRAY}
        }
    }
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
    pub fn add_local(&mut self, msg: KeyUpdateMessage) {
        let header = self.db.begin_session_mut()
            .unwrap().append_local(msg.clone()).unwrap();
        self.comm.outbuf.push_back(DistributorMessage::Message {
            source: EphemeralNodeId::new(self.whoami as u16),
            message: SerializedMessage::from_header_and_body(header, msg).unwrap(),
            demand_ack: false,
            origin: EphemeralNodeId::new(self.whoami as u16),
        });
    }
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
    fn receive_message(&mut self, message: DistributorMessage, src: u8, now: Instant, actual_ether: &mut ActualEther) {
        self.comm.receive_message(&mut self.db, std::iter::once((Address::from(src), message)), now).unwrap();

    }
    fn step(&mut self, now: Instant, actual_ether: &mut ActualEther) {
        if now >= self.last_periodic + self.comm.periodic_message_interval() {
            let session = self.db.begin_session().unwrap();
            let msgs = self.comm.get_periodic_message(&session).unwrap();
            for msg in msgs {
                actual_ether.do_send(self.whoami, msg, now).unwrap();
            }
            self.last_periodic = now;
        }

        while !self.comm.outbuf.is_empty() {
            let msg = self.comm.outbuf.pop_front().unwrap();
            actual_ether.do_send(self.whoami, msg, now).unwrap();
        }

    }
}


impl ActualEther {

    fn do_send(&mut self, src: u8, data: DistributorMessage, now: Instant) -> std::io::Result<()> {

        let own_node = &self.node_metadata[src as usize];
        for (index, dest) in self.node_metadata.iter().enumerate() {
            if index as u8 != src {
                let velocity = 0.4;
                let dist_time = (dest.pos - own_node.pos).length() / velocity;

                let mut ifp = InflightPacket {
                    from: src,
                    to: index as u8,
                    start_time: now,
                    velocity,
                    arrive_time: now + Duration::from_secs_f32(dist_time),
                    data: data.clone(),
                    arrived: false,
                    packet_number: self.next_packet_number,
                };
                self.next_packet_number += 1;
                self.packets.push(ifp);
            }
        }
        Ok(())
    }
}
impl Ether {
    fn step(&mut self, step_time: Duration) {

        while let Some((msg, from, to)) = self.recv() {
            self.nodes[to as usize].receive_message(msg, from, self.now, &mut self.actual_ether);
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
                    if x.arrive_time <= self.now && result.is_none() {
                        x.arrived = true;
                        result = Some((x.data.clone(), x.from, x.to));
                    }
                    true
                } else {
                    (self.now.saturating_duration_since(x.arrive_time).as_secs() <= 5)
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
    next_packet_number: usize,
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

#[derive(PartialEq,Clone,Copy)]
enum Selected {
    Node(usize),
    Packet(usize)
}

struct Visualizer {
    ether: Ether,
    last_update: Instant,
    selected: Selected,

    temp_col: Rgb,
    temp_set: f64,
    temp_inc: f64,
    paused: bool
}

impl Default for Visualizer {
    fn default() -> Self {
        Self {
            ether: Default::default(),
            last_update: Instant::now(),
            selected: Selected::Node(0),
            temp_col: Rgb::GREEN,
            temp_set: 0.0,
            temp_inc: 0.0,
            paused: false,
        }
    }
}

impl App for Visualizer {
    fn update(&mut self, ctx: &Context, frame: &mut Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ctx.set_pixels_per_point(2.0);



            ui.horizontal(|ui| {

                if ui.button(if self.paused {"Play"} else {"Stop"}).clicked() {
                    self.paused= !self.paused;
                    if !self.paused {
                        self.last_update = Instant::now();;
                    }
                }

                ui.label("Time:");
                let mut t = format!("{:?}", self.ether.elapsed());
                use egui::Widget;
                TextEdit::singleline(&mut t).interactive(false).ui(ui);
            });

            let width_available = (ui.available_width()-300.0).max(50.0);
            let height_available = ui.available_height();
            let square_available = width_available.min(ui.available_height() ) - 2.0*ui.spacing().item_spacing.y;

            ui.horizontal(|ui| {
                let (id, mut allocated_rect) = ui.allocate_space(Vec2::new(width_available, height_available));
                let canvas_rect = Rect::from_min_max(
                    allocated_rect.min, allocated_rect.min+Vec2::new(square_available, square_available)
                );



                let mut shapes = vec![];

                let response = ui.interact(canvas_rect, id, Sense::click());
                let click = if response.clicked() { response.interact_pointer_pos() } else {None};

                for (node_index, (node, node_meta)) in self.ether.nodes.iter().zip(self.ether.actual_ether.node_metadata.iter()).enumerate() {

                    let mut pos_x1 = canvas_rect.width()* (node_meta.pos.x-0.025);
                    let mut pos_y1 = canvas_rect.height()*(node_meta.pos.y-0.025);
                    let mut pos_x2 = canvas_rect.width()* (node_meta.pos.x+0.025);
                    let mut pos_y2 = canvas_rect.height()*(node_meta.pos.y+0.025);

                    let mut rect = Rect {
                        min: canvas_rect.min + Vec2::new(pos_x1, pos_y1),
                        max: canvas_rect.min + Vec2::new(pos_x2, pos_y2),
                    };
                    if let Some(clicked) = click {
                        if rect.contains(clicked) {
                            self.selected = Selected::Node(node_index);
                        }
                    }

                    shapes.push(Shape::Rect(RectShape {
                        rect,
                        corner_radius: CornerRadius::from(5.0),
                        fill: if Selected::Node(node_index) == self.selected {Color32::LIGHT_GRAY} else {Color32::GRAY},
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
                    let elapsed = (self.ether.now - packet.start_time).as_secs_f32();
                    let along = elapsed/journey_time;

                    let pos = *from + (*to-*from)*along;

                    let mut pos_x1 = canvas_rect.width()* (pos.x-0.015);
                    let mut pos_y1 = canvas_rect.height()*(pos.y-0.015);
                    let mut pos_x2 = canvas_rect.width()* (pos.x+0.015);
                    let mut pos_y2 = canvas_rect.height()*(pos.y+0.015);



                    let mut rect = Rect {
                        min: canvas_rect.min + Vec2::new(pos_x1, pos_y1),
                        max: canvas_rect.min + Vec2::new(pos_x2, pos_y2),
                    };
                    if let Some(clicked) = click {
                        if rect.contains(clicked) {
                            self.selected = Selected::Packet(packet.packet_number);
                        }
                    }
                    shapes.push(Shape::Rect(RectShape {
                        rect,
                        corner_radius: CornerRadius::from(3.0),
                        fill: packet.color(),
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

                match self.selected {
                    Selected::Node(selected_node) => {
                        ui.vertical(|ui| {

                            egui::Grid::new("node_properties").show(ui, |ui| {
                                ui.label("Node:");
                                let mut t = "test".to_string();
                                TextEdit::singleline(&mut t).interactive(false).ui(ui);
                                ui.end_row();
                                ui.label("Some:");
                                let mut t = "thing".to_string();
                                TextEdit::singleline(&mut t).interactive(false).ui(ui);
                                ui.end_row();


                                ui.horizontal(|ui|{
                                    let sess = self.ether.nodes[selected_node].db.begin_session().unwrap();
                                    let root = sess.with_root(|root|{
                                        root.detach()
                                    });
                                    if Button::new(RichText::new(format!("Red:{}", root.key_values.get(&Rgb::RED).unwrap_or(&0))).color(Color32::BLACK)).fill(Rgb::RED.color()).ui(ui).clicked() {
                                        self.temp_col = Rgb::RED;
                                    }
                                    if Button::new(RichText::new(format!("Green:{}", root.key_values.get(&Rgb::GREEN).unwrap_or(&0))).color(Color32::BLACK)).fill(Rgb::GREEN.color()).ui(ui).clicked() {
                                        self.temp_col = Rgb::GREEN;
                                    }
                                    if Button::new(RichText::new(format!("Blue:{}", root.key_values.get(&Rgb::BLUE).unwrap_or(&0))).color(Color32::BLACK) ) .fill(Rgb::BLUE.color()).ui(ui).clicked() {
                                        self.temp_col = Rgb::BLUE;
                                    }
                                });

                                ui.end_row();

                                ui.add(egui::Slider::new(&mut self.temp_set, -5.0..=5.0).step_by(1.0).text("Set value"));
                                if ui.button("Set").clicked() {


                                    self.ether.nodes[selected_node].add_local(KeyUpdateMessage::Set(self.temp_col, self.temp_set.round() as i32));

                                }

                                ui.end_row();

                                ui.add(egui::Slider::new(&mut self.temp_inc, -5.0..=5.0).step_by(1.0).text("Change"));
                                if ui.button("Inc").clicked() {
                                    self.ether.nodes[selected_node].db.begin_session_mut()
                                        .unwrap().append_local(KeyUpdateMessage::Change(self.temp_col, self.temp_inc.round() as i32)).unwrap();
                                }

                                ui.end_row();


                            });

                            egui::ScrollArea::vertical().scroll_bar_visibility(ScrollBarVisibility::AlwaysVisible).show(ui, |ui| {
                                ui.vertical(|ui|
                                    {
                                        let node = &self.ether.nodes[selected_node];
                                        let session = node.db.begin_session().unwrap();
                                        for msg in session.get_all_messages().unwrap() {
                                            match &msg.payload {
                                                KeyUpdateMessage::Set(rgb, new) => {
                                                    ui.colored_label(*rgb, format!("= {}", new) );
                                                }
                                                KeyUpdateMessage::Change(rgb, inc) => {
                                                    ui.colored_label(*rgb, format!("+ {}", inc) );
                                                }
                                            }
                                        }

                                    });
                                // Add a lot of widgets here.
                            });

                        });

                    }
                    Selected::Packet(cur_packet) => {
                        if let Some(packet) = self.ether.actual_ether.packets.iter().find(|x|x.packet_number == cur_packet) {
                            ui.vertical(|ui| {
                                ui.horizontal(|ui| {
                                    ui.label("Packet:");
                                    let mut t = packet.packet_number.to_string();
                                    TextEdit::singleline(&mut t).interactive(false).ui(ui);
                                });
                                let mut t= format!("{:#?}", packet.data);
                                ui.label(&t);
                            });
                        }
                    }
                }

            });


            if !self.paused {
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(self.last_update);
                self.last_update = now;
                self.ether.step(elapsed);
            }

        });
    }
}

fn main() {

    let mut visualizer = Visualizer::default();

    visualizer.ether.add_node(Vec2::new(0.2,0.2));
    visualizer.ether.add_node(Vec2::new(0.7,0.1));
    visualizer.ether.add_node(Vec2::new(0.3,0.7));

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