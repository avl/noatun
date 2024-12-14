use std::borrow::Borrow;
use anyhow::Result;
use arcshift::ArcShift;
use datetime_literal::datetime;
use eframe::epaint::{EllipseShape, FontId};
use eframe::{App, Frame};
use egui::epaint::{RectShape, TextShape};
use egui::scroll_area::ScrollBarVisibility;
use egui::{Button, Color32, Context, CornerRadius, FontFamily, Pos2, Rect, RichText, Sense, Shape, Stroke, StrokeKind, TextEdit, Vec2, Widget};
use noatun::{noatun_object, CutOffDuration, Database, Message, MessageFrame, MessageId, NoatunCell, NoatunStorable, Object, Savefile, SavefileMessageSerializer, SchemaHasher};
use std::default::Default;
use std::fmt::Debug;
use std::hash::Hasher;
use std::pin::Pin;
use std::time::{Duration, Instant};

use noatun::data_types::{NoatunHashMap, NoatunKey};
use noatun::database::DatabaseSettings;
use noatun::distributor::{Address, Distributor, DistributorMessage, EphemeralNodeId, SerializedMessage};

const RANGE: f32 = 0.5;


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
unsafe impl NoatunStorable for Rgb {
    fn hash_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("Rgb")
    }
}

impl NoatunKey for Rgb {
    type NativeType = Rgb;
    type NativeOwnedType = Rgb;

    fn hash<H>(tself: &Self::NativeType, state: &mut H)
    where
        H: Hasher
    {
        use std::hash::Hash;
        (*tself).hash(state);
    }

    fn destroy(&mut self) {

    }

    fn export_key_ref(&self) -> impl Borrow<Self::NativeType> {
        self
    }

    fn export_key(&self) -> Self::NativeOwnedType {
        *self
    }

    fn eq(a: &Self::NativeType, b: &Self::NativeType) -> bool {
        a == b
    }

    fn init_from(mut self: Pin<&mut Self>, external: &Self::NativeType) {
        self.set(*external);
    }

    fn hash_key_schema(hasher: &mut SchemaHasher) {
        hasher.write_str("Rgb")
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
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        match self {
            KeyUpdateMessage::Set(key, val) => {
                root.key_values.insert(key, val);
            }
            KeyUpdateMessage::Change(key, msg_val) => {
                if let Some(val) = root.key_values.as_mut().get_mut_val(key) {
                    let prev = val.get();
                    let new = prev.saturating_add(*msg_val);
                    val.set(new);
                } else {
                    root.key_values.insert(key, msg_val);
                }
            }
        }
    }

}


struct InflightPacket {
    from: u8,
    to: u8,
    start_time: Instant,
    arrive_time: Instant,
    end_time: Instant,
    data: DistributorMessage,
    arrived: bool,
    packet_number: usize,
    decoded: Option<MessageFrame<KeyUpdateMessage>>,
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
            DistributorMessage::Message { message, .. } => {
                match message.to_message_from_ref::<KeyUpdateMessage>().unwrap().payload {
                    KeyUpdateMessage::Set(rgb, _) => {(rgb).into()}
                    KeyUpdateMessage::Change(rgb, _) => {(rgb).into()}
                }
            }
        }
    }
}

#[derive(Debug)]
struct Node {
    whoami: u8,
    db: Database<KeyUpdateMessage>,
    distributor: Distributor,
    last_periodic: Instant,
}

impl Node {
    pub fn add_local(&mut self, msg: KeyUpdateMessage) {
        let header = self.db.begin_session_mut()
            .unwrap().append_local(msg.clone()).unwrap();
        self.distributor.outbuf.push_back(DistributorMessage::Message {
            source: *self.distributor.ephemeral_node_id.get(),
            message: SerializedMessage::from_header_and_body(header, msg).unwrap(),
            demand_ack: false,
            origin: *self.distributor.ephemeral_node_id.get(),
            explicit_retransmit: false,
        });
    }
    pub fn new(id: u8, ephemeral_node_id: Option<EphemeralNodeId>, now: Instant,) -> Node {

        let db = Database::create_in_memory(1_000_000, DatabaseSettings {
            mock_time: Some(datetime!(2020-01-01 T 00:00:00 Z).into()),
            projection_time_limit: None,
            auto_prune: false,
            max_file_size: 100_000_000,
            cutoff_interval: CutOffDuration::from_minutes(5),
            ..Default::default()
        }).unwrap();

        Node {
            whoami: id,
            db,
            distributor: Distributor::new(Duration::from_secs(5), ArcShift::new(ephemeral_node_id.unwrap_or(
                EphemeralNodeId::new(id.into())
            )), now.into(), None),
            last_periodic: now,
        }
    }
    fn receive_message(&mut self, message: DistributorMessage, src: u8, now: Instant, _actual_ether: &mut ActualEther) {
        self.distributor.receive_message(&mut self.db, std::iter::once((Some(Address::from(src)), EphemeralNodeId::new(src as u16), message)), now.into()).unwrap();

    }
    fn step(&mut self, now: Instant, actual_ether: &mut ActualEther) {
        if now >= self.last_periodic + self.distributor.periodic_message_interval() {
            let session = self.db.begin_session().unwrap();
            let msgs = self.distributor.get_periodic_message(&session, now.into()).unwrap();
            for msg in msgs {
                actual_ether.do_send(self.whoami, msg, now).unwrap();
            }
            self.last_periodic = now;
        }

        while !self.distributor.outbuf.is_empty() {
            let msg = self.distributor.outbuf.pop_front().unwrap();
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
                let dist = (dest.pos - own_node.pos).length();
                let dist_time = dist / velocity;
                let end_time = RANGE / velocity; 

                let mut user_message = None;
                if let DistributorMessage::Message {  message, .. } = &data {
                    user_message = Some(message.to_message_from_ref().unwrap());
                }

                let ifp = InflightPacket {
                    from: src,
                    to: index as u8,
                    start_time: now,
                    arrive_time: now + Duration::from_secs_f32(dist_time),
                    end_time: now + Duration::from_secs_f32(end_time),
                    data: data.clone(),
                    arrived: false,
                    packet_number: self.next_packet_number,
                    decoded: user_message,
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
                if !x.arrived {
                    if x.arrive_time <= self.now && result.is_none() {
                        x.arrived = true;
                        result = Some((x.data.clone(), x.from, x.to));
                    }
                }
                self.now < x.end_time
            });
        }
        result
    }
}

struct NodeMetaData {
    #[allow(unused)]
    whoami: u8,
    ephemeral_node_id: ArcShift<EphemeralNodeId>,
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
    pub fn add_node(&mut self, pos: Vec2, id: Option<u16>) {
        let myid = self.actual_ether.node_metadata.len().try_into().unwrap();
        let node = Node::new(
            myid,
            id.map(EphemeralNodeId::new), self.now);
        self.actual_ether.node_metadata.push(NodeMetaData {
            whoami: node.whoami,
            ephemeral_node_id: node.distributor.ephemeral_node_id.clone(),
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

    drag: Option<(usize, Pos2/*drag start pos*/, Vec2/*node start pos*/)>,

    temp_col: Rgb,
    temp_set: f64,
    temp_inc: f64,
    paused: bool,
    new_node_id: String,
}

impl Default for Visualizer {
    fn default() -> Self {
        Self {
            ether: Default::default(),
            last_update: Instant::now(),
            selected: Selected::Node(0),
            temp_col: Rgb::GREEN,
            temp_set: 0.0,
            temp_inc: 1.0,
            paused: false,
            drag: None,
            new_node_id: String::new(),
        }
    }
}

impl App for Visualizer {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ctx.set_pixels_per_point(2.0);

            ui.horizontal(|ui| {

                if ui.button(if self.paused {"Play"} else {"Stop"}).clicked() {
                    self.paused= !self.paused;
                    if !self.paused {
                        self.last_update = Instant::now();
                    }
                }



                ui.label("Time:");
                let mut t = format!("{:?}", self.ether.elapsed());
                use egui::Widget;
                TextEdit::singleline(&mut t).interactive(false).ui(ui);

                if ui.button("New Node").clicked() {
                    let id = self.new_node_id.trim().parse::<_>().ok();
                    self.ether.add_node(Vec2::new(0.5,0.5), id);
                }

                ui.text_edit_singleline(&mut self.new_node_id);

            });

            let width_available = (ui.available_width()-300.0).max(50.0);
            let height_available = ui.available_height();
            let square_available = width_available.min(ui.available_height() ) - 2.0*ui.spacing().item_spacing.y;

            ui.horizontal(|ui| {
                let (id, allocated_rect) = ui.allocate_space(Vec2::new(width_available, height_available));
                let canvas_rect = Rect::from_min_max(
                    allocated_rect.min, allocated_rect.min+Vec2::new(square_available, square_available)
                );



                let mut shapes = vec![];

                let response = ui.interact(canvas_rect, id, Sense::drag());


                let scale = Vec2::new(canvas_rect.width(), canvas_rect.height());

                /*let to_logical = |screen_pos: Pos2| -> Pos2 {
                    Pos2::new(
                        (screen_pos.x - canvas_rect.min.x)/scale.x,
                        (screen_pos.y - canvas_rect.min.y)/scale.y
                    )
                };*/
                let to_pixels = |logical: Vec2| -> Pos2 {
                    Pos2::new(
                        scale.x*logical.x + canvas_rect.min.x,
                        scale.y*logical.y + canvas_rect.min.y
                    )
                };

                if response.drag_stopped() {
                    self.drag = None;
                } else if let Some((dragged_node, last_pos, last_node_pos)) = self.drag {
                    if let Some(pos) = response.interact_pointer_pos() {
                        let delta = (pos - last_pos)/scale;
                        let mut new_pos = last_node_pos + delta;
                        new_pos.x = new_pos.x.clamp(0.0,1.0);
                        new_pos.y = new_pos.y.clamp(0.0,1.0);
                        self.ether.actual_ether.node_metadata[dragged_node].pos = new_pos;
                    }
                }

                for (node_index, (node, node_meta)) in self.ether.nodes.iter().zip(self.ether.actual_ether.node_metadata.iter()).enumerate() {

                    let pixel_pos = to_pixels(node_meta.pos);
                    let pixel_pos1 = to_pixels(node_meta.pos-Vec2::new(0.025,0.025));
                    let pixel_pos2 = to_pixels(node_meta.pos+Vec2::new(0.025,0.025));

                    let rect = Rect {
                        min: pixel_pos1,
                        max: pixel_pos2
                    };
                    if let Some(clicked) = response.interact_pointer_pos(){
                        if rect.contains(clicked) && response.drag_started() {
                            self.selected = Selected::Node(node_index);
                        }
                    }
                    if let Some(pos) = response.interact_pointer_pos() {
                        if rect.contains(pos) {
                            if response.drag_started() {
                                self.drag = Some((node_index, pos, node_meta.pos));
                            }
                        }
                    }

                    shapes.push(Shape::Rect(RectShape {
                        rect,
                        corner_radius: CornerRadius::from(4.0),
                        fill: Color32::DARK_GRAY,
                        stroke: Stroke::new(1.0, if Selected::Node(node_index) == self.selected {Color32::WHITE} else {Color32::GRAY}),
                        stroke_kind: StrokeKind::Inside,
                        round_to_pixels: None,
                        blur_width: 0.0,
                        brush: None,
                    }));


                    shapes.push(Shape::Ellipse(EllipseShape {
                        center: pixel_pos,
                        radius: RANGE*scale,
                        fill: Color32::TRANSPARENT,
                        stroke: Stroke::new(2.0, Color32::from_rgba_unmultiplied(100,100,255,128)),
                    }));

                    ui.fonts(|fonts|{

                        let session = node.db.begin_session().unwrap();
                        let (r,g,b) = session.with_root(|root|{
                            (root.key_values.get(&Rgb::RED).map(|x|x.export()).unwrap_or(0),
                             root.key_values.get(&Rgb::GREEN).map(|x|x.export()).unwrap_or(0),
                             root.key_values.get(&Rgb::BLUE).map(|x|x.export()).unwrap_or(0))
                        });
                        let mut pixel_pos = pixel_pos1;
                        for (value, color) in [
                            (r, Rgb::RED.into()),
                            (g, Rgb::GREEN.into()),
                            (b, Rgb::BLUE.into())
                        ] {
                            let galley = fonts.layout_no_wrap(value.to_string(), FontId::new(10.0, FontFamily::Proportional), color);
                            let stride = galley.rect.width()*1.1;
                            shapes.push(Shape::Text(TextShape::new(
                                pixel_pos,galley,color
                            )));
                            pixel_pos.x += stride;
                        }
                    });


                }

                for packet in self.ether.actual_ether.packets.iter() {
                    let from = &self.ether.actual_ether.node_metadata[packet.from as usize].pos;
                    let to = &self.ether.actual_ether.node_metadata[packet.to as usize].pos;
                    let journey_time = (packet.arrive_time - packet.start_time).as_secs_f32();
                    let elapsed = (self.ether.now - packet.start_time).as_secs_f32();
                    let along = elapsed/journey_time;

                    let pos = *from + (*to-*from)*along;


                    let pixel_pos1 = to_pixels(pos-Vec2::new(0.015,0.015));
                    let pixel_pos2 = to_pixels(pos+Vec2::new(0.015,0.015));

                    let rect = Rect {
                        min: pixel_pos1,
                        max: pixel_pos2,
                    };
                    if let Some(clicked) = response.interact_pointer_pos() {
                        if rect.contains(clicked) && response.drag_started() {
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

                    ui.fonts(|fonts|{
                        match &packet.decoded {
                            Some(msg) => {
                                let (KeyUpdateMessage::Set(rgb, val) |
                                    KeyUpdateMessage::Change(rgb, val)) = &msg.payload;

                                let galley = fonts.layout_no_wrap(val.to_string(), FontId::new(10.0, FontFamily::Proportional), (*rgb).into());
                                shapes.push(Shape::Text(TextShape::new(
                                    pixel_pos1,galley,(*rgb).into()
                                )));

                            }
                            _ =>  {}
                        }
                    })


                }

                if !self.ether.actual_ether.packets.is_empty() {
                    ui.ctx().request_repaint();
                }  else {
                    ui.ctx().request_repaint_after(Duration::from_millis(50));
                }
                let mut thin = vec![];
                if let Selected::Node(node_index) = self.selected {
                    let node = &mut self.ether.nodes[node_index];
                    //assert_eq!(node_index, node.distributor.ephemeral_node_id.shared_get().raw_u16() as usize);
                    let node_pos = self.ether.actual_ether.node_metadata[node_index].pos;
                    for (a_peer_id, _a_peer_info) in &node.distributor.neighborhood.peers {
                        for (b_peer_id, _b_peer_info) in &node.distributor.neighborhood.peers {
                            

                            let Some(origin_pos) = self.ether.actual_ether.node_metadata.iter().find(|x|*x.ephemeral_node_id.shared_get() == *a_peer_id).map(|x|x.pos) else {
                                continue;
                            };
                            let Some(recv_from_pos) = self.ether.actual_ether.node_metadata.iter().find(|x|*x.ephemeral_node_id.shared_get() == *b_peer_id).map(|x|x.pos) else {
                                continue;
                            };
                            if !node.distributor.neighborhood.fast_pather.write().unwrap().should_i_forward(a_peer_id.raw_u16(), b_peer_id.raw_u16()) {
                                continue;
                            }

                            thin.push(Shape::LineSegment {
                                points: [
                                    to_pixels(origin_pos),
                                    to_pixels(recv_from_pos),
                                ],
                                stroke: Stroke::new(1.0, Color32::from_rgb(100,100,155)),
                            });
                            shapes.push(Shape::LineSegment {
                                points: [
                                    to_pixels(recv_from_pos),
                                    to_pixels(node_pos),
                                ],
                                stroke: Stroke::new(3.0, Color32::from_rgb(200,255,200)),
                            });

                        }


                    }
                }

                shapes.extend(thin);
                ui.painter().with_clip_rect(canvas_rect).extend(shapes);


                match self.selected {
                    Selected::Node(selected_node) => {
                        ui.vertical(|ui| {

                            egui::Grid::new("node_properties").show(ui, |ui| {
                                ui.label("Node:");
                                let mut t = self.ether.nodes[selected_node].distributor.ephemeral_node_id.get().to_string();
                                TextEdit::singleline(&mut t).interactive(false).ui(ui);
                                ui.end_row();
                                if ui.button("debug").clicked() {
                                    let node = &self.ether.nodes[selected_node].distributor;
                                    println!("Distributor for node {}:\n{:#?}", selected_node, node);
                                    println!("Messages:\n{:#?}", self.ether.nodes[selected_node].db.begin_session().unwrap().get_all_messages_vec());
                                }
                                ui.end_row();


                                ui.horizontal(|ui|{
                                    let sess = self.ether.nodes[selected_node].db.begin_session().unwrap();
                                    let root = sess.with_root(|root|{
                                        root.export()
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

                                    self.ether.nodes[selected_node].add_local(KeyUpdateMessage::Change(self.temp_col, self.temp_inc.round() as i32));

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
                                let t= format!("{:#?}", packet.data);
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

    visualizer.ether.add_node(Vec2::new(0.2,0.2), None);
    visualizer.ether.add_node(Vec2::new(0.5,0.5), None);
    visualizer.ether.add_node(Vec2::new(0.8,0.8), None);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1500.0, 900.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Visualizer",
        options,
        Box::new(|_cc| {
            Ok(Box::new(visualizer))
        }),
    ).unwrap()


}