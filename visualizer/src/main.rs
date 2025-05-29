use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use eframe::{App, Frame};
use egui::{vec2, Color32, Context, CornerRadius, Pos2, Rect, Shape, StrokeKind, TextBuffer, Vec2};
use egui::epaint::RectShape;
use noatun::{msg_deserialize, msg_serialize, noatun_object, Application, CutOffDuration, Database, Message, MessageFrame, MessageId, NoatunCell, NoatunTime, Savefile};
use anyhow::Result;
use bytes::BufMut;
use tokio::time::Instant;
use noatun::communication::{CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket, DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::data_types::{NoatunHashMap, NoatunString};
use noatun::database::DatabaseSettings;

#[derive(Debug, Savefile)]
pub enum KeyUpdate {
    Set(String, i32),
    Change(String, i32)
}

noatun_object!{
    struct Document {
        object key_values: NoatunHashMap<NoatunString, NoatunCell<i32>>
    }
}

impl Message for KeyUpdate {
    type Root = Document;

    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        match self {
            KeyUpdate::Set(key, val) => {
                root.key_values.insert(key.as_str(), val);
            }
            KeyUpdate::Change(key, msg_val) => {
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
    type Message = KeyUpdate;
    type Params = ();
}

struct InflightPacket {
    from: u8,
    to: u8,
    start_time: Instant,
    velocity: f32,
    arrive_time: Instant,
    data: Vec<u8>,
    arrived: bool,
}

struct Node {
    whoami: u8,
    pos: Vec2,
    comm: Option<DatabaseCommunication<Document>>
}

impl Node {
    pub fn new(id: u8, pos: Vec2) -> Node {

        Node {
            whoami: id,
            pos,
            comm: None,
        }
    }
    pub async fn start(whoami: u8, ether: Arc<Mutex<Ether>>) -> DatabaseCommunication<Document> {
        let mut driver = Driver {
            whoami,
            ether,
        };

        let mut db: Database<Document> = Database::create_in_memory(
            2_500_000,
            CutOffDuration::from_minutes(15),
            DatabaseSettings {
                projection_time_limit: None,
                ..DatabaseSettings::default()
            },
            (),
        )
            .unwrap();


        let mut config =  DatabaseCommunicationConfig {
            listen_address: "dummy".to_string(),
            multicast_address: "dummy".to_string(),
            mtu: 1500,
            bandwidth_limit_bytes_per_second: 1000,
            retransmit_interval_seconds: 1.0,
            retransmit_buffer_size_bytes: 1_000_000,
            debug_logger: Some(Box::new(move |ev| {
                //let mut log = log.lock().unwrap();
                //log.push(ev);
            })),
            periodic_message_interval: Duration::from_secs(5),
            initial_ephemeral_node_id: None,
            disable_retransmit: false,
        };

        let comm = DatabaseCommunication::async_tokio_new(
            &mut driver,
            db,
            config,
        )
            .await
            .unwrap();
        comm
    }
}

struct Sender {
    whoami: u8,
    ether: Arc<Mutex<Ether>>
}
struct Receiver {
    whoami: u8,
    ether: Arc<Mutex<Ether>>
}

impl CommunicationSendSocket<u8> for Sender {
    fn local_addr(&self) -> anyhow::Result<u8> {
        Ok(self.whoami)
    }

    async fn send_to(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut ether = self.ether.lock().unwrap();
        let mut ether = &mut *ether;

        let own_node = &ether.nodes[self.whoami as usize];
        for (index, dest) in ether.nodes.iter().enumerate() {
            if index as u8 != self.whoami {
                println!("{} sending to {}", self.whoami, index);
                let velocity = 1.0;
                let dist_time = (dest.pos - own_node.pos).length() / velocity;
                let now = Instant::now();
                ether.packets.push(InflightPacket {
                    from: self.whoami,
                    to: index as u8,
                    start_time: now,
                    velocity,
                    arrive_time: now + Duration::from_secs_f32(dist_time),
                    data: buf.to_vec(),
                    arrived: false,
                });
            }
        }
        Ok(())
    }
}

impl CommunicationReceiveSocket<u8> for Receiver{
    async fn recv_buf_from<B: BufMut + Send>(&mut self, buf: &mut B) -> std::io::Result<(usize, u8)> {

        loop {

            compile_error!("Add observability of high level messages, and correlate them with low-level messages (how?)");
            let mut result = None;
            {
                let mut ether = self.ether.lock().unwrap();
                let mut ether = &mut *ether;

                ether.packets.retain_mut(|x|{
                    let retained = if x.to == self.whoami && !x.arrived {
                        if x.arrive_time >= Instant::now() && result.is_none() {
                            buf.put(&*x.data);
                            x.arrived = true;
                            result = Some((x.data.len(), x.from));
                        }
                        true
                    } else {
                        x.arrive_time.elapsed().as_secs() <= 5
                    };
                    retained
                });
            }
            match result {
                Some((size, src)) => {return Ok((size,src));}
                None => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

    }
}

struct Driver {
    whoami: u8,
    ether: Arc<Mutex<Ether>>
}

impl CommunicationDriver for Driver {
    type Receiver = Receiver;
    type Sender = Sender;
    type Endpoint = u8;

    async fn initialize(&mut self, bind_address: &str, multicast_group: &str, mtu: usize) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
        Ok((
            Sender {
                whoami: self.whoami,
                ether: self.ether.clone()
            },
            Receiver {
                whoami: self.whoami,
                ether: self.ether.clone(),
            }
            ))
    }

    fn parse_endpoint(s: &str) -> anyhow::Result<Self::Endpoint> {
        Ok(s.parse::<u8>()?)
    }
}

#[derive(Default)]
struct Ether {
    packets: Vec<InflightPacket>,
    nodes: Vec<Node>
}

#[derive(Default)]
struct Visualizer {
    ether: Arc<Mutex<Ether>>,
}

impl App for Visualizer {
    fn update(&mut self, ctx: &Context, frame: &mut Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.ctx().request_repaint();


            let square_available = ui.available_width().min(ui.available_height() );


            let desired_size = vec2(
                square_available,
                square_available
            );
            let (id, canvas_rect) = ui.allocate_space(desired_size);

            let mut shapes = vec![];

            let ether = self.ether.lock().unwrap();
            for node in ether.nodes.iter() {

                let mut pos_x1 = (canvas_rect.width()* (node.pos.x-0.025));
                let mut pos_y1 = (canvas_rect.height()*(node.pos.y-0.025));
                let mut pos_x2 = (canvas_rect.width()* (node.pos.x+0.025));
                let mut pos_y2 = (canvas_rect.height()*(node.pos.y+0.025));

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

            for packet in ether.packets.iter() {
                let from = &ether.nodes[packet.from as usize].pos;
                let to = &ether.nodes[packet.to as usize].pos;
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


            ui.painter().extend(shapes);

        });
    }
}

async fn add_node(ether: Arc<Mutex<Ether>>, x: f32, y: f32) {

    let node_id = ether.lock().unwrap().nodes.len().try_into().unwrap();
    {
        let node1 = Node::new(node_id, Vec2::new(x,y));
        let mut ether = ether.lock().unwrap();
        ether.nodes.push(node1);
    }
    let comm1 = Node::start(node_id, ether.clone()).await;
    {
        let mut ether = ether.lock().unwrap();
        ether.nodes[node_id as usize].comm = Some(comm1);
    }

}
fn main() {
    let mut ether: Arc<Mutex<Ether>> = Arc::new(Default::default());

    let mut visualizer = Visualizer {
        ether: ether.clone(),
    };

    std::thread::spawn(move ||{

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let ether = ether.clone();
        runtime.block_on(async {

            add_node(ether.clone(), 0.2,0.2).await;
            add_node(ether.clone(), 0.7,0.1).await;
            add_node(ether.clone(), 0.3,0.7).await;

            std::future::pending::<()>().await;
        });


    });


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