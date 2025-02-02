use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io::Write;
use std::ops::Add;
use std::pin::Pin;
use std::time::Duration;
use tracing::info;
use datetime_literal::datetime;
use crate::communication::{CommunicationDriver, CommunicationReceiveSocket, CommunicationSendSocket, DatabaseCommunication, DatabaseCommunicationConfig};
use tokio::sync::mpsc::{Sender, Receiver};
use crate::{Application, Database, MessagePayload, NoatunContext, NoatunTime, Object};
use crate::tests::CounterObject;
use crate::Savefile;
use std::sync::Arc;
use arcshift::ArcShift;
use bytes::BufMut;
use chrono::TimeDelta;
use libc::send;
use rand::{thread_rng, Rng};
use tokio::test;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::cell::RefCell;
use rand::distributions::uniform::SampleUniform;

thread_local! {
    pub static MY_THREAD_RNG: RefCell<Option<SmallRng>> = const { RefCell::new(None) };
}


noatun_object!(
    #[derive(PartialEq)]
    struct SyncApp {
        pod counter: u32,
        pod sum: u32,
    }
);

#[derive(Debug, Savefile, PartialEq)]
pub struct SyncMessage {
    value: u32
}


impl MessagePayload for SyncMessage {
    type Root = SyncApp;

    fn apply(&self, time: NoatunTime, root: Pin<&mut Self::Root>) {
        let mut project = root.pin_project();

        let prev_counter =  project.counter.get();
        let prev_sum =  project.sum.get();
        project.counter.set(prev_counter.wrapping_add(1));
        project.sum.set(prev_sum.wrapping_add(self.value));
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        crate::msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        crate::msg_serialize(self, writer)
    }
}

#[derive(Clone,Default)]
struct TestDriverInner {
    senders: Vec<Sender<(u8/*src*/,Vec<u8>)>>,
    loss: f32,
}

struct TestDriver {
    senders: ArcShift<TestDriverInner>,
}
impl TestDriver {
    pub fn set_loss(&mut self, loss: f32) {
        self.senders.rcu_safe(|item|{
            let mut cloned = item.clone();
            cloned.loss = loss;
            cloned
        })
    }
}
impl Default for TestDriver {
    fn default() -> Self {
        TestDriver {
            senders: ArcShift::new(TestDriverInner::default()),
        }
    }
}
struct TestDriverReceiver(Receiver<(u8/*src*/,Vec<u8>)>);
struct TestDriverSender(u8/*own addr*/,ArcShift<TestDriverInner>);

impl CommunicationReceiveSocket<u8> for TestDriverReceiver {
    fn recv_buf_from<B: BufMut + Send>(&mut self, buf: &mut B) -> impl Future<Output=std::io::Result<(usize, u8)>> + Send {
        async {
            let (src_addr, data) = self.0.recv().await.ok_or(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "all senders have gone away"))?;

            buf.put(&*data);
            Ok((data.len(), src_addr))
        }


    }
}

impl CommunicationSendSocket<u8> for TestDriverSender {
    fn local_addr(&self) -> anyhow::Result<u8> {
        Ok(self.0)
    }

    fn send_to(&mut self, buf: &[u8]) -> impl Future<Output=std::io::Result<usize>> + Send {
        async {
            let driver_inner = self.1.get();
            let data = buf.to_vec();
            for item in driver_inner.senders.iter() {
                if driver_inner.loss < random(0.0..1.0) {
                    item.send((self.0/*src*/,data.clone())).await;
                } else {
                    info!("== SIMULATOR CAUSED PACKET LOSS ==");
                }
            }
            Ok((buf.len()))
        }
    }
}

impl CommunicationDriver for TestDriver {
    type Receiver = TestDriverReceiver;
    type Sender = TestDriverSender;
    type Endpoint = u8;

    async fn initialize(&mut self, bind_address: &str, multicast_group: &str, mtu: usize) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
        let (tx,rx) = tokio::sync::mpsc::channel(100);

        let mut index = None;
        self.senders.rcu_safe(|prev|{
            let mut senders = prev.clone();
            index = Some(senders.senders.len());
            senders.senders.push(tx.clone());
            senders
        });

        Ok((TestDriverSender(index.unwrap().try_into().unwrap(), self.senders.clone()), TestDriverReceiver(rx)))
    }

    fn parse_endpoint(s: &str) -> anyhow::Result<Self::Endpoint> {
        Ok(s.parse()?)
    }
}


impl Application for SyncApp {
    type Message = SyncMessage;
    type Params = ();

    fn initialize_root<'a>(params: &Self::Params) -> Pin<&'a mut Self> {
        NoatunContext.allocate_pod()
    }
}

async fn create_app(driver: &mut TestDriver, node: u8) -> DatabaseCommunication<SyncApp> {

    let mut db: Database<SyncApp> = Database::create_in_memory(
        10000,
        Duration::from_secs(1000),
        Some(datetime!(2020-01-01 Z)),
        None,
        (),
    )
        .unwrap();

    DatabaseCommunication::async_tokio_new(
        driver,
        db,
        DatabaseCommunicationConfig {
            listen_address: "dummy".to_string(),
            multicast_address: "dummy".to_string(),
            mtu: 1500,
            bandwidth_limit_bytes_per_second: 1000,
        }
    ).await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn all_up_simple_sync_test() {
    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver, 1).await;
    let mut app2 = create_app(&mut driver, 2).await;

    app1.add_message(SyncMessage{value: 1}).await;
    app2.add_message(SyncMessage{value: 2}).await;

    tokio::time::sleep(Duration::from_secs(10)).await;

    let root1 = app1.with_root(|root|root.detach());
    let root2 = app2.with_root(|root|root.detach());

    assert_eq!(root1.sum, 3);
    assert_eq!(root2.sum, 3);
    assert_eq!(root1.counter, 2);
    assert_eq!(root2.counter, 2);
    assert_eq!(root1, root2);
}

fn random<T:SampleUniform+PartialOrd>(range: std::ops::Range<T>) -> T {
    MY_THREAD_RNG.with(|rng|rng.borrow_mut().as_mut().unwrap().gen_range(range))
}



#[tokio::test(start_paused = true)]
async fn all_up_gradual_update_sync_test() {

    pub struct TracingTimer(tokio::time::Instant);

    impl tracing_subscriber::fmt::time::FormatTime for TracingTimer {
        fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> core::fmt::Result {
            let t = tokio::time::Instant::now();
            write!(w, "{:>10?}", (t-self.0))
        }
    }


    let stdout_log = tracing_subscriber::fmt::layer()
        .with_timer(TracingTimer(tokio::time::Instant::now()))
        .pretty()
        .with_file(false)
        .with_writer(std::io::stdout);

    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    let subscriber = tracing_subscriber::registry()
        .with(
            stdout_log
        );
    tracing::subscriber::set_global_default(subscriber).expect("Failed to initialize tracing");


    let mut driver = TestDriver::default();
    let mut app1 = create_app(&mut driver, 1).await;
    let mut app2 = create_app(&mut driver, 2).await;

    MY_THREAD_RNG.set(Some(SmallRng::seed_from_u64(2)));
    //app1.add_message(SyncMessage{value: 1}).await;
    //app2.add_message(SyncMessage{value: 2}).await;

    driver.set_loss(0.5);
    let mut correct_count = 0;
    let mut correct_sum = 0;
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        app1.add_message_at(datetime!(2020-01-01 Z).add(TimeDelta::seconds(random(0..100))), SyncMessage{value: 1}).await;
        tokio::time::sleep(Duration::from_secs(random(0..10))).await;
        app2.add_message_at(datetime!(2020-01-01 Z).add(TimeDelta::seconds(random(0..100))),SyncMessage{value: 2}).await;
        correct_count += 2;
        correct_sum += 3;
    }
    info!(" -------------- NETWORK HEALED -----------------");
    driver.set_loss(0.0);
    tokio::time::sleep(Duration::from_secs(50)).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    let root1 = app1.with_root(|root|root.detach());
    let root2 = app2.with_root(|root|root.detach());

    let msgs1 = app1.get_all_messages().unwrap();
    let msgs2 = app2.get_all_messages().unwrap();
    info!("Msgs 1:\n{:#?}\nMsgs 2:\n{:#?}", msgs1, msgs2);
    assert_eq!(msgs1.len(), msgs2.len());
    assert_eq!(msgs1, msgs2);
    assert_eq!(root1.sum, correct_sum);
    assert_eq!(root2.sum, correct_sum);
    assert_eq!(root1.counter, correct_count);
    assert_eq!(root2.counter, correct_count);
    assert_eq!(root1, root2);
}

