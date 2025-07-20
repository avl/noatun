use noatun::communication::udp::TokioUdpDriver;
use noatun::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::data_types::NoatunCell;
use noatun::database::{DatabaseSettings, OpenMode};
use noatun::{
    CutOffDuration, Database, Message, MessageId, NoatunContext, NoatunStorable, Object,
    SavefileMessageSerializer, ThinPtr,
};
use savefile_derive::Savefile;
use std::pin::Pin;
use std::time::Duration;

#[derive(Savefile, Debug)]
struct MazeMessage {
    delta_x: i32,
    delta_y: i32,
}

#[derive(Debug)]
#[repr(C)]
struct Maze {
    player_pos_x: NoatunCell<u32>,
    player_pos_y: NoatunCell<u32>,
}

unsafe impl NoatunStorable for Maze {}

impl Object for Maze {
    type Ptr = ThinPtr;
    type DetachedType = (u32, u32);
    type DetachedOwnedType = (u32, u32);

    fn detach(&self) -> Self::DetachedOwnedType {
        unimplemented!()
    }

    fn destroy(self: Pin<&mut Self>) {
        unimplemented!()
    }

    fn init_from_detached(self: Pin<&mut Self>, detached: &Self::DetachedType) {
        let tself = unsafe { self.get_unchecked_mut() };
        unsafe {
            Pin::new_unchecked(&mut tself.player_pos_x).set(detached.0);
            Pin::new_unchecked(&mut tself.player_pos_y).set(detached.1);
        }
    }

    unsafe fn allocate_from_detached<'a>(detached: &Self::DetachedType) -> Pin<&'a mut Self> {
        let mut temp: Pin<&mut Maze> = NoatunContext.allocate();
        temp.as_mut().init_from_detached(detached);
        temp
    }
}

impl Message for MazeMessage {
    type Root = Maze;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, mut root: Pin<&mut Self::Root>) {
        let root_player_pos_x = unsafe { root.as_mut().map_unchecked_mut(|x| &mut x.player_pos_x) };
        let x = root_player_pos_x.get().saturating_add_signed(self.delta_x);
        root_player_pos_x.set(x);

        let root_player_pos_y = unsafe { root.as_mut().map_unchecked_mut(|x| &mut x.player_pos_y) };
        let y = root_player_pos_y.get().saturating_add_signed(self.delta_y);
        root_player_pos_y.set(y);
    }
}

#[tokio::test]
#[ignore]
async fn test_sync_app() {
    let local = tokio::task::LocalSet::new();

    local
        .run_until(async move {
            let mut comms = vec![];

            for i in 0..2 {
                let db: Database<MazeMessage> = Database::create_new(
                    format!("test/test_sync_app{i}.bin"),
                    OpenMode::Overwrite,
                    DatabaseSettings {
                        cutoff_interval: CutOffDuration::from_days(1).unwrap(),
                        ..Default::default()
                    },
                )
                .unwrap();
                let comm = DatabaseCommunication::new_custom(
                    &mut TokioUdpDriver,
                    db,
                    DatabaseCommunicationConfig::default(),
                )
                .await
                .unwrap();
                comms.push(comm);
            }

            comms[0]
                .add_message(MazeMessage {
                    delta_x: 1,
                    delta_y: 0,
                })
                .await
                .unwrap();
            loop {
                for (i, comm) in comms.iter_mut().enumerate() {
                    println!("State of db #{i}:");
                    comm.with_root(|root| {
                        println!("Root: {root:#?}");
                    });
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await;
}
