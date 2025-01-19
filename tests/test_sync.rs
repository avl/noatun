use std::io::{Cursor, Write};
use std::time::Duration;
use bytemuck::{Pod, Zeroable};
use savefile::{Deserializer, Serializer};
use savefile_derive::Savefile;
use noatun::{disable_multi_instance_blocker, Application, Database, MessagePayload, NoatunContext, NoatunTime, Object, ThinPtr};
use noatun::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::data_types::DatabaseCell;

#[derive(Savefile, Debug)]
struct MazeMessage {
    delta_x: i32,
    delta_y: i32,
}

#[derive(Copy,Clone,Pod, Zeroable, Debug)]
#[repr(C)]
struct Maze {
    player_pos_x: DatabaseCell<u32>,
    player_pos_y: DatabaseCell<u32>,
}

impl Object for Maze {
    type Ptr = ThinPtr;

    unsafe fn access<'a>(index: Self::Ptr) -> &'a Self {
        unsafe { NoatunContext.access_pod(index) }
    }

    unsafe fn access_mut<'a>(index: Self::Ptr) -> &'a mut Self {
        unsafe { NoatunContext.access_pod_mut(index) }
    }
}

//compile_error!("COnsider introducing a thread local DatabaseContext!")

impl MessagePayload for MazeMessage {
    type Root = Maze;


    fn apply(&self, _time: NoatunTime, root: &mut Self::Root) {
        root.player_pos_x.set(root.player_pos_x.get().saturating_add_signed(self.delta_x));
        root.player_pos_y.set(root.player_pos_y.get().saturating_add_signed(self.delta_y));
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        Ok(Deserializer::bare_deserialize(&mut Cursor::new(buf),0)?)
    }

    fn serialize<W: Write>(&self, mut writer: W) -> anyhow::Result<()> {
        Ok(Serializer::bare_serialize(&mut writer, 0, self)?)
    }
}

impl Application for Maze {
    type Message = MazeMessage;
    type Params = ();

    fn initialize_root<'a>(_params:&()) -> &'a mut Maze {
        let maze = NoatunContext.allocate_pod();

        maze
    }
}

#[tokio::test]
#[ignore]
async fn test_sync_app() {
    let local = tokio::task::LocalSet::new();
    unsafe {
        // # SAFETY
        // We don't mix up DatabaseContext objects from different databases.
        // (This isn't exactly easy to do by accident, and we don't).
        disable_multi_instance_blocker();
    }

    local.run_until(async move {
        let mut comms = vec![];

        for i in 0..2 {

            let db: Database<Maze> = Database::create_new(
                format!("test/test_sync_app{}.bin",i),
                true,
                1_000_000,
                Duration::from_secs(86400),
                None,
                ()
            ).unwrap();
            let comm = DatabaseCommunication::new(db, DatabaseCommunicationConfig::default()).await;
            comms.push(comm);
        }

        comms[0].add_message(MazeMessage {
            delta_x: 1,
            delta_y: 0,
        }).await.unwrap();
        loop {
            for (i,comm) in comms.iter_mut().enumerate() {
                println!("State of db #{}:", i);
                comm.with_root(|root|{
                    println!("Root: {:#?}", &*root);
                });
            }
            tokio::time::sleep(Duration::from_secs(1)).await;

        }
    }).await;

}


