use crate::{MessageId, SavefileMessageSerializer};
use crate::data_types::NoatunHashMap;
use crate::database::{DatabaseSettings, OpenMode};
use crate::{ Database, Message, OpaqueNoatunCell, NoatunTime, Object};
use savefile_derive::Savefile;
use std::pin::Pin;

noatun_object!(
    struct MapDoc {
        object items: NoatunHashMap<u32, OpaqueNoatunCell<u32>>,
    }
);

#[derive(Savefile, Debug)]
pub struct MapMessage {
    index: usize,
    val: u32,
    reset: bool,
    destroy: bool,
}


impl Message for MapMessage {
    type Root = MapDoc;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        if self.destroy {
            root.items.destroy();
        } else if self.reset {
            root.items.clear();
        } else {
            let value = root.items.get_insert(&(self.index as u32));
            value.set(self.val);
        }
    }
}

#[test]
fn test_map_local1() {
    super::setup_tracing();
    let mut db: Database<MapMessage> = Database::create_new(
        "test/test_map_subsumption1",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
        .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();
    db.append_local(MapMessage {
        index: 42,
        val: 43,
        reset: false,
        destroy: false,
    })
        .unwrap();
    db.compact().unwrap();
    assert_eq!(db.count_messages(), 1);
    db.append_local(MapMessage {
        index: 0,
        val: 0,
        reset: true,
        destroy: false,
    })
        .unwrap();

    assert_eq!(db.count_messages(), 1, "1 remains, since tombstones can't be pruned until cutoff");
}

#[test]
fn test_map2() {
    super::setup_tracing();
    let mut db: Database<MapMessage> = Database::create_new(
        "test/test_map_subsumption2",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
        .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();
    let msg1 = db.append_local(MapMessage {
        index: 42,
        val: 43,
        reset: false,
        destroy: false,
    })
        .unwrap();
    db.mark_transmitted(msg1.id).unwrap();

    assert_eq!(db.count_messages(), 1);
    db.append_local(MapMessage {
        index: 0,
        val: 0,
        reset: true,
        destroy: false,
    })
        .unwrap();

    assert_eq!(db.count_messages(), 1, "all but the last message are subsumed, because local");
}
#[test]
fn test_map3() {
    super::setup_tracing();
    let mut db: Database<MapMessage> = Database::create_new(
        "test/test_map_subsumption3",
        OpenMode::Overwrite,
        DatabaseSettings {
            mock_time: Some(NoatunTime::debug_time(0)),
            ..DatabaseSettings::default()
        },

    )
        .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();
    let msg1 = db.append_local(MapMessage {
        index: 42,
        val: 43,
        reset: false,
        destroy: false,
    })
        .unwrap();
    db.mark_transmitted(msg1.id).unwrap();

    assert_eq!(db.count_messages(), 1);
    db.append_local(MapMessage {
        index: 0,
        val: 0,
        reset: true,
        destroy: false,
    })
        .unwrap();

    db.set_mock_time(NoatunTime::debug_time(1440)).unwrap();
    db.maybe_advance_cutoff().unwrap();

    assert_eq!(db.count_messages(), 1, "the reset-message is present in the clear-marker for the map");


    db.append_local(MapMessage {
        index: 0,
        val: 0,
        reset: false,
        destroy: true,
    })
        .unwrap();

    db.set_mock_time(NoatunTime::debug_time(2*1440)).unwrap();
    db.maybe_advance_cutoff().unwrap();
    assert_eq!(db.count_messages(), 0, "Destroying the map should erase the clear-marker too");
}
