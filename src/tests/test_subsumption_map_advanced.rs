use std::sync::Mutex;
use std::fmt::Formatter;
use crate::data_types::NoatunHashMap;
use crate::database::DatabaseSettings;
use crate::{Database, Message, OpaqueNoatunCell, NoatunTime, NoatunMessageSerializer, MessageId};
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use byteorder::{LittleEndian, WriteBytesExt};
use byteorder::ReadBytesExt;
use std::io::Cursor;
noatun_object!(
    struct MapDoc {
        object items: NoatunHashMap<u32, OpaqueNoatunCell<u32>>,
    }
);



#[derive(Clone)]
pub struct MapMessage {
    func: Arc<dyn Fn(Pin<&mut MapDoc>)>,
}

impl MapMessage {
    pub fn new(func: impl Fn(Pin<&mut MapDoc>) + 'static) -> MapMessage {
        MapMessage { func: Arc::new(func) }
    }
}

impl std::fmt::Debug for MapMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MapMessage")
    }
}


thread_local! {
    static TSMA_TEMP: Mutex<Vec<MapMessage>> = const { Mutex::new(Vec::new()) };
}

pub struct TsmaSerializer;

impl NoatunMessageSerializer<MapMessage> for TsmaSerializer {
    fn deserialize(buf: &[u8]) -> anyhow::Result<MapMessage>
    where
        Self: Sized,
    {
        let mut buf = Cursor::new(buf);
        let index = buf.read_u32::<LittleEndian>().unwrap() as usize;
        Ok(TSMA_TEMP.with(move |x| {
            let guard = x.lock().unwrap();
            guard[index].clone()
        }))
    }

    fn serialize<W: Write>(msg: &MapMessage, mut writer: W) -> anyhow::Result<()> {
        TSMA_TEMP.with(move |x| {
            let mut guard = x.lock().unwrap();
            let index = guard.len();
            guard.push(msg.clone());
            writer.write_u32::<LittleEndian>(index as u32)?;
            Ok(())
        })
    }
}


impl Message for MapMessage {
    type Root = MapDoc;
    type Serializer = TsmaSerializer;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        (self.func)(root)
    }


}


#[test]
fn test_advanced_map1() {
    super::setup_tracing();
    let mut db: Database<MapMessage> = Database::create_in_memory(
        10_000_000,
        DatabaseSettings {
            mock_time: Some(NoatunTime::debug_time(0)),
            ..DatabaseSettings::default()
        },

    )
        .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();
    let msg = db.append_local(MapMessage::new(|doc|{
        let mut project = doc.pin_project();
        project.items.as_mut().insert(32, &32);

    }))
        .unwrap();
    db.mark_transmitted(msg.id).unwrap();

    let msg = db.append_local(MapMessage::new(|doc|{
        let mut project = doc.pin_project();
        project.items.as_mut().remove(&32);
        project.items.as_mut().insert(33, &33);

    }))
        .unwrap();
    db.mark_transmitted(msg.id).unwrap();

    let msg = db.append_local(MapMessage::new(|doc|{
        let mut project = doc.pin_project();
        project.items.as_mut().remove(&33);

    }))
        .unwrap();
    db.mark_transmitted(msg.id).unwrap();

    assert_eq!(db.count_messages(), 2, "both messages that do remove are tombstones");

    db.with_root(|root|{
        assert!(root.items.get(&32).is_none())
    });
}

#[test]
fn test_advanced_map2() {
    super::setup_tracing();
    let mut db: Database<MapMessage> = Database::create_in_memory(
        10_000_000,
        DatabaseSettings {
            mock_time: Some(NoatunTime::debug_time(0)),
            ..DatabaseSettings::default()
        },
    )
        .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();
    let msg = db.append_local(MapMessage::new(|doc| {
        let mut project = doc.pin_project();
        project.items.as_mut().insert(32, &32);
    }))
        .unwrap();
    db.mark_transmitted(msg.id).unwrap();
    let msg = db.append_local(MapMessage::new(|doc| {
        let mut project = doc.pin_project();
        let mut item = project.items.as_mut().get_mut_val(&32).unwrap();
        item += 2;
        item -= 1;
    }))
        .unwrap();
    db.mark_transmitted(msg.id).unwrap();
    
    db.with_root(|root|{
        let value = root.items.get(&32).unwrap();
        assert_eq!(value.query(), 33);
    });

    let msg = db.append_local(MapMessage::new(|doc| {
        let mut project = doc.pin_project();
        let mut item = project.items.as_mut().get_mut_val(&32).unwrap();
        item += 2;
        item -= 1;
    }))
        .unwrap();
    db.mark_transmitted(msg.id).unwrap();

    db.with_root(|root|{
        let value = root.items.get(&32).unwrap();
        assert_eq!(value.query(), 34);
    });
    
    assert_eq!(db.count_messages(), 3, "all the messages still affect the db");


}