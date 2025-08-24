use crate::data_types::OpaqueNoatunVec;
use crate::database::{DatabaseSettings, OpenMode};
use crate::{
    Database, Message, MessageId, NoatunTime, Object, OpaqueNoatunCell, SavefileMessageSerializer,
};
use savefile_derive::Savefile;
use std::pin::Pin;

noatun_object!(
    struct VecDoc {
        object items: OpaqueNoatunVec<OpaqueNoatunCell<u32>>,
    }
);

#[derive(Savefile, Debug)]
pub struct VecMessage {
    index: usize,
    val: u32,
    reset: bool,
    push: bool,
    destroy: bool,
}

impl Message for VecMessage {
    type Root = VecDoc;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();
        if self.reset {
            root.items.clear();
        } else if self.destroy {
            root.items.destroy();
        } else if self.push {
            root.items.push(self.val);
        } else {
            root.items.set_item_infallible(self.index, self.val);
        }
    }
}

#[test]
fn test_vec0() {
    super::setup_tracing();
    let mut db: Database<VecMessage> = Database::create_new(
        "test/test_subsumption0",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    db.append_local(VecMessage {
        index: 0,
        val: 42,
        reset: false,
        push: false,
        destroy: false,
    })
    .unwrap();

    assert_eq!(db.count_messages(), 1);
    db.append_local(VecMessage {
        index: 0,
        val: 0,
        reset: true,
        push: false,
        destroy: false,
    })
    .unwrap();

    assert_eq!(
        db.count_messages(),
        1,
        "last message is present in clear-tracker"
    );
}

#[test]
fn test_vec1() {
    super::setup_tracing();
    let mut db: Database<VecMessage> = Database::create_new(
        "test/test_subsumption1",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    for i in 0..3 {
        db.append_local(VecMessage {
            index: i,
            val: (i + 10) as u32,
            reset: false,
            push: false,
            destroy: false,
        })
        .unwrap();
        db.compact().unwrap();
    }
    assert_eq!(db.count_messages(), 3);
    db.append_local(VecMessage {
        index: 0,
        val: 0,
        reset: true,
        push: false,
        destroy: false,
    })
    .unwrap();

    assert_eq!(
        db.count_messages(),
        1,
        "last message is present in clear-tracker"
    );
}

#[test]
fn test_vec2() {
    super::setup_tracing();
    let mut db: Database<VecMessage> = Database::create_new(
        "test/test_subsumption2",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    for _i in 0..2 {
        let msg = db
            .append_local(VecMessage {
                index: 0,
                val: 0,
                reset: true,
                push: false,
                destroy: false,
            })
            .unwrap();
        db.mark_transmitted(msg.id).unwrap();
    }
    assert_eq!(
        db.count_messages(),
        1,
        "last message is present in clear tracker"
    );
}

#[test]
fn test_vec3() {
    super::setup_tracing();
    let mut db: Database<VecMessage> = Database::create_new(
        "test/test_subsumption3",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    for i in 0..2 {
        let msg = db
            .append_local(VecMessage {
                index: 0,
                val: i,
                reset: false,
                push: false,
                destroy: false,
            })
            .unwrap();
        db.mark_transmitted(msg.id).unwrap();
    }
    assert_eq!(
        db.count_messages(),
        1,
        "the second message subsumes the first"
    );
}

#[test]
fn test_vec4() {
    super::setup_tracing();
    let mut db: Database<VecMessage> = Database::create_new(
        "test/test_subsumption4",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    for i in 0..3 {
        let msg = db
            .append_local(VecMessage {
                index: i,
                val: (i + 10) as u32,
                reset: false,
                push: false,
                destroy: false,
            })
            .unwrap();
        db.mark_transmitted(msg.id).unwrap();
    }
    assert_eq!(db.count_messages(), 3);

    db.append_local(VecMessage {
        index: 0,
        val: 0,
        reset: true,
        push: false,
        destroy: false,
    })
    .unwrap();

    assert_eq!(
        db.count_messages(),
        1,
        "last message must remain, since earlier messages were non-local"
    );
}

#[test]
fn test_vec5() {
    let mut db: Database<VecMessage> = Database::create_in_memory(
        10_000_000,
        DatabaseSettings {
            mock_time: Some(NoatunTime::debug_time(0)),
            ..DatabaseSettings::default()
        },
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.disable_filesystem_sync().unwrap();

    for i in 0..3 {
        let msg = db
            .append_local(VecMessage {
                index: i,
                val: (i + 10) as u32,
                reset: false,
                push: true,
                destroy: false,
            })
            .unwrap();
        db.mark_transmitted(msg.id).unwrap();
    }

    db.with_root(|root| {
        assert_eq!(root.items[0].query(), 10);
    });

    assert_eq!(db.count_messages(), 3);

    db.append_local(VecMessage {
        index: 0,
        val: 0,
        reset: true,
        push: false,
        destroy: false,
    })
    .unwrap();

    assert_eq!(
        db.count_messages(),
        1,
        "last message must remain, since earlier messages were non-local"
    );

    db.append_local(VecMessage {
        index: 0,
        val: 0,
        reset: false,
        push: false,
        destroy: true,
    })
    .unwrap();

    // Destroying the vec makes the message a tombstone-message
    assert_eq!(
        db.count_messages(),
        1,
        "last message still remains, since it's a tombstone message"
    );

    db.set_mock_time(NoatunTime::debug_time(1440)).unwrap();

    assert_eq!(
        db.count_messages(),
        0,
        "the tombstone is removed after the cutoff-period has elapsed"
    );

    db.with_root(|root| {
        assert_eq!(root.items.detach(), vec![]);
    });
}
