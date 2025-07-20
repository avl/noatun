use crate::{
    CutOffDuration, Database, DatabaseSettings, Message, MessageFrame, MessageId,
    SavefileMessageSerializer,
};
use savefile_derive::Savefile;
use std::pin::Pin;
use std::time::Duration;

noatun_object!(
    struct Doc {
        pod counter: u32,
    }
);

#[derive(Savefile, Debug)]
pub struct DocMessage {
    val: u32,
    reset: bool,
}

impl Message for DocMessage {
    type Root = Doc;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        if self.reset {
            root.counter.set(0);
        } else {
            root.counter += self.val;
        }
    }
}

#[test]
fn test_subsumption_cutoff_interaction() {
    // Is cutoff_interval implemented correctly?
    // it needs to know the last time information established by the prune candidate was observable.
    // this time can be looong after the directly written information was overwritten. I guess it
    // "just works", because this is actually the first time the pruning candidate can fulfill
    // the "not used" condition. But do we correctly wait for this overwriter to pass into the
    // before-cutoff region, if the overwriter doesn't depend on the overwritten?

    super::setup_tracing();
    let t0 = MessageId::new_debug2(0).timestamp();
    let mut db: Database<DocMessage> = Database::create_in_memory(
        10_000_000,
        DatabaseSettings {
            mock_time: Some(t0),
            cutoff_interval: CutOffDuration::from_hours(1).unwrap(),
            ..DatabaseSettings::default()
        },
    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();

    println!("Cur cutoff time: {:?}", db.current_cutoff_time());
    db.append_single(
        &MessageFrame::new(
            MessageId::generate_for_time(t0).unwrap(),
            vec![],
            DocMessage {
                val: 0,
                reset: true,
            },
        ),
        false,
    )
    .unwrap();

    let t1 = t0 + Duration::from_secs(1800);
    db.set_mock_time(t1).unwrap();

    db.append_single(
        &MessageFrame::new(
            MessageId::generate_for_time(t1).unwrap(),
            vec![],
            DocMessage {
                val: 0,
                reset: true,
            },
        ),
        false,
    )
    .unwrap();

    assert_eq!(
        db.count_messages(),
        2,
        "nothing can be pruned, observer could sneak in before t1"
    );

    let t2 = t1 + Duration::from_secs(1801);
    db.set_mock_time(t2).unwrap();
    db.maybe_advance_cutoff().unwrap();

    assert_eq!(
        db.count_messages(),
        2,
        "nothing can be pruned, observer could still sneak in before t1"
    );

    let t3 = t1 + Duration::from_secs(3600 + 1800);
    db.set_mock_time(t3).unwrap();
    db.maybe_advance_cutoff().unwrap();

    assert_eq!(
        db.count_messages(),
        1,
        "first can be pruned, observer can no longer sneak in before t1"
    );
}
