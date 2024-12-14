use crate::database::DatabaseSettings;
use crate::distributor::{Distributor, DistributorMessage, EphemeralNodeId, Neighborhood};
use crate::mini_pather::MiniPather;
use crate::noatun_instant::Instant;
use crate::tests::CounterMessage;
use crate::{set_test_epoch, Database, MessageId, NoatunTime};
use arcshift::ArcShift;
use datetime_literal::datetime;
use std::iter::once;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

fn create_app<'a>(
    msgs: impl IntoIterator<
        Item = (
            impl Into<NoatunTime>,
            &'a [NoatunTime],
            i32,
            u32,
            bool, /*local*/
        ),
    >,
) -> Database<CounterMessage> {
    let mut db: Database<CounterMessage> = Database::create_in_memory(
        10000,
        DatabaseSettings {
            mock_time: Some(datetime!(2021-01-01 Z).into()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut sess = db.begin_session_mut().unwrap();

    for (id, parents, inc1, set1, local) in msgs {
        let id: NoatunTime = id.into();
        sess.append_single(
            &CounterMessage {
                id: MessageId::from_parts_for_test(id, 0),
                parent: parents
                    .iter()
                    .copied()
                    .map(|x| MessageId::from_parts_for_test(x, 0))
                    .collect(),
                inc1,
                set1,
            }
            .wrap(sess.current_cutoff_time().unwrap()),
            local,
        )
        .unwrap();
    }
    drop(sess);
    db
}

#[test]
fn test_distributor() {
    set_test_epoch(Instant::now());
    let mut app1 = create_app([(datetime!(2021-01-01 Z), [].as_slice(), 1, 0, true)]);
    let mut app2 = create_app([(datetime!(2021-01-02 Z), [].as_slice(), 1, 0, true)]);

    let mut dist1 = Distributor::new(
        Duration::from_secs(5),
        ArcShift::new(EphemeralNodeId::new(1)),
        Instant::now(),
        None,
    );
    let mut dist2 = Distributor::new(
        Duration::from_secs(5),
        ArcShift::new(EphemeralNodeId::new(2)),
        Instant::now(),
        None,
    );

    dist1.neighborhood =
        Neighborhood::new(Instant::now(), Arc::new(RwLock::new(MiniPather::new(1))));
    dist2.neighborhood =
        Neighborhood::new(Instant::now(), Arc::new(RwLock::new(MiniPather::new(2))));
    dist1
        .neighborhood
        .get_insert_peer(EphemeralNodeId::new(2), Instant::now());

    let sess1 = app1.begin_session().unwrap();
    let mut msg1 = dist1.get_periodic_message(&sess1, Instant::now()).unwrap();
    assert_eq!(msg1.len(), 1, "no resync is in progress");
    let msg1 = msg1.pop().unwrap();

    println!("dist1 sent: {msg1:?}");
    let mut result = dist2
        .receive_message2(&mut app2, once(msg1), Instant::now())
        .unwrap();

    println!("dist2 sent: {result:?}");

    insta::assert_debug_snapshot!(result);
    assert_eq!(result.len(), 1);

    let mut result = dist1
        .receive_message2(&mut app1, once(result.pop().unwrap()), Instant::now())
        .unwrap();
    println!("dist1 sent: {result:?}");
    insta::assert_debug_snapshot!(result);
    assert_eq!(result.len(), 1);

    let mut result = dist2
        .receive_message2(&mut app2, once(result.pop().unwrap()), Instant::now())
        .unwrap();
    println!("dist2 sent: {result:?}");
    insta::assert_debug_snapshot!(result);

    let mut result = dist1
        .receive_message2(&mut app1, once(result.pop().unwrap()), Instant::now())
        .unwrap();
    println!("dist1 sent: {result:?}");
    assert!(matches!(
        &result[0],
        DistributorMessage::Message {
            demand_ack: false,
            ..
        }
    ));
    assert_eq!(result.len(), 1);

    let _result = dist2
        .receive_message2(&mut app2, once(result.pop().unwrap()), Instant::now())
        .unwrap();
    let sess2 = app2.begin_session().unwrap();
    println!("App2 all msgs: {:?}", sess2.get_all_message_ids().unwrap());
    println!("App2 update heads: {:?}", sess2.get_update_heads());

    insta::assert_debug_snapshot!(sess2.get_all_message_ids().unwrap());
    insta::assert_debug_snapshot!(sess2.get_update_heads());
}
