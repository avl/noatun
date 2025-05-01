use crate::cutoff::CutOffDuration;
use crate::database::DatabaseSettings;
use crate::distributor::{Distributor, DistributorMessage};
use crate::tests::{CounterMessage, CounterObject};
use crate::{Database, MessageId, NoatunTime};
use datetime_literal::datetime;
use std::iter::once;
use std::mem::swap;

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
) -> Database<CounterObject> {
    let mut db: Database<CounterObject> = Database::create_in_memory(
        10000,
        CutOffDuration::from_minutes(15),
        DatabaseSettings {
            mock_time: Some(datetime!(2021-01-01 Z).into()),
            ..Default::default()
        },
        (),
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
    //println!("Messages present: {:?}", db.get_all_message_ids());
    drop(sess);
    db
}

#[derive(Debug)]
struct SyncReport {
    num_messages: usize,
}

fn sync(dbs: Vec<Database<CounterObject>>) -> SyncReport {
    let mut report = SyncReport { num_messages: 0 };
    let mut dbs: Vec<(Distributor, Database<_>)> = dbs
        .into_iter()
        .enumerate()
        .map(|(index, x)| (Distributor::new(&index.to_string()), x))
        .collect();
    let mut ether = vec![];
    for (db_id, (distr, db)) in dbs.iter_mut().enumerate() {
        let sess = db.begin_session().unwrap();
        let mut sent = distr.get_periodic_message(&sess).unwrap();
        assert_eq!(sent.len(), 1, "no resync is active");
        let sent = sent.pop().unwrap();

        println!("db: {db_id:?} sent initial {sent:?}");
        report.num_messages += 1;
        ether.push((db_id, sent));
    }
    let mut next_ether = vec![];
    loop {
        for (db_id, (distr, db)) in dbs.iter_mut().enumerate() {
            let sent = distr
                .receive_message(
                    db,
                    ether
                        .iter()
                        .filter(|(x_src_id, _msg)| *x_src_id != db_id)
                        .map(|(_src, x)| x.clone()),
                )
                .unwrap();
            report.num_messages += sent.len();
            println!("db: {db_id:?} sent {sent:?}");
            next_ether.extend(sent.into_iter().map(|x| (db_id, x)));
        }
        if next_ether.is_empty() {
            break;
        }
        swap(&mut ether, &mut next_ether);
        next_ether.clear();
    }

    let first_set: Vec<_> = dbs[0]
        .1
        .begin_session()
        .unwrap()
        .get_all_message_ids()
        .unwrap();
    for (_distr, db) in dbs.iter().skip(1) {
        assert_eq!(
            first_set,
            db.begin_session().unwrap().get_all_message_ids().unwrap()
        );
    }
    report
}
#[test]
#[rustfmt::skip]
fn distributor_simple_deleted() {
    let dbs = vec![
        create_app(
            [
                (datetime!(2021-01-02 00:00:00 Z),[].as_slice(),1,0,true),
                (datetime!(2021-01-03 00:00:00 Z),[ NoatunTime::from_datetime(datetime!(2021-01-02 00:00:00 Z))].as_slice(),0,7,true),
            ]),
        create_app(
                [
                (datetime!(2021-01-04 00:00:00 Z),[].as_slice(),3,0,true),
            ]),
    ];
    let report = sync(dbs);
    assert_eq!(report.num_messages, 10);
}

#[test]
#[rustfmt::skip]
fn distributor_simple_unsync() {
    let dbs = vec![
        create_app(
            [
                (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
                (datetime!(2021-01-02 00:00:00 Z),[datetime!(2021-01-01 00:00:00 Z).into()].as_slice(),2,0,true),
            ]),
        create_app(
            [
                (datetime!(2021-01-03 00:00:00 Z),[].as_slice(),3,0,true),
            ]),
    ];
    let report = sync(dbs);
    assert_eq!(report.num_messages, 11);
}

#[test]
#[rustfmt::skip]
fn distributor_simple_in_sync() {
    let dbs = vec![
        create_app(
            [
                (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
            ]),
        create_app(
            [
                (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
            ]),
    ];
    let report = sync(dbs);
    assert_eq!(report.num_messages, 2);
}

#[test]
#[rustfmt::skip]
fn distributor_simple_almost_sync() {
    let dbs = vec![
        create_app(
            [
                (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
                (datetime!(2021-01-02 00:00:00 Z),[datetime!(2021-01-01 00:00:00 Z).into()].as_slice(),2,0,true),
            ]),
        create_app(
            [
                (datetime!(2021-01-01 00:00:00 Z),[].as_slice(),1,0,true),
            ]),
    ];
    sync(dbs);
}
#[test]
fn test_distributor() {
    let mut app1 = create_app([(datetime!(2021-01-01 Z), [].as_slice(), 1, 0, true)]);
    let mut app2 = create_app([(datetime!(2021-01-02 Z), [].as_slice(), 1, 0, true)]);

    let mut dist1 = crate::distributor::Distributor::new("1");
    let mut dist2 = crate::distributor::Distributor::new("2");

    let sess1 = app1.begin_session().unwrap();
    let mut msg1 = dist1.get_periodic_message(&sess1).unwrap();
    assert_eq!(msg1.len(), 1, "no resync is in progress");
    let msg1 = msg1.pop().unwrap();

    println!("dist1 sent: {msg1:?}");
    let mut result = dist2.receive_message(&mut app2, once(msg1)).unwrap();

    println!("dist2 sent: {result:?}");

    insta::assert_debug_snapshot!(result);
    assert_eq!(result.len(), 1);

    let mut result = dist1
        .receive_message(&mut app1, once(result.pop().unwrap()))
        .unwrap();
    println!("dist1 sent: {result:?}");
    insta::assert_debug_snapshot!(result);
    assert_eq!(result.len(), 1);

    let mut result = dist2
        .receive_message(&mut app2, once(result.pop().unwrap()))
        .unwrap();
    println!("dist2 sent: {result:?}");
    insta::assert_debug_snapshot!(result);

    let mut result = dist1
        .receive_message(&mut app1, once(result.pop().unwrap()))
        .unwrap();
    println!("dist1 sent: {result:?}");
    assert!(matches!(&result[0], DistributorMessage::Message(_, false)));
    assert_eq!(result.len(), 1);

    let _result = dist2
        .receive_message(&mut app2, once(result.pop().unwrap()))
        .unwrap();
    let sess2 = app2.begin_session().unwrap();
    println!("App2 all msgs: {:?}", sess2.get_all_message_ids().unwrap());
    println!("App2 update heads: {:?}", sess2.get_update_heads());

    insta::assert_debug_snapshot!(sess2.get_all_message_ids().unwrap());
    insta::assert_debug_snapshot!(sess2.get_update_heads());
}
