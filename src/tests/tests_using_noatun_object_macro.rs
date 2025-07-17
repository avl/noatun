use crate::SavefileMessageSerializer;
use crate::cutoff::CutOffDuration;
use crate::data_types::NoatunVec;
use crate::database::DatabaseSettings;
use crate::{Database, Message, NoatunTime};
use datetime_literal::datetime;
use savefile_derive::Savefile;
use std::pin::Pin;

noatun_object!(
    struct Customer {
        pod name: u32,
        pod worth: u32,
    }
);
noatun_object!(
    struct Bank {
        pod total_money:u32,
        object customers: NoatunVec<Customer>
    }
);

#[derive(Debug, Savefile)]
pub enum BankMessage {
    AddCustomerAndMoney { money: u32, customer: u32 },
}

impl Message for BankMessage {
    type Root = Bank;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: NoatunTime, mut root: Pin<&mut Self::Root>) {
        match self {
            BankMessage::AddCustomerAndMoney { money, customer: _ } => {
                let prev_money = root.total_money();
                root.as_mut().set_total_money(prev_money + *money);
                root.customers_mut().push(&CustomerDetached {
                    name: 42,
                    worth: 100,
                });
            }
        }
    }

    
}


#[test]
fn init_bank_miri() {
    let mut db: Database<BankMessage> = Database::create_in_memory(
        10_000,
        DatabaseSettings {
            mock_time: Some(datetime!(2023-01-01 Z).into()),
            cutoff_interval:         CutOffDuration::from_minutes(15),
            ..Default::default()
        },    )
    .unwrap();
    let mut db = db.begin_session_mut().unwrap();
    db.append_local(BankMessage::AddCustomerAndMoney {
        money: 10,
        customer: 10,
    })
    .unwrap();

    db.append_local(BankMessage::AddCustomerAndMoney {
        money: 10,
        customer: 10,
    })
    .unwrap();
    db.with_root(|root| {
        assert_eq!(root.total_money.get(), 20);
        assert_eq!(root.customers().len(), 2);
    })
}
