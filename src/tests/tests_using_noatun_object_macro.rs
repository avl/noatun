use crate::data_types::{DatabaseCell, DatabaseVec};
use crate::{msg_deserialize, msg_serialize};
use crate::{Application, Database, MessagePayload, NoatunContext, NoatunTime};
use datetime_literal::datetime;
use savefile::Deserializer;
use savefile_derive::Savefile;
use std::io::Write;
use std::pin::Pin;
use std::time::Duration;
noatun_object!(
    struct Customer {
        pod name: u32,
        pod worth: u32,
    }
);
noatun_object!(
    struct Bank {
        pod total_money:u32,
        object customers: DatabaseVec<Customer>
    }
);

#[derive(Debug, Savefile)]
pub enum BankMessage {
    AddCustomerAndMoney { money: u32, customer: u32 },
}

// TODO: Rename MessagePayload to Message, and Message to MessageFrame?
impl MessagePayload for BankMessage {
    type Root = Bank;

    fn apply(&self, time: NoatunTime, mut root: Pin<&mut Self::Root>) {
        match self {
            BankMessage::AddCustomerAndMoney { money, customer } => {
                let prev_money = root.total_money();
                root.as_mut().set_total_money(prev_money + *money);
                root.customers_mut().push(&CustomerDetached {
                    name: 42,
                    worth: 100,
                });
            }
        }
    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, mut writer: W) -> anyhow::Result<()> {
        msg_serialize(self, writer)
    }
}

impl Application for Bank {
    type Message = BankMessage;
    type Params = ();

    fn initialize_root<'a>(params: &Self::Params) -> Pin<&'a mut Self> {
        NoatunContext.allocate_pod()
    }
}

#[test]
fn init_bank_miri() {
    let mut db: Database<Bank> = Database::create_in_memory(
        10_000,
        Duration::from_secs(1000),
        Some(datetime!(2023-01-01 Z)),
        None,
        (),
    )
    .unwrap();

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
