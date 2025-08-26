//! Simple app to show using a noatun db, without any replication
use anyhow::Result;
use noatun::data_types::{NoatunHashMap, NoatunString};
use noatun::database::{DatabaseSettings, OpenMode};
use noatun::{noatun_object, Database, Message, MessageId, Object, SavefileMessageSerializer};
use savefile_derive::Savefile;
use std::pin::Pin;

noatun_object!(
    #[derive(PartialEq)]
    struct Employee {
        object name: NoatunString,
        pod salary: u32
    }
);

noatun_object!(
    struct ExampleDb {
        pod total_salary_cost: u32,
        object employees: NoatunHashMap<NoatunString, Employee>,
    }
);

#[derive(Savefile, Debug)]
pub struct ExampleMessage {
    name: String,
    salary: u32,
}

impl Message for ExampleMessage {
    type Root = ExampleDb;
    type Serializer = SavefileMessageSerializer<Self>;

    fn apply(&self, _time: MessageId, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();

        let new_total_salary = root.total_salary_cost.export() + self.salary;
        root.total_salary_cost.set(new_total_salary);

        root.employees.insert(
            self.name.as_str(),
            &EmployeeNative {
                name: self.name.clone(),
                salary: self.salary,
            },
        );
    }
}

fn main() -> Result<()> {
    let mut db: Database<ExampleMessage> = Database::create_new(
        "test/example1.bin",
        OpenMode::Overwrite,
        DatabaseSettings::default(),
    )
    .unwrap();

    let mut s = db.begin_session_mut()?;
    s.append_local(ExampleMessage {
        name: "Andersen".to_string(),
        salary: 25,
    })?;
    s.append_local(ExampleMessage {
        name: "Smith".to_string(),
        salary: 20,
    })?;

    let mut employees: Vec<_> = s.with_root(|root| {
        assert_eq!(root.total_salary_cost.get(), 45);
        root.employees.export().values().cloned().collect()
    });

    employees.sort_by_key(|emp| emp.name.clone());
    assert_eq!(
        employees,
        vec![
            EmployeeNative {
                name: "Andersen".to_string(),
                salary: 25,
            },
            EmployeeNative {
                name: "Smith".to_string(),
                salary: 20,
            },
        ]
    );
    drop(s);
    db.sync_all()?;

    Ok(())
}
