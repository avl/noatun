use std::io::Write;
use std::pin::Pin;
use savefile_derive::Savefile;
use noatun::{msg_deserialize, msg_serialize, noatun_object, Application, CutOffDuration, Database, Message, NoatunTime, Object};
use noatun::data_types::{NoatunString, NoatunVec};
use noatun::database::DatabaseSettings;

noatun_object!(
    struct Employee {
        object name: NoatunString,
        pod salary: u32
    }
);

noatun_object!(
    struct ExampleDb {
        pod total_salary_cost: u32,
        object employees: NoatunVec<Employee>,
    }
);

#[derive(Savefile, Debug)]
pub struct ExampleMessage {
    name: String,
    salary: u32,
}

impl Message for ExampleMessage {
    type Root = ExampleDb;

    fn apply(&self, _time: NoatunTime, root: Pin<&mut Self::Root>) {
        let root = root.pin_project();

        let new_total_salary = root.total_salary_cost.detach() + self.salary;
        root.total_salary_cost.set(new_total_salary);

        root.employees.push(EmployeeDetached {
            name: self.name.clone(),
            salary: self.salary,
        });

    }

    fn deserialize(buf: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        msg_deserialize(buf)
    }

    fn serialize<W: Write>(&self, writer: W) -> anyhow::Result<()> {
        msg_serialize(self, writer)
    }
}

impl Application for ExampleDb {
    type Message = ExampleMessage;
    type Params = ();
}

fn main() {
    //TODO: Add convenience-constructor for create_new, with lots of defaults.
    let db: Database<ExampleDb> = Database::create_new("test/example1.bin", true, 1000000, CutOffDuration::from_minutes(15), DatabaseSettings::default(), ()).unwrap();



}