

#[derive(Debug)]
struct OnDiskMessage {
    id: u64,
    seq: u64,
    data: Vec<u8>,
}

struct OnDiskMessageStore {


}



#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Instant;
    use rusqlite::Connection;
    use crate::on_disk_message_store::OnDiskMessage;

    #[test]
    fn test_create_disk_store() {
        if std::path::Path::exists(Path::new("test.bin")) {
            std::fs::remove_file("test.bin").unwrap();
        }
        let mut conn = Connection::open("test.bin").unwrap();

        conn.execute(
            "CREATE TABLE message (
            id    INTEGER PRIMARY KEY,
            seq INTEGER,
            data  BLOB
        );
        ",
            (), // empty list of parameters.
        ).unwrap();

        /*
        conn.execute(
            "
            CREATE INDEX message_idx1 ON message (id);
            ",
            (), // empty list of parameters.
        ).unwrap();
*/


        let trans = conn.transaction().unwrap();
        let bef_insert = Instant::now();
        for i in 0..1000_000 {
            let me = OnDiskMessage {
                id: 1000_000+i,
                seq: i,
                data: vec![42u8;1024],
            };
            trans.execute(
                "INSERT INTO message (id, data) VALUES (?1, ?2)",
                (&me.id, me.data),
            ).unwrap();
        }
        println!("Inserting 1 million: {:?}", bef_insert.elapsed());
        trans.commit().unwrap();

        let bef_query = Instant::now();

  /*      let mut stmt = conn.prepare("
                SELECT * FROM (
                SELECT ROW_NUMBER () OVER (
                    ORDER BY id
                ) rownum,id,data FROM message) t
                 where id >= ?1").unwrap();
*/

        let mut stmt = conn.prepare("
                SELECT seq FROM message
                 where id >= ?1").unwrap();
        let person_iter = stmt.query_map([1_000_000+999_900], |row| {

            let count: usize = row.get(0)?;
            println!("Seq: {}", count);
            Ok(())
           /* let rowno:usize = row.get(0)?;

            Ok((rowno-1,OnDiskMessage {
                id: row.get(1)?,
                data: row.get(2)?,
            }))*/
        }).unwrap();

        let mut idsum = 0;
        for person in person_iter {
            //println!("got: {:?}", person.unwrap());
            idsum += 1;
        }
        println!("Query {:?} (ids: {})", bef_query.elapsed(), idsum);
    }
}