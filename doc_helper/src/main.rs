use mermaid_rs::Mermaid;
use regex::{Captures, Regex};
fn main() {
    let mermaid = Mermaid::new().unwrap(); // An error may occur if the embedded Chromium instance fails to initialize
    //println!("{}", mermaid.render("graph TB\na-->b").unwrap());


    let doc = std::fs::read_to_string("../docs/docs.md").unwrap();
    let mut re = Regex::new(r"(?s)```mermaid(.*?)```").unwrap();

    let val = re.replace_all(&doc, |cap: &Captures|{
        //println!("Capture: {:?}", &cap[1]);
        let render = mermaid.render(&cap[1]).unwrap();
        render +"\n"
    });

    //println!("Result: {}", val);
    std::fs::write("docs-svg.md", val.as_bytes()).unwrap();

}