use mermaid_rs::Mermaid;
use regex::{Captures, Regex};


fn main() {
    let mermaid = Mermaid::new().unwrap();

    let doc = std::fs::read_to_string("../docs/docs.md").unwrap();
    let re = Regex::new(r"(?s)```mermaid(.*?)```").unwrap();
    let re2 = Regex::new(r"`([\w_:]+?)`").unwrap();

    let val = re.replace_all(&doc, |cap: &Captures|{
        let render = mermaid.render(&cap[1]).unwrap();
        render +"\n"
    });

    let val = re2.replace_all(&val, |cap: &Captures| {
        format!("[`{}`]",&cap[1])
    });

    std::fs::write("../src/docs-svg.md", val.as_bytes()).unwrap();
}