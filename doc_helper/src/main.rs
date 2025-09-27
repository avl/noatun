use mermaid_rs::Mermaid;
use regex::{Captures, Regex};


fn main() {
    let mermaid = Mermaid::new().unwrap();

    let doc = std::fs::read_to_string("../docs/docs.md").unwrap();
    let mut re = Regex::new(r"(?s)```mermaid(.*?)```").unwrap();
    let mut re2 = Regex::new(r"`([\w:]+?)`").unwrap();

    let val = re.replace_all(&doc, |cap: &Captures|{
        let render = mermaid.render(&cap[1]).unwrap();
        render +"\n"
    });

    let val = re2.replace_all(&val, |cap: &Captures| {
        format!("[`{}`]",&cap[1])
    });

    std::fs::write("docs-svg.md", val.as_bytes()).unwrap();
}