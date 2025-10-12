use regex::{Captures, Regex};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;


fn main() {
    let doc = std::fs::read_to_string("../docs/docs.md").unwrap();
    let mermaid_re = Regex::new(r"(?s)```mermaid(.*?)```").unwrap();
    let docref_re = Regex::new(r"`([\w_:]+?)`").unwrap();
    let mermaid_strings_re = Regex::new(r##"(?s)"([^"]*?)""##).unwrap();

    let val = mermaid_re.replace_all(&doc, |cap: &Captures|{
        let mermaid = &cap[1];
        std::fs::write("temp.mmd", mermaid).unwrap();

        let strs = mermaid_strings_re.captures_iter(&mermaid)
            .map(|m| m[1].to_string())
            .collect::<Vec<_>>();

        std::process::Command::new("mmdc")
            .args(["-i","temp.mmd","-o","temp.png"])
            .status().unwrap();

        let render = std::fs::read("temp.png").unwrap();
        let b64 = BASE64_STANDARD.encode(&render);

        format!("
<img src=\"data:image/png;base64,{}\" alt=\"{}\"/>\n", b64, strs.join("\n"))
    });

    let val = docref_re.replace_all(&val, |cap: &Captures| {
        format!("[`{}`]", &cap[1])
    });

    std::fs::write("../src/docs-svg.md", val.as_bytes()).unwrap();
}