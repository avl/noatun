use colored::{ColoredString, Colorize};

pub fn lightblue(s: &str) -> ColoredString {
    s.truecolor(158, 190, 255)
}
pub fn lightgray(s: &str) -> ColoredString {
    s.truecolor(140, 140, 160)
}
pub fn lightgreen(s: &str) -> ColoredString {
    s.bright_green()
}
pub fn orange(s: &str) -> ColoredString {
    s.truecolor(0xff, 0x56, 00)
}
pub fn lightbluegreen(s: &str) -> ColoredString {
    s.truecolor(0, 0xff, 0xce)
}
pub fn red(s: &str) -> ColoredString {
    s.truecolor(0xff, 0x51, 0x50)
}
pub fn brightred(s: &str) -> ColoredString {
    s.truecolor(0xff, 0x90, 0x60)
}
pub fn lightbrown(s: &str) -> ColoredString {
    s.truecolor(0xff, 0x94, 0x52)
}
pub fn pink(s: &str) -> ColoredString {
    s.truecolor(0xff, 0x45, 0xa7)
}
pub fn turqouise(s: &str) -> ColoredString {
    s.truecolor(0x4c, 0xff, 0xb7)
}
pub fn rgb(s: &str, r: u8, g: u8, b: u8) -> ColoredString {
    s.truecolor(r, g, b)
}

fn colored_int_impl(i: i64, color: u32, hex: bool) -> ColoredString {
    let p = (color * 167 + 203) % (128 * 3);
    let r;
    let g;
    let b;
    if p < 128 {
        r = 255 - p;
        g = 128 + p;
        b = 255;
    } else if p < 256 {
        let p = p - 128;
        r = 128 + p;
        g = 255;
        b = 255 - p;
    } else {
        let p = p - 256;
        r = 255;
        g = 255 - p;
        b = 128 + p;
    }
    if hex { format!("{i:x}") } else { i.to_string() }.truecolor(r as u8, g as u8, b as u8)
}

pub fn colored_hex_int(i: u32) -> ColoredString {
    colored_int_impl(i as i64, i, true)
}
pub fn colored_int(i: u32) -> ColoredString {
    colored_int_impl(i as i64, i, false)
}
pub fn colored_sint(i: i32) -> ColoredString {
    colored_int_impl(i as i64, i as u32, false)
}
