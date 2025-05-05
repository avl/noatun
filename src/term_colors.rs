use colored::{ColoredString, Colorize};

pub fn lightblue(s: &str) -> ColoredString {
    s.truecolor(158, 190, 255)
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

pub fn colored_int(i: u32) -> ColoredString {
    let p = (i * 181 + 203) % (128 * 3);
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
    i.to_string().truecolor(r as u8, g as u8, b as u8)
}
