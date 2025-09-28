![build](https://github.com/avl/noatun/actions/workflows/rust.yml/badge.svg)
![clippy](https://github.com/avl/noatun/actions/workflows/clippy.yml/badge.svg)

# Noatun

Welcome to Noatun!

Noatun is an in-process, multi master, distributed database with automatic
garbage collection and a materialized view support, written in 100% Rust.
Noatun currently supports linux.

Using Noatun:

 * Define your messages
 * Define rules to apply messages to a materialized view
 * Noatun applies messages in order, time-traveling as needed if messages arrive out-of-order
 * Query materialized view using native rust

Noatun properties:
 * Synchronizes messages efficiently over networks
 * Automatically prunes messages that no longer have any effect
 * Fast, in-process, memory mapped materialized view

Resources:

[Rust Docs](https://docs.rs/noatun/latest/noatun/) 

[Manual](https://github.com/avl/noatun/blob/master/docs/docs.md)

### Limitations

 * Noatun is very new. There is an extensive test suite, but there may be bugs.
 * Currently only linux is supported. Macos/windows support is possible, and PR:s are welcome.
 
### Examples

The folder `examples` contains several examples. `examples/issue_tracker.rs` contains a ratatui-based
issue tracker.


