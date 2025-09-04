![build](https://github.com/avl/noatun/actions/workflows/rust.yml/badge.svg)
![clippy](https://github.com/avl/noatun/actions/workflows/clippy.yml/badge.svg)

# Noatun

Welcome to Noatun!

Noatun is an in-process, multi master, distributed event sourced database with automatic
garbage collection and an materialized view support, written in 100% Rust.

Using Noatun:

 * Define your messages
 * Define rules to apply messages to a materialized view
 * Noatun applies messages in order, time-traveling as needed if messages arrive out-of-order

Noatun properties:
 * Synchronizes messages efficiently over networks
 * Automatically prunes messages that no longer affect state
 * Fast, in-process, memory mapped view

Resources:

[Docs](https://docs.rs/noatun/latest/noatun/) [Manual](https://github.com/avl/noatun/blob/master/docs/docs.md)





