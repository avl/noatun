#![cfg(feature="ratatui")]


use std::fmt::{Debug};

use itertools::Itertools;
use ratatui::layout::Constraint::{Length, Min, Percentage};
use ratatui::layout::Layout;
use ratatui::{Frame};

use ratatui::crossterm::event::{Event, KeyCode, KeyEvent};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};
use tokio::time::Instant;
use crate::communication::DatabaseCommunication;


use crate::{Message, Object};
use crate::simple_metrics::SimpleMetricsRecorder;


pub struct RatatuiInspector {
    received_packet_table: Table<'static>,
    sent_packet_table: Table<'static>,
    received_message_table: Table<'static>,
    sent_message_table: Table<'static>,
    db_message_table: Table<'static>,
    metrics_table: Table<'static>,
    start: Instant,
    x_offset: usize,
}

impl RatatuiInspector {
    pub fn input(&mut self, event:&Event) {
        match &event {
            Event::Key(KeyEvent { code: KeyCode::Left, .. }) => {
                self.x_offset = self.x_offset.saturating_sub(5);
            }
            Event::Key(KeyEvent { code: KeyCode::Right, .. }) => {
                self.x_offset = self.x_offset.saturating_add(5);
            }
            _ => {}}
        }
        pub fn new() -> Self {
        let rows: [Row;0] = [];

        let received_packet_table = Table::new(rows.clone(), [
            Length(25),
            Min(10),
            Length(15),
        ])
            .block(Block::new().title("Received Packets"))
            .header(Row::new(vec!["Time", "Type", "Size"]));

        let sent_packet_table = Table::new(rows.clone(), [
            Length(25),
            Min(10),
            Length(15),
        ])
            .block(Block::new().title("Sent Packets"))
            .header(Row::new(vec!["Time", "Type", "Size"]));

        let received_message_table = Table::new(rows.clone(), [
            Length(25),
            Min(10),
            Length(8),
            Length(15)
        ])
            .block(Block::new().title("Received Messages"))
            .header(Row::new(vec!["Time", "Message", "From", "Src addr"]));

        let sent_message_table = Table::new(rows.clone(), [
            Length(25),
            Min(10),
        ])
            .block(Block::new().title("Sent Messages"))
            .header(Row::new(vec!["Time", "Message"]));

        let db_message_table = Table::new(rows.clone(), [
            Length(50),
            Length(5),
            Length(5),
            Length(6),
            Percentage(15),
            Percentage(15),
            Percentage(15),
            Min(10)
        ])
            .block(Block::new().title("Message"))
            .header(Row::new(vec!["Id", "Seq2", "Live", "Flags", "Parents", "Reads", "Overwrites", "Message"]));

        let metrics_table = Table::new(rows.clone(), [
            Percentage(50),
            Percentage(50),
        ])
            .block(Block::new().title("Metrics/info"))
            .header(Row::new(vec!["Key", "Value"]));

        RatatuiInspector {
            received_packet_table,
            db_message_table,
            sent_packet_table,
            received_message_table,
            sent_message_table,

            metrics_table,

            start: Instant::now(),
            x_offset: 0,
        }
    }

    /// Draw the diagnostics ui
    ///
    /// Note, the root object must implement Debug. You can derive this like so:
    ///
    /// ```
    /// use noatun::noatun_object;
    ///
    /// noatun_object!(
    ///     #[derive(Debug)]
    ///     struct IssueDb {
    ///         // fields here
    ///     }
    /// );
    /// ```
    ///
    pub fn draw<MSG:Message+Send>(&mut self, frame: &mut Frame, recorder: &SimpleMetricsRecorder, comm: &DatabaseCommunication<MSG>) where <MSG::Root as Object>::DetachedOwnedType: Debug {
        let data = comm.inspector_data();
        let data = data.as_ref().expect("diagnostics are enabled, inspector data should be present");

        let main_layout = Layout::vertical([
            Percentage(33),
            Percentage(33),
            Percentage(33),
        ]);

        let [upper_area, mid_area, bottom_area] = main_layout.areas(frame.area());

        let split_layout = Layout::horizontal([
            Percentage(50),
            Percentage(50),
        ]);
        let split_layout3 = Layout::horizontal([
            Percentage(33),
            Percentage(33),
            Percentage(34),
        ]);

        let [received_packets_area, sent_packets_area, root_obj_area] = split_layout3.areas(upper_area);
        let [received_messages_area, sent_messages_area] = split_layout.areas(mid_area);


        let split_layout = Layout::horizontal([
            Percentage(25),
            Percentage(75),
        ]);
        let [metrics_area, db_msg_area] = split_layout.areas(bottom_area);


        let received_packet_block = Block::new()
            .borders(Borders::ALL)
            .title(format!("Packets Received"));

        let sent_packet_block = Block::new()
            .borders(Borders::ALL)
            .title(format!("Packets Sent"));

        let root_obj_block = Block::new()
            .borders(Borders::ALL)
            .title(format!("Root obj"));

        let received_messages_block = Block::new()
            .borders(Borders::ALL)
            .title(format!("Messages Received"));

        let sent_messages_block = Block::new()
            .borders(Borders::ALL)
            .title(format!("Messages sent"));

        let metrics_block = Block::new()
            .borders(Borders::ALL)
            .title(format!("Metrics"));

        let db_msg_block = Block::new()
            .borders(Borders::ALL)
            .title(format!("Database messages"));


        let mut received_packet_rows = vec![];
        for packet in data.received_packets.iter() {
            received_packet_rows.push(Row::new([
                format!("{:?}", packet.time.saturating_duration_since(self.start))
                , packet.packet.to_string(), packet.size.to_string()
            ]));
        }

        let mut sent_packet_rows = vec![];
        for packet in data.sent_packets.iter() {
            sent_packet_rows.push(Row::new([
                format!("{:?}", packet.time.saturating_duration_since(self.start))
                , packet.packet.to_string(), packet.size.to_string()
            ]));
        }

        let mut max_x_offset = 0;

        let x_offset = self.x_offset;
        let mut offset = |x: &str| -> String {
            let chars: Vec<char> = x.chars().collect();
            let cur_max_x_offset = x_offset.min(chars.len().saturating_sub(1));
            max_x_offset = cur_max_x_offset.max(max_x_offset);
            return chars[cur_max_x_offset..].iter().collect();
        };

        let mut received_message_rows = vec![];
        for message in data.received_messages.iter() {
            received_message_rows.push(Row::new([
                format!("{:?}", message.time.saturating_duration_since(self.start)), offset(&message.message), message.from.to_string(),
                message.src_addr.map(|x|x.to_string()).unwrap_or("?".to_string()),
            ]));
        }
        let mut sent_message_rows = vec![];
        for message in data.sent_messages.iter() {
            sent_message_rows.push(Row::new([
                format!("{:?}", message.time.saturating_duration_since(self.start)), offset(&message.message),
            ]));
        }
        let mut metrics_rows = vec![];

        let cutoff = comm.get_cutoff_time().unwrap();
        let heads = comm.get_update_heads().unwrap();
        metrics_rows.push(Row::new([
            "cutoff".to_string(), cutoff.to_string()
        ]));
        metrics_rows.push(Row::new([
            "heads".to_string(), heads.iter().map(|x|x.short()).join(", ")
        ]));
        metrics_rows.push(Row::new([
            "heads(count)".to_string(), heads.len().to_string()
        ]));


        for (key,val) in recorder.metrics_items() {
            metrics_rows.push(Row::new([
                key, val
            ]));
        }

        let mut db_rows = vec![];
        let all_msgs = comm.inner_database().begin_session().unwrap().get_all_messages_meta_vec().unwrap();
        let max_rows = db_msg_area.height.saturating_sub(3) as usize;
        for msg in &all_msgs[all_msgs.len().saturating_sub(max_rows)..] {
            db_rows.push(Row::new([
                msg.frame.header.id.to_string(),
                msg.seq.to_string(),
                msg.live.to_string(),
                msg.flags.to_string(),
                offset(&msg.frame.header.parents.iter().map(|x|x.short()).join(", ")),
                msg.reads.iter().join(","),
                msg.writes.iter().join(","),
                offset(&format!("{:?}", msg.frame.payload))
            ]));
        }
        frame.render_widget(
            self.received_packet_table.clone().rows(received_packet_rows).block(received_packet_block),
            received_packets_area);
        frame.render_widget(
            self.sent_packet_table.clone().rows(sent_packet_rows).block(sent_packet_block),
            sent_packets_area);

        let root_obj_str = comm.with_root(|root|format!("{:#?}", root.detach()));
        frame.render_widget(
            Paragraph::new(root_obj_str).block(root_obj_block),
            root_obj_area);

        frame.render_widget(
            self.received_message_table.clone().rows(received_message_rows).block(received_messages_block),
            received_messages_area);
        frame.render_widget(
            self.sent_message_table.clone().rows(sent_message_rows).block(sent_messages_block),
            sent_messages_area);

        frame.render_widget(
            self.metrics_table.clone().rows(metrics_rows).block(metrics_block),
            metrics_area);

        frame.render_widget(
            self.db_message_table.clone().rows(db_rows).block(db_msg_block),
            db_msg_area);
    }
}