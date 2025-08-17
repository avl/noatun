//! With the optional feature "ratatui", noatun has built-in support for showing
//! diagnostics information using the "ratatui" terminal UI library.
//!
//! Construct an instance of [`RatatuiInspector`] to use this feature.
#![cfg(feature = "ratatui")]

use std::fmt::Debug;
use std::time::Duration;
use anyhow::Context;
use itertools::Itertools;
use ratatui::crossterm::event;
use ratatui::layout::Constraint::{Length, Min, Percentage};
use ratatui::layout::Layout;
use ratatui::Frame;

use crate::communication::DatabaseCommunication;
use ratatui::crossterm::event::{Event, KeyCode, KeyEvent};
use ratatui::prelude::{Color, Stylize};
use ratatui::style::Style;
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table, TableState};
use crate::noatun_instant::Instant;

use crate::simple_metrics::SimpleMetricsRecorder;
use crate::{Message, Object};

/// State used for rendering diagnostics information
pub struct RatatuiInspector {
    received_packet_table: Table<'static>,
    sent_packet_table: Table<'static>,
    received_message_table: Table<'static>,
    sent_message_table: Table<'static>,
    db_message_table: Table<'static>,
    metrics_table: Table<'static>,
    highlight: usize,
    zoom: bool,
    start: Instant,
    x_offset: usize,
    table_state: [TableState;7],
}

impl Default for RatatuiInspector {
    fn default() -> Self {
        Self::new()
    }
}

trait BlockExt : Sized {
    fn highlight(self, our_index: usize, active_highlight: usize) -> Self;

}

impl<'a> BlockExt for Block<'a> {
    fn highlight(self, our_index: usize, active_highlight: usize) -> Block<'a> {

        if our_index == active_highlight {
            self.border_style(Style::default().bg(Color::White).fg(Color::Black))
        } else {
            self
        }
    }
}

/// Run
pub fn run_inspector<MSG: Message + Send>(
    recorder: Option<&SimpleMetricsRecorder>,
    comm: &DatabaseCommunication<MSG>,
) -> anyhow::Result<()> where
    <MSG::Root as Object>::DetachedOwnedType: Debug {
    let mut inspector = RatatuiInspector::new();
    let mut terminal = ratatui::init();

    loop {
        if event::poll(Duration::from_millis(250)).context("event poll failed")? {
            let event = event::read().context("event read failed")?;
            if !inspector.input(&event) {
                if let Event::Key(KeyEvent { code: KeyCode::Esc|KeyCode::Char('q')|KeyCode::Char('Q'), .. }) = event {
                    ratatui::restore();
                    return Ok(());
                }
            }
        }
        terminal.draw(|frame| {
            inspector.draw(frame, recorder, comm);
        })?;
    }
}

impl RatatuiInspector {

    /// Supply information to the inspector. Use this from a ratatui main-loop in a pre-existing
    /// ratatui app.
    ///
    /// Returns true if input key was caught
    pub fn input(&mut self, event: &Event) -> bool {
        match &event {
            Event::Key(KeyEvent{code, ..}) => {
                match code {
                    KeyCode::Esc if self.zoom => {
                        self.zoom = false;
                    }
                    KeyCode::Left => {
                        self.x_offset = self.x_offset.saturating_sub(5);
                    }

                    KeyCode::Right=> {
                        self.x_offset = self.x_offset.saturating_add(5);
                    }
                    KeyCode::Enter|KeyCode::Char(' ') => {
                        self.zoom = !self.zoom;
                    }
                    KeyCode::Tab => {
                        self.highlight += 1;
                        self.highlight%=7;
                    }
                    KeyCode::BackTab => {
                        self.highlight += 7 - 1;
                        self.highlight%=7;
                    }
                    KeyCode::Up => {
                        let next = self.table_state[self.highlight].selected().unwrap_or(0).saturating_sub(1);
                        self.table_state[self.highlight].select(Some(next));
                    }
                    KeyCode::Down => {
                        let next = self.table_state[self.highlight].selected().map(|x|x+1).unwrap_or(0);
                        self.table_state[self.highlight].select(Some(next));
                    }
                    _ => {return false}
                }
            }
            _ => {
                return false
            }
        }
        true
    }

    /// Create a new ratatui inspector
    pub fn new() -> Self {
        let rows: [Row; 0] = [];

        let received_packet_table = Table::new(rows.clone(), [Length(25), Min(10), Length(15)])
            .block(Block::new().title("Received Packets"))
            .header(Row::new(vec!["Time", "Type", "Size"])).row_highlight_style(Style::new().reversed());

        let sent_packet_table = Table::new(rows.clone(), [Length(25), Min(10), Length(15)])
            .block(Block::new().title("Sent Packets"))
            .header(Row::new(vec!["Time", "Type", "Size"])).row_highlight_style(Style::new().reversed());

        let received_message_table =
            Table::new(rows.clone(), [Length(25), Min(10), Length(8), Length(15)])
                .block(Block::new().title("Received Messages"))
                .header(Row::new(vec!["Time", "Message", "From", "Src addr"])).row_highlight_style(Style::new().reversed());

        let sent_message_table = Table::new(rows.clone(), [Length(25), Min(10)])
            .block(Block::new().title("Sent Messages"))
            .header(Row::new(vec!["Time", "Message"])).row_highlight_style(Style::new().reversed());

        let db_message_table = Table::new(
            rows.clone(),
            [
                Length(50),
                Length(5),
                Length(5),
                Length(6),
                Percentage(15),
                Percentage(15),
                Percentage(15),
                Min(10),
            ],
        )
        .block(Block::new().title("Message"))
        .header(Row::new(vec![
            "Id",
            "Seq",
            "Live",
            "Flags",
            "Parents",
            "Reads",
            "Overwrites",
            "Message",
        ]))
            .row_highlight_style(Style::new().reversed());

        let metrics_table = Table::new(rows.clone(), [Percentage(50), Percentage(50)])
            .block(Block::new().title("Metrics/info"))
            .header(Row::new(vec!["Key", "Value"])).row_highlight_style(Style::new().reversed());

        RatatuiInspector {
            received_packet_table,
            db_message_table,
            sent_packet_table,
            received_message_table,
            sent_message_table,

            metrics_table,

            highlight: 0,
            zoom: false,
            start: Instant::now(),
            x_offset: 0,
            table_state: [();7].map(|_|Default::default()),

        }
    }


    /// Draw the diagnostics ui.
    ///
    /// This method is useful if you already have a ratatui app and want to show
    /// the inspector ui from within this app.
    pub fn draw<MSG: Message + Send>(
        &mut self,
        frame: &mut Frame,
        recorder: Option<&SimpleMetricsRecorder>,
        comm: &DatabaseCommunication<MSG>,
    ) where
        <MSG::Root as Object>::DetachedOwnedType: Debug,
    {
        let data = comm.diagnostics_data();
        let data = data
            .as_ref()
            .expect("Diagnostics must be enabled in order to use ratatui inspector. Set DatabaseCommunicationConfig::enable_diagnostics to true.");

        let main_layout = Layout::vertical([Percentage(33), Percentage(33), Percentage(33)]);

        let frame_area = frame.area();

        let zoom = self.zoom;
        let zoomed = move |area| if zoom {frame_area} else {area};

        let [upper_area, mid_area, bottom_area] = main_layout.areas(frame_area);

        let split_layout = Layout::horizontal([Percentage(50), Percentage(50)]);
        let split_layout3 = Layout::horizontal([Percentage(33), Percentage(33), Percentage(34)]);

        let [received_packets_area, sent_packets_area, root_obj_area] =
            split_layout3.areas(upper_area);
        let [received_messages_area, sent_messages_area] = split_layout.areas(mid_area);

        let split_layout = Layout::horizontal([Percentage(25), Percentage(75)]);
        let [metrics_area, db_msg_area] = split_layout.areas(bottom_area);

        let received_packet_block = Block::new().borders(Borders::ALL).title("Packets Received").highlight(0, self.highlight);

        let sent_packet_block = Block::new().borders(Borders::ALL).title("Packets Sent").highlight(1, self.highlight);

        let root_obj_block = Block::new().borders(Borders::ALL).title("Root obj").highlight(2, self.highlight);

        let received_messages_block = Block::new()
            .borders(Borders::ALL)
            .title("Messages Received").highlight(3, self.highlight);

        let sent_messages_block = Block::new().borders(Borders::ALL).title("Messages sent").highlight(4, self.highlight);

        let metrics_block = Block::new().borders(Borders::ALL).title("Metrics").highlight(5, self.highlight);

        let db_msg_block = Block::new()
            .borders(Borders::ALL)
            .title("Database messages").highlight(6, self.highlight)
            .title_bottom("Flags: T = Overwriter tainted, S = Tombstone, N = Wrote non-opaque");

        let mut max_x_offset = 0;

        let x_offset = self.x_offset;
        let mut offset = |x: &str| -> String {
            let chars: Vec<char> = x.chars().collect();
            let cur_max_x_offset = x_offset.min(chars.len().saturating_sub(1));
            max_x_offset = cur_max_x_offset.max(max_x_offset);
            chars[cur_max_x_offset..].iter().collect()
        };

        let mut received_packet_rows = vec![];
        for packet in data.received_packets.iter() {
            received_packet_rows.push(Row::new([
                format!("{:?}", packet.time.saturating_duration_since(self.start)),
                offset(&packet.packet),
                packet.size.to_string(),
            ]));
        }

        let mut sent_packet_rows = vec![];
        for packet in data.sent_packets.iter() {
            sent_packet_rows.push(Row::new([
                format!("{:?}", packet.time.saturating_duration_since(self.start)),
                offset(&packet.packet),
                packet.size.to_string(),
            ]));
        }



        let mut received_message_rows = vec![];
        for message in data.received_messages.iter() {
            received_message_rows.push(Row::new([
                format!("{:?}", message.time.saturating_duration_since(self.start)),
                offset(&message.message),
                message.from.to_string(),
                message
                    .src_addr
                    .map(|x| x.to_string())
                    .unwrap_or("?".to_string()),
            ]));
        }
        let mut sent_message_rows = vec![];
        for message in data.sent_messages.iter() {
            sent_message_rows.push(Row::new([
                format!("{:?}", message.time.saturating_duration_since(self.start)),
                offset(&message.message),
            ]));
        }
        let mut metrics_rows = vec![];

        let cutoff = comm.get_cutoff_time().unwrap();
        let heads = comm.get_update_heads().unwrap();
        metrics_rows.push(Row::new(["cutoff".to_string(), cutoff.to_string()]));
        metrics_rows.push(Row::new([
            "heads".to_string(),
            heads.iter().map(|x| x.short()).join(", "),
        ]));
        metrics_rows.push(Row::new([
            "heads(count)".to_string(),
            heads.len().to_string(),
        ]));

        if let Some(recorder) = recorder {
            for (key, val) in recorder.metrics_items() {
                metrics_rows.push(Row::new([key, val]));
            }
        }

        let mut db_rows = vec![];
        let all_msgs = comm
            .inner_database()
            .begin_session()
            .unwrap()
            .get_all_messages_meta_vec()
            .unwrap();

        for msg in &all_msgs {
            db_rows.push(Row::new([
                msg.frame.header.id.to_string(),
                msg.seq.to_string(),
                msg.live.to_string(),
                msg.flags.to_string(),
                offset(
                    &msg.frame
                        .header
                        .parents
                        .iter()
                        .map(|x| x.short())
                        .join(", "),
                ),
                msg.reads.iter().join(","),
                msg.writes.iter().join(","),
                offset(&format!("{:?}", msg.frame.payload)),
            ]));
        }

        if !self.zoom || self.highlight == 0 {
            frame.render_stateful_widget(
                self.received_packet_table
                    .clone()
                    .rows(received_packet_rows)
                    .block(received_packet_block),
                zoomed(received_packets_area),
                &mut self.table_state[0]
            );
        }
        if !self.zoom || self.highlight == 1 {
            frame.render_stateful_widget(
                self.sent_packet_table
                    .clone()
                    .rows(sent_packet_rows)
                    .block(sent_packet_block),
                zoomed(sent_packets_area),
                &mut self.table_state[1]
            );
        }

        if !self.zoom || self.highlight == 2 {
            let root_obj_str = comm.with_root(|root| format!("{:#?}", root.detach()));


            frame.render_widget(
                Paragraph::new(root_obj_str).block(root_obj_block),
                zoomed(root_obj_area)
            );

        }

        if !self.zoom || self.highlight == 3 {
            frame.render_stateful_widget(
                self.received_message_table
                    .clone()
                    .rows(received_message_rows)
                    .block(received_messages_block),
                zoomed(received_messages_area),
                &mut self.table_state[3]
            );
        }
        if !self.zoom || self.highlight == 4 {
            frame.render_stateful_widget(
                self.sent_message_table
                    .clone()
                    .rows(sent_message_rows)
                    .block(sent_messages_block),
                zoomed(sent_messages_area),
                &mut self.table_state[4]
            );
        }

        if !self.zoom || self.highlight == 5 {
            frame.render_stateful_widget(
                self.metrics_table
                    .clone()
                    .rows(metrics_rows)
                    .block(metrics_block),
                zoomed(metrics_area),
                &mut self.table_state[5]
            );
        }

        if !self.zoom || self.highlight == 6 {
            frame.render_stateful_widget(
                self.db_message_table
                    .clone()
                    .rows(db_rows)
                    .block(db_msg_block),
                zoomed(db_msg_area),
                &mut self.table_state[6]
            );
        }
    }
}
