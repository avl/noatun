use std::pin::Pin;
use std::time::Duration;
use anyhow::{Context, Result};
use flexi_logger::{FileSpec, LogSpecification};
use flexi_logger::trc::FormatConfig;
use flexi_logger::writers::FileLogWriter;

use ratatui::{ crossterm::event::{self, Event, KeyCode}, widgets::Paragraph, DefaultTerminal, Frame};
use ratatui::crossterm::event::KeyEvent;
use ratatui::layout::Constraint::Percentage;
use ratatui::layout::{Flex, Layout};
use ratatui::prelude::Constraint::{Length,  Min};
use ratatui::prelude::{Constraint, Line, Rect, Style, Stylize};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Clear, Row, Table, TableState, Wrap};
use savefile_derive::Savefile;
use tracing::trace;

use tui_textarea::TextArea;
use noatun::data_types::{NoatunHashMap, NoatunHashMapEntry, NoatunString, NoatunVec};
use noatun::{noatun_object, CutOffDuration, Database, DatabaseSettings, Message, MessageId, NoatunTime, Object, OpenMode, SavefileMessageSerializer};
use noatun::communication::{DatabaseCommunication, DatabaseCommunicationConfig};
use noatun::ratatui_inspector::RatatuiInspector;
use noatun::simple_metrics::SimpleMetricsRecorder;

noatun_object!(
    struct DescriptionText {
        pod time: NoatunTime,
        object text: NoatunString,
        object added_by: NoatunString,
    }
);

noatun_object!(
    struct Issue {
        pod created: NoatunTime,
        object reporter: NoatunString,
        object description: NoatunVec<DescriptionText>,
    }
);

noatun_object!(
    struct IssueDb {
        object issues: NoatunHashMap<NoatunString, Issue>,
    }
);

#[derive(Savefile, Debug)]
enum IssueMessage {
    AddIssue {
        reporter: String,
        heading: String,
    },
    AppendText {
        id: String,
        reporter: String,
        text: String,
    },
    RemoveIssue {
        id: String,
    },
    RenameIssue {
        id: String,
        id_new: String,
    }
}

impl Message for IssueMessage {
    type Root = IssueDb;
    type Serializer = SavefileMessageSerializer<IssueMessage>;

    fn apply(&self, message_id: MessageId, root: Pin<&mut Self::Root>) {
        let mut root = root.pin_project();
        match self {
            IssueMessage::AddIssue { reporter, heading } => {
                let issue = root.issues.get_insert(heading.as_str());
                let issue = issue.pin_project();
                trace!("assigning created");
                issue.created.set(message_id.timestamp());
                trace!("assigning reporter");
                issue.reporter.assign(reporter);

            }
            IssueMessage::RemoveIssue { id } => {
                root.issues.remove(id.as_str());
            }
            IssueMessage::AppendText { id, reporter, text } => {
                if let Some(issue) = root.issues.get_mut_val(id.as_str()) {
                    let issue = issue.pin_project();
                    issue.description.push(DescriptionTextDetached {
                        time: message_id.timestamp(),
                        text: text.to_string(),
                        added_by: reporter.to_string(),
                    });
                }
            }
            IssueMessage::RenameIssue { id, id_new } => {
                if !root.issues.as_mut().contains_key(id_new) {
                    match root.issues.as_mut().entry(id.to_string()) {
                        NoatunHashMapEntry::Occupied(o) => {
                            let prev = o.remove();
                            root.issues.as_mut().insert(id_new.as_str(), &prev);
                        }
                        NoatunHashMapEntry::Vacant(_) => {}
                    }
                }
            }
        }
    }
}


/// This is a bare minimum example. There are many approaches to running an application loop, so
/// this is not meant to be prescriptive. It is only meant to demonstrate the basic setup and
/// teardown of a terminal application.
///
/// This example does not handle events or update the application state. It just draws a greeting
/// and exits when the user presses 'q'.
fn main() -> Result<()> {
    let _keep_alive_handles = flexi_logger::trc::setup_tracing(
        LogSpecification::env().unwrap(),
        None,
        FileLogWriter::builder(FileSpec::default().suppress_timestamp()),
        &FormatConfig::default()
            .with_file(true),
    )?;


    let terminal = ratatui::init();
    let app_result = run(terminal).context("app loop failed");
    ratatui::restore();
    app_result
}


enum Popup {
    None,
    AddHeading(TextArea<'static>),
    AddText(TextArea<'static>),
    Rename(String, TextArea<'static>)
}

impl Popup {
    fn is_some(&self) -> bool {
        !matches!(self, Popup::None)
    }
}

struct AppState {
    text_table: Table<'static>,
    text_table_state: TableState,

    table: Table<'static>,
    table_state: TableState,
    selected_heading: Option<String>,
    row_count: usize,
    user: String,
    comms: DatabaseCommunication<IssueMessage>,
    popup: Popup,
    diagnostics: bool,
    recorder: SimpleMetricsRecorder,
    inspector: RatatuiInspector,

}

impl AppState {
    pub fn create_event(&mut  self, heading: &str) -> Result<()> {
        self.comms.blocking_add_message(IssueMessage::AddIssue {
            reporter: self.user.clone(),
            heading: heading.to_string(),
        })
    }
    pub fn rename(&mut self, old: String, new: String) -> Result<()> {
        self.comms.blocking_add_message(IssueMessage::RenameIssue {
            id: old,
            id_new: new,
        })
    }
    pub fn add_text(&mut  self, text: &str) -> Result<()> {
        if let Some(selected_heading) = &self.selected_heading {
            self.comms.blocking_add_message(IssueMessage::AppendText {
                reporter: self.user.clone(),
                id: selected_heading.to_string(),
                text: text.to_string(),
            })?;
        }
        Ok(())
    }
    pub fn delete_event(&mut self, heading: &str) -> Result<()> {
        self.comms.blocking_add_message(IssueMessage::RemoveIssue {
            id: heading.to_string(),
        })
    }
}

fn start_communication() -> Result<DatabaseCommunication<IssueMessage>> {
    let db: Database<IssueMessage> =
        Database::create_new(
            "issue_db",
            OpenMode::OpenCreate, DatabaseSettings {
                cutoff_interval: CutOffDuration::from_minutes(2),
                ..DatabaseSettings::default()
            })?;

    let mut distributed_db = DatabaseCommunication::new(
        db,
        DatabaseCommunicationConfig {
            enable_diagnostics: true,
            ..Default::default()
        },
    )?;

    Ok(distributed_db)
}

/// Run the application loop. This is where you would handle events and update the application
/// state. This example exits when the user presses 'q'. Other styles of application loops are
/// possible, for example, you could have multiple application states and switch between them based
/// on events, or you could have a single application state and update it based on events.
fn run(mut terminal: DefaultTerminal) -> Result<()> {
    let recorder = SimpleMetricsRecorder::default();
    recorder.clone().register_global();

    let comms = start_communication()?;



    let user = std::env::var("USER").unwrap_or("default-user".to_string());
    // Note: TableState should be stored in your application state (not constructed in your render
    // method) so that the selected row is preserved across renders
    let table_state = TableState::default();
    let text_table_state = TableState::default();


    let widths = [
        Length(25),
        Min(10),
        Length(15),
    ];

    let rows: [Row;0] = [];
    let table = Table::new(rows.clone(), widths)
        .block(Block::new().title("Table"))
        .header(Row::new(vec!["Time", "Heading", "Reporter"]))
        .row_highlight_style(Style::new().reversed())
        .highlight_symbol(">>");

    let text_widths = [
        Length(25), //Timestamp
        Length(25), //Reporter
        Min(10),    //Text
    ];

    let text_table = Table::new(rows, text_widths)
        .block(Block::new().title("Table"))
        .header(Row::new(vec!["Time", "Reporter", "Text"]))
        .row_highlight_style(Style::new().reversed())
        .highlight_symbol(">>");

    let mut app = AppState {
        text_table,
        text_table_state,
        table,
        selected_heading: None,
        table_state,
        row_count: 0, //TODO: Do we need this field?
        user,
        comms,
        popup: Popup::None,
        diagnostics: false,
        recorder,
        inspector: RatatuiInspector::new(),
    };


    loop {

        terminal.draw(|frame|{
            if app.diagnostics {
                app.inspector.draw(frame, &app.recorder, &app.comms);
            } else {
                draw(frame, &mut app).expect("rendering should not fail");
            }


            match &mut app.popup {
                Popup::None => {}
                Popup::AddHeading(text) => {
                    let area = popup_area(frame.area(), 75);
                    frame.render_widget(Clear, area); //this clears out the background
                    frame.render_widget(&*text, area);
                }
                Popup::AddText(text) => {
                    let area = popup_area(frame.area(), 75);
                    frame.render_widget(Clear, area); //this clears out the background
                    frame.render_widget(&*text, area);
                }
                Popup::Rename(old,new) => {
                    let area = popup_area(frame.area(), 75);
                    frame.render_widget(Clear, area); //this clears out the background
                    frame.render_widget(&*new, area);
                }
            }

        })?;

        if poll_input(&mut app)? {
            break;
        }
    }
    Ok(())
}

/// Render the application. This is where you would draw the application UI. This example draws a
/// greeting.
fn draw(frame: &mut Frame, app: &mut AppState) -> Result<()> {


    app.selected_heading = None;

    let rows: Vec<Row> = app.comms.with_root(|root|{
        let mut rows = Vec::new();
        for (k,v) in root.issues.iter() {
            rows.push((v.created.get(), k.detach(), v.reporter.detach()));
        }
        rows.sort();
        for (idx, item) in rows.iter().enumerate() {
            if Some(idx) == app.table_state.selected() {
                app.selected_heading = Some(item.1.to_string());
            }
        }
        app.row_count = rows.len();
        rows.into_iter().map(|(time,heading,count)|Row::new([
            time.to_string(), heading, count.to_string()
        ])).collect()
    });

    let message_count = app.comms.count_messages();

    //TODO: Document the `blocking` methods (and other methods) on comms
    let sync_status = app.comms.get_status_blocking().map(|x|
        x.to_string()
    ).unwrap_or_else(|_|"Failed".to_string());

    let mut debug_spans = vec![
        ratatui::prelude::Span::styled("User: ", Style::default()),
        ratatui::prelude::Span::styled(app.user.to_string(), Style::default().bold()),
        ratatui::prelude::Span::styled(", Message count: ", Style::default()),
        ratatui::prelude::Span::styled(message_count.to_string(), Style::default().bold()),
        ratatui::prelude::Span::styled(", Sync status: ", Style::default()),
        ratatui::prelude::Span::styled(sync_status.to_string(), Style::default().bold()),
    ];
    for (key, val) in app.recorder.metrics_items() {
        debug_spans.push(ratatui::prelude::Span::styled(format!(", {}: ", key), Style::default()));
        debug_spans.push(ratatui::prelude::Span::styled(val, Style::default().bold()));
    }

    let metrics_paragraph = Paragraph::new(Line::from(
        debug_spans)).wrap(Wrap {
        trim: true
    });



    let text_rows: Vec<Row> = app.comms.with_root(|root|{
        let mut rows = Vec::new();
        if let Some(selected_heading) = &app.selected_heading {
            if let Some(item) = root.issues.get(selected_heading) {
                for text in item.description.iter() {
                    rows.push((text.time.to_string(), text.added_by.to_string(), text.text.detach()));
                }
            }
        }
        rows.sort();
        rows.into_iter().map(|(time,heading,count)|Row::new([
            time.to_string(), heading, count.to_string()
        ])).collect()
    });

    let main_status_layout = Layout::vertical([
        Length(3), //Keybinds
        Min(0), // List
        Length((2+metrics_paragraph.line_count(frame.area().width-2) as u16).min(20)), //Status
    ]);

    let [keybinds_area, list_area, status_area] = main_status_layout.areas(frame.area());

    let list_details_layout = Layout::horizontal([
        Percentage(50),
        Percentage(50),
    ]);

    let [list_area, details_area] = list_details_layout.areas(list_area);

    let details_layout = Layout::vertical([
        Length(4), // Time/Heading
        Min(4), // Texts
    ]);

    let [time_heading_area, descriptions_label_area] = details_layout.areas(details_area);

    if let Some(selected_row) = &app.selected_heading {
        let issue_block = Block::new().borders(Borders::ALL).title("Issue");

        if let Some(issue) = app.comms.with_root(|root| {
            root.issues.get(selected_row).map(|x|x.detach())
        }) {

            frame.render_widget(
                Paragraph::new(
                    vec![
                        ratatui::prelude::Line::from(
                            vec![
                                Span::styled("Timestamp: ", Style::default().bold()),
                                Span::styled(issue.created.to_string(), Style::default()),
                            ]),
                        ratatui::prelude::Line::from(
                            vec![
                                Span::styled("Heading:   ", Style::default().bold()),
                                Span::styled(selected_row, Style::default()),
                            ]),
                    ]).block(issue_block),
                time_heading_area);

        }
    }

    let issue_block = Block::new()
        .borders(Borders::ALL)
        .title(format!("Issues"));

    let text_block = Block::new()
        .borders(Borders::ALL)
        .title(format!("Texts"));

    let status_block = Block::new()
        .borders(Borders::ALL)
        .title(format!("Status"));

    let keybinds_block = Block::new()
        .borders(Borders::ALL)
        .title(format!("Keys"));


    frame.render_widget(
        Paragraph::new(Line::from(

            compile_error!("Consider if F2 rename is a good thing to show off. It doesn't really work, doing it this way, since we don't track causality. A real app should use surrogate keys.")
                ratatui::prelude::Span::styled("A - Add entry, DEL - Delete entry, F2 - Rename entry, T - Add text, D - Diagnostics", Style::default()),
            )).block(keybinds_block),
        keybinds_area);


    frame.render_widget(
        metrics_paragraph.block(status_block),
        status_area);



    let table = app.table.clone().rows(rows);
    frame.render_stateful_widget(table.block(issue_block), list_area, &mut &mut app.table_state);

    if app.selected_heading.is_some() {
        let text_table = app.text_table.clone().rows(text_rows);
        frame.render_stateful_widget(text_table.block(text_block), descriptions_label_area, &mut &mut app.text_table_state);
    }

    Ok(())
}

fn popup_area(area: Rect, percent_x: u16) -> Rect {
    let vertical = Layout::vertical([Constraint::Length(3)]).flex(Flex::Center);
    let horizontal = Layout::horizontal([Constraint::Percentage(percent_x)]).flex(Flex::Center);
    let [area] = vertical.areas(area);
    let [area] = horizontal.areas(area);
    area
}

/// Check if the user has pressed 'q'. This is where you would handle events. This example just
/// checks if the user has pressed 'q' and returns true if they have. It does not handle any other
/// events. There is a 250ms timeout on the event poll to ensure that the terminal is rendered at
/// least once every 250ms. This allows you to do other work in the application loop, such as
/// updating the application state, without blocking the event loop for too long.
fn poll_input(app: &mut AppState) -> Result<bool> {
    if event::poll(Duration::from_millis(250)).context("event poll failed")? {
        let event = event::read().context("event read failed")?;
        if app.diagnostics {
            app.inspector.input(&event);
        }
        match event {
            input if app.popup.is_some() => {


                match &mut app.popup {
                    Popup::None => {}
                    Popup::AddHeading(w) |
                    Popup::Rename(_,w) |
                    Popup::AddText(w) => {

                        match &input {
                            Event::Key(KeyEvent{code: KeyCode::Esc,..}) => {
                                app.popup = Popup::None;
                                return Ok(false);
                            }
                            Event::Key(KeyEvent{code: KeyCode::Enter,..}) => {
                                match &mut app.popup {
                                    Popup::AddHeading(w) => {
                                        let heading: String = w.lines()[0].to_string();
                                        app.create_event(&heading)?;
                                    }
                                    Popup::AddText(w) => {
                                        let text: String = w.lines()[0].to_string();
                                        app.add_text(&text)?;
                                    }
                                    Popup::Rename(old, new) => {
                                        let old = old.to_string();
                                        let text: String = new.lines()[0].to_string();
                                        app.rename(old, text)?;
                                    }
                                    Popup::None => {}
                                }
                                app.popup = Popup::None;
                                return Ok(false);
                            }
                            _ => {}
                        }

                        w.input(input);
                    }
                }
            }
            Event::Key(key) => {
                match key.code {
                    KeyCode::Esc => {
                        if app.diagnostics {
                            app.diagnostics = false;
                        } else {
                            return Ok(true);
                        }
                    }
                    KeyCode::Char('q') => {
                        return Ok(true);
                    }
                    KeyCode::Delete => {
                        if let Some(heading) = &app.selected_heading {
                            let heading = heading.to_string();
                            app.delete_event(&heading)?;
                            return Ok(false);
                        }
                    }
                    KeyCode::F(2) => {
                        if let Some(heading) = &app.selected_heading {
                            let mut text = TextArea::default();
                            text.set_block(Block::new().borders(Borders::ALL).title("New heading"));
                            app.popup = Popup::Rename(heading.to_string(), text);
                        }
                    }
                    KeyCode::Char('a') => {
                        let mut text = TextArea::default();
                        text.set_block(Block::new().borders(Borders::ALL).title("Enter heading"));
                        app.popup = Popup::AddHeading(text);
                    }
                    KeyCode::Char('t') => {
                        let mut text = TextArea::default();
                        text.set_block(Block::new().borders(Borders::ALL).title("Enter text"));
                        app.popup = Popup::AddText(text);
                    }
                    KeyCode::Char('d') => {
                        app.diagnostics = !app.diagnostics;
                        app.popup = Popup::None;
                    }
                    KeyCode::Up => {
                        let next = app.table_state.selected().unwrap_or(0).saturating_sub(1);
                        let next = next.clamp(0, app.row_count.saturating_sub(1));
                        app.table_state.select(Some(next));
                    }
                    KeyCode::Down => {
                        let next = app.table_state.selected().unwrap_or(0)+1;
                        let next = next.clamp(0, app.row_count.saturating_sub(1));
                        app.table_state.select(Some(next));
                    }
                    _ => {}
                }
                return Ok(KeyCode::Char('q') == key.code);
            }
            _ => {}
        }
    }
    Ok(false)
}

