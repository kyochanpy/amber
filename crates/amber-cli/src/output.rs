use amber_core::SessionStatus;
use chrono::{DateTime, SecondsFormat, Utc};

use crate::commands::list::SessionListEntry;

pub fn render_session_list(entries: &[SessionListEntry]) -> String {
    if entries.is_empty() {
        return "No sessions found.\n".to_owned();
    }

    let mut lines = vec![format!(
        "{:<36}  {:<20}  {:<20}  {:<11}  {:>7}  {:<11}  {:<7}",
        "SESSION ID", "STARTED AT", "ENDED AT", "STATUS", "STREAMS", "PENDING_WAL", "PARQUET"
    )];

    for entry in entries {
        lines.push(format!(
            "{:<36}  {:<20}  {:<20}  {:<11}  {:>7}  {:<11}  {:<7}",
            entry.manifest.session_id,
            format_timestamp(entry.manifest.started_at),
            entry
                .manifest
                .ended_at
                .map(format_timestamp)
                .unwrap_or_else(|| "-".to_owned()),
            format_status(entry.manifest.status),
            entry.manifest.observed_streams.len(),
            yes_no(entry.has_pending_wal),
            yes_no(entry.has_committed_parquet),
        ));
    }

    lines.join("\n") + "\n"
}

pub fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn format_status(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Open => "open",
        SessionStatus::Closed => "closed",
        SessionStatus::Interrupted => "interrupted",
    }
}

pub fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
