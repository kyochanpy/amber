pub mod compact;
pub mod inspect;
pub mod list;

use anyhow::Result;

use crate::cli::{Cli, Command};
use crate::output::render_session_list;

pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Compact(args) => {
            let summary = compact::run_compact(&args).await?;

            if summary.compacted_segments == 0 && summary.deleted_segments > 0 {
                println!(
                    "No new WAL segments required compaction. Deleted {} previously compacted WAL segment(s).",
                    summary.deleted_segments
                );
            } else if summary.compacted_segments == 0 {
                println!("No eligible closed WAL segments found for compaction.");
            } else {
                println!(
                    "Compacted {} WAL segment(s) into {} Parquet file(s).",
                    summary.compacted_segments, summary.created_parquet_files
                );
            }

            if args.cleanup && summary.compacted_segments > 0 {
                if summary.deleted_segments == 0 {
                    println!("No compacted WAL segments required cleanup.");
                } else {
                    println!(
                        "Deleted {} compacted WAL segment(s).",
                        summary.deleted_segments
                    );
                }
            }
        }
        Command::Inspect(args) => {
            inspect::run_inspect(&args).await?;
        }
        Command::List(args) => {
            let sessions = list::run_list(&args).await?;
            print!("{}", render_session_list(&sessions));
        }
    }

    Ok(())
}
