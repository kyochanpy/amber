use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "amber", version, about = "Amber command-line tools")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Compact(CompactArgs),
    Inspect(InspectArgs),
    List(ListArgs),
}

#[derive(Debug, Args)]
pub struct CompactArgs {
    #[arg(long, default_value = "amber.yaml")]
    pub config: PathBuf,
    #[arg(long)]
    pub cleanup: bool,
}

#[derive(Debug, Args)]
pub struct ListArgs {
    #[arg(long, default_value = "amber.yaml")]
    pub config: PathBuf,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    #[arg(long)]
    pub latest: bool,
    #[arg(long)]
    pub limit: Option<usize>,
    #[arg(long)]
    pub tag: Option<String>,
}

#[derive(Debug, Args)]
pub struct InspectArgs {
    #[arg(value_name = "SESSION_SELECTOR")]
    pub selector: Option<String>,
    #[arg(long, default_value = "amber.yaml")]
    pub config: PathBuf,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    #[arg(long, conflicts_with = "selector")]
    pub session: Option<String>,
    #[arg(long)]
    pub node: Option<String>,
    #[arg(long)]
    pub output: Option<String>,
    #[arg(long)]
    pub rerun: bool,
    #[arg(long)]
    pub blueprint: Option<PathBuf>,
}
