use amber_cli::{cli::Cli, commands::run};
use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli).await
}
