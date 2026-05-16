use amber_node::run;
use tracing::{Level, error};

#[tokio::main]
async fn main() {
    init_tracing();

    if let Err(error) = run().await {
        error!(error = %error, "amber-node startup failed");
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .try_init();
}
