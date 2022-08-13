//! Shared utilities
//!
use tracing_subscriber::EnvFilter;

pub fn setup_logging() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // Setup from the environment (RUST_LOG)
        .with_env_filter(EnvFilter::from_default_env())
        // this needs to be set to false, otherwise ANSI color codes will
        // show up in a confusing manner in CloudWatch logs.
        .with_ansi(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();
}
