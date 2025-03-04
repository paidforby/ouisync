mod client;
mod geo_ip;
mod handler;
mod metrics;
mod options;
mod protocol;
mod repository;
mod server;
mod state;
mod transport;
mod utils;

use anyhow::Result;
use clap::Parser;
use options::Options;
use protocol::Request;

pub(crate) const APP_NAME: &str = "ouisync";
pub(crate) const DB_EXTENSION: &str = "ouisyncdb";

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::parse();

    if let Request::Start {
        log_format,
        log_color,
    } = &options.request
    {
        server::run(options.dirs, options.socket, *log_format, *log_color).await
    } else {
        client::run(options.dirs, options.socket, options.request).await
    }
}
