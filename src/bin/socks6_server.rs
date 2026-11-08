use socks6::{Server, SocketBackend};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let address: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1086".to_owned())
        .parse()?;
    let listener = TcpListener::bind(address).await?;
    let server = Arc::new(Server::new(SocketBackend));
    tracing::info!(%address, "SOCKS6 test server listening");
    server.run(listener).await?;
    Ok(())
}
