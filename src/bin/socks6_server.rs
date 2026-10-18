use socks6::{Endpoint, Server};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let bind_addr: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1086".to_string())
        .parse()?;

    // Advertise supported endpoint formats
    let capabilities = vec![
        Endpoint::Socket("0.0.0.0:0".parse()?),
        Endpoint::Ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        Endpoint::Lokinet([0u8; 32]),
        Endpoint::Domain("example.loki".to_string(), 0),
    ];

    let server = Arc::new(Server::new(bind_addr, capabilities));
    let listener = TcpListener::bind(bind_addr).await?;
    println!("SOCKS6 server running on {}", bind_addr);

    server.run(listener).await?;
    Ok(())
}
