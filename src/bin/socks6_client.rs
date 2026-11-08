use socks6::{Client, Endpoint};
use std::env;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let address: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1086".to_owned())
        .parse()?;
    let client = Client::connect(address).await?;
    let capabilities = client.query_capabilities().await?;
    println!("SOCKS6 capabilities: {capabilities:?}");
    println!("service status: {:?}", client.query_status().await?);
    client.ping().await?;
    println!("ping successful");

    let Some(target) = env::args().nth(2) else {
        return Ok(());
    };
    let target: SocketAddr = target.parse()?;
    let udp = client.open_udp(None).await?;
    println!("UDP association bound at {:?}", udp.bound_endpoint());
    udp.send_to(Endpoint::Socket(target), b"hello from socks6".to_vec())
        .await?;
    let reply = tokio::time::timeout(std::time::Duration::from_secs(5), udp.recv_from()).await??;
    println!(
        "received {} bytes from {}",
        reply.payload.len(),
        reply.endpoint
    );
    udp.close().await?;

    let mut tcp = client.connect_tcp(Endpoint::Socket(target)).await?;
    tcp.write_all(b"hello from socks6").await?;
    tcp.shutdown().await?;
    let mut response = Vec::new();
    tcp.read_to_end(&mut response).await?;
    println!("received {} TCP bytes", response.len());
    Ok(())
}
