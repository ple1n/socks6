use bytes::Bytes;
use socks6::{Client, Endpoint};
use std::env;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:1086".to_string())
        .parse()?;

    println!("Connecting to SOCKS6 proxy at {}...", server_addr);
    let mut client = Client::connect(server_addr).await?;

    // 1. Ping
    client.ping().await?;
    println!("Ping successful");

    // 2. Discover available capabilities & endpoints
    let capabilities = client.query_capabilities().await?;
    println!("Server advertised {} capabilities:", capabilities.len());
    for (i, ep) in capabilities.iter().enumerate() {
        println!("  [{}] {:?}", i, ep);
    }

    // 3. Allocate an inbound binding (NAT-capable public endpoint)
    let session = client.bind(None).await?;
    println!(
        "Allocated binding ID: {}, Assigned endpoint: {:?}",
        session.binding_id, session.assigned_endpoint
    );

    // 4. If target address provided as 2nd argument, send a test packet
    if let Some(target_str) = env::args().nth(2) {
        let target_addr: SocketAddr = target_str.parse()?;
        let target_ep = Endpoint::Socket(target_addr);
        println!("Sending test datagram to {:?}...", target_ep);
        session
            .send_to(&client, target_ep, Bytes::from_static(b"hello from socks6"))
            .await?;
        println!("Datagram sent!");
    }

    // 5. Inbound listener loop
    println!("Listening for inbound packets (Ctrl+C to stop)...");
    while let Some(packet) = client.recv_from().await {
        println!(
            "Received {} bytes from {:?} on binding {}: {:?}",
            packet.payload.len(),
            packet.peer_endpoint,
            packet.binding_id,
            String::from_utf8_lossy(&packet.payload)
        );
    }

    Ok(())
}
