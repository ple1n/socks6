#![cfg(unix)]

use socks6::{Client, Endpoint, IdentitySelection, Server, SocketBackend};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};

#[tokio::test]
async fn tcp_stream_relay_works_over_unix_ipc() {
    let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_task = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = target_listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            stream.write_all(&request).await.unwrap();
        }
    });

    let socket_path = std::env::temp_dir().join(format!(
        "socks6-test-{}-{}.sock",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = Arc::new(Server::new(SocketBackend));
    let server_task = tokio::spawn(server.run(listener));

    let client = Client::connect_unix(&socket_path).await.unwrap();
    let mut negotiation = client.identity_session().await.unwrap();
    assert_eq!(negotiation.identities().len(), 1);
    let identities = negotiation.refresh_identities().await.unwrap().to_vec();
    assert_eq!(identities.len(), 1);
    let lease = negotiation
        .lease_identity(Some(identities[0].id.clone()))
        .await
        .unwrap();
    let mut stream = negotiation
        .connect_stream(
            Endpoint::Socket(target_addr),
            IdentitySelection::Lease(lease.id.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        stream.identity().lease_id.as_deref(),
        Some(lease.id.as_str())
    );
    stream.write_all(b"first stream").await.unwrap();
    stream.shutdown().await.unwrap();

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("timed out waiting for stream response")
        .unwrap();
    assert_eq!(response, b"first stream");

    let mut second = negotiation
        .connect_stream(
            Endpoint::Socket(target_addr),
            IdentitySelection::Lease(lease.id.clone()),
        )
        .await
        .unwrap();
    second.write_all(b"reused lease").await.unwrap();
    second.shutdown().await.unwrap();
    response.clear();
    tokio::time::timeout(Duration::from_secs(5), second.read_to_end(&mut response))
        .await
        .expect("timed out waiting for stream response using a reused lease")
        .unwrap();
    assert_eq!(response, b"reused lease");

    target_task.await.unwrap();
    negotiation.release_identity(&lease).await.unwrap();
    negotiation.close().await.unwrap();
    assert!(client
        .connect_stream(
            Endpoint::Socket(target_addr),
            IdentitySelection::Lease(lease.id.clone()),
        )
        .await
        .is_err());
    server_task.abort();
    fs::remove_file(socket_path).unwrap();
}
