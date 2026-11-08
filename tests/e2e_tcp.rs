#![cfg(unix)]

use socks6::{Client, Endpoint, Server, SocketBackend};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};

#[tokio::test]
async fn tcp_stream_relay_works_over_unix_ipc() {
    let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_task = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        stream.write_all(&request).await.unwrap();
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
    let mut stream = client
        .connect_tcp(Endpoint::Socket(target_addr))
        .await
        .unwrap();
    stream.write_all(b"hello over SOCKS6").await.unwrap();
    stream.shutdown().await.unwrap();

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("timed out waiting for TCP response")
        .unwrap();
    assert_eq!(response, b"hello over SOCKS6");

    target_task.await.unwrap();
    server_task.abort();
    fs::remove_file(socket_path).unwrap();
}
