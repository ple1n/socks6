use socks6::{Client, Endpoint, Server, SocketBackend};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, UdpSocket};

#[tokio::test]
async fn client_sends_and_receives_udp_datagrams() {
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_listener.local_addr().unwrap();
    let server = Arc::new(Server::new(SocketBackend));
    let server_task = tokio::spawn(server.run(server_listener));

    let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let remote_addr = remote.local_addr().unwrap();
    let client = Client::connect(server_addr).await.unwrap();
    assert!(client.query_capabilities().await.unwrap().udp_associate);
    assert!(client.query_status().await.unwrap().connected);

    let association = client.open_udp(None).await.unwrap();
    assert!(association.bound_endpoint().is_some());
    let payload = b"udp integration test".to_vec();
    association
        .send_to(Endpoint::Socket(remote_addr), payload.clone())
        .await
        .unwrap();

    let mut buffer = [0; 128];
    let (size, sender) =
        tokio::time::timeout(Duration::from_secs(5), remote.recv_from(&mut buffer))
            .await
            .expect("timed out waiting for outbound datagram")
            .unwrap();
    assert_eq!(&buffer[..size], payload);
    remote.send_to(b"echo reply", sender).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), association.recv_from())
        .await
        .expect("timed out waiting for inbound datagram")
        .unwrap();
    assert_eq!(reply.endpoint, Endpoint::Socket(remote_addr));
    assert_eq!(reply.payload, b"echo reply");
    association.close().await.unwrap();
    server_task.abort();
}
