use bytes::Bytes;
use socks6::{Endpoint, Server};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::{TcpListener, UdpSocket};

#[tokio::test]
async fn client_sends_and_receives_udp_datagrams() {
    let capabilities = vec![Endpoint::Socket(SocketAddr::from(([127, 0, 0, 1], 1080)))];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let server = Arc::new(Server::new(server_addr, capabilities.clone()));
    tokio::spawn(async move {
        server.run(listener).await.unwrap();
    });

    let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let remote_addr = remote.local_addr().unwrap();
    let mut client = socks6::Client::connect(server_addr).await.unwrap();

    assert_eq!(client.query_capabilities().await.unwrap(), capabilities);
    let session = client.bind(None).await.unwrap();

    let payload = Bytes::from_static(b"udp integration test");
    session
        .send_to(&client, Endpoint::Socket(remote_addr), payload.clone())
        .await
        .unwrap();

    let mut buf = [0; 128];
    let (len, sender_addr) =
        tokio::time::timeout(Duration::from_secs(5), remote.recv_from(&mut buf))
            .await
            .expect("timed out waiting for datagram from SOCKS6 server")
            .unwrap();
    assert_eq!(&buf[..len], payload.as_ref());

    remote.send_to(b"echo reply", sender_addr).await.unwrap();

    let packet = tokio::time::timeout(Duration::from_secs(5), client.recv_from())
        .await
        .expect("timed out waiting for echoed datagram")
        .expect("SOCKS6 client connection closed");
    assert_eq!(packet.binding_id, session.binding_id);
    assert_eq!(packet.peer_endpoint, Endpoint::Socket(remote_addr));
    assert_eq!(packet.payload, Bytes::from_static(b"echo reply"));
}
