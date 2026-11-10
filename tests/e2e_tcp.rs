#![cfg(unix)]

use async_trait::async_trait;
use socks6::{
    Backend, BackendError, Capabilities, Client, ConnectContext, ConnectProgress, Datagram,
    DatagramChannel, Endpoint, IdentityOffer, IdentitySelection, Server, SocketBackend,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::oneshot;

struct ProgressBackend {
    delay: Duration,
}

#[async_trait]
impl Backend for ProgressBackend {
    fn capabilities(&self) -> Capabilities {
        SocketBackend.capabilities()
    }

    fn identity_offers(&self) -> Result<Vec<IdentityOffer>, BackendError> {
        SocketBackend.identity_offers()
    }

    async fn connect_stream(
        &self,
        identity: &IdentityOffer,
        target: Endpoint,
    ) -> Result<socks6::BoxStream, BackendError> {
        SocketBackend.connect_stream(identity, target).await
    }

    async fn connect_stream_with_context(
        &self,
        identity: &IdentityOffer,
        target: Endpoint,
        context: ConnectContext,
    ) -> Result<socks6::BoxStream, BackendError> {
        context
            .report_progress(
                ConnectProgress::new("establishing_route").with_detail("waiting for the route"),
            )
            .await
            .map_err(|_| BackendError::Unavailable("progress receiver closed".to_owned()))?;
        tokio::time::sleep(self.delay).await;
        SocketBackend.connect_stream(identity, target).await
    }

    async fn open_datagrams(
        &self,
        identity: &IdentityOffer,
    ) -> Result<Box<dyn DatagramChannel>, BackendError> {
        SocketBackend.open_datagrams(identity).await
    }
}

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

#[tokio::test]
async fn client_receives_backend_progress_before_stream_ready() {
    let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_task = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.unwrap();
        let mut request = [0; 4];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(&request).await.unwrap();
    });

    let socket_path = std::env::temp_dir().join(format!(
        "socks6-progress-{}-{}.sock",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = Arc::new(Server::new(ProgressBackend {
        delay: Duration::from_millis(20),
    }));
    let server_task = tokio::spawn(server.run(listener));
    let client = Client::connect_unix(&socket_path).await.unwrap();
    let mut progress = Vec::new();
    let mut stream = client
        .connect_stream_with_progress(
            Endpoint::Socket(target_addr),
            IdentitySelection::Any,
            |event| progress.push(event),
        )
        .await
        .unwrap();
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].phase, "establishing_route");
    assert_eq!(progress[0].detail.as_deref(), Some("waiting for the route"));

    stream.write_all(b"ping").await.unwrap();
    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"ping");
    target_task.await.unwrap();

    server_task.abort();
    fs::remove_file(socket_path).unwrap();
}

#[tokio::test]
async fn timed_out_connection_returns_retry_metadata_only_after_failure() {
    let socket_path = std::env::temp_dir().join(format!(
        "socks6-timeout-{}-{}.sock",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = Arc::new(
        Server::new(ProgressBackend {
            delay: Duration::from_secs(5),
        })
        .with_connect_timeout(Duration::from_millis(50))
        .unwrap(),
    );
    let server_task = tokio::spawn(server.run(listener));
    let client = Client::connect_unix(&socket_path).await.unwrap();
    let mut progress = Vec::new();
    let error = match client
        .connect_stream_with_progress(
            Endpoint::Domain("127.0.0.1".to_owned(), 9),
            IdentitySelection::Any,
            |event| progress.push(event),
        )
        .await
    {
        Ok(_) => panic!("connection should time out"),
        Err(error) => error,
    };
    assert_eq!(progress.len(), 1);
    let socks6::Error::ConnectFailed {
        code, retry_after, ..
    } = error
    else {
        panic!("expected structured connection failure");
    };
    assert_eq!(code, socks6::ErrorCode::Unavailable);
    assert_eq!(retry_after, Some(Duration::from_secs(1)));

    server_task.abort();
    fs::remove_file(socket_path).unwrap();
}

struct CancelBackend(std::sync::Mutex<Option<oneshot::Sender<()>>>);

struct NotifyOnDrop(Option<oneshot::Sender<()>>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[async_trait]
impl Backend for CancelBackend {
    fn capabilities(&self) -> Capabilities {
        SocketBackend.capabilities()
    }

    fn identity_offers(&self) -> Result<Vec<IdentityOffer>, BackendError> {
        SocketBackend.identity_offers()
    }

    async fn connect_stream(
        &self,
        _identity: &IdentityOffer,
        _target: Endpoint,
    ) -> Result<socks6::BoxStream, BackendError> {
        Err(BackendError::Internal(
            "context-aware connection hook was not used".to_owned(),
        ))
    }

    async fn connect_stream_with_context(
        &self,
        _identity: &IdentityOffer,
        _target: Endpoint,
        context: ConnectContext,
    ) -> Result<socks6::BoxStream, BackendError> {
        context
            .report_progress(ConnectProgress::new("establishing_route"))
            .await
            .map_err(|_| BackendError::Unavailable("progress receiver closed".to_owned()))?;
        let _notify_on_drop = NotifyOnDrop(self.0.lock().unwrap().take());
        std::future::pending().await
    }

    async fn open_datagrams(
        &self,
        identity: &IdentityOffer,
    ) -> Result<Box<dyn DatagramChannel>, BackendError> {
        SocketBackend.open_datagrams(identity).await
    }
}

#[tokio::test]
async fn dropping_a_pending_client_request_drops_the_backend_attempt() {
    let socket_path = std::env::temp_dir().join(format!(
        "socks6-cancel-{}-{}.sock",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let server = Arc::new(Server::new(CancelBackend(std::sync::Mutex::new(Some(
        cancelled_tx,
    )))));
    let server_task = tokio::spawn(server.run(listener));
    let client = Client::connect_unix(&socket_path).await.unwrap();
    let (progress_tx, mut progress_rx) = oneshot::channel();
    let mut progress_tx = Some(progress_tx);
    let mut attempt = Box::pin(client.connect_stream_with_progress(
        Endpoint::Domain("unused.test".to_owned(), 80),
        IdentitySelection::Any,
        move |_| {
            if let Some(sender) = progress_tx.take() {
                let _ = sender.send(());
            }
        },
    ));
    tokio::select! {
        _ = &mut attempt => panic!("connection unexpectedly completed"),
        result = &mut progress_rx => result.expect("progress callback was not invoked"),
    }
    drop(attempt);
    tokio::time::timeout(Duration::from_secs(2), cancelled_rx)
        .await
        .expect("backend attempt was not dropped")
        .expect("backend drop notification was lost");

    server_task.abort();
    fs::remove_file(socket_path).unwrap();
}
