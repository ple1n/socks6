use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::frame::{Frame, Packet};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Debug)]
enum ControlResponse {
    Capabilities(Vec<Endpoint>),
    BindResponse {
        binding_id: u32,
        assigned_endpoint: Endpoint,
    },
    Pong,
}

pub struct Client {
    writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    control_tx: Arc<Mutex<Option<oneshot::Sender<ControlResponse>>>>,
    inbound_rx: mpsc::Receiver<Packet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSession {
    pub binding_id: u32,
    pub assigned_endpoint: Endpoint,
}

impl BoundSession {
    /// Send packet originating from this binding to a target endpoint
    pub async fn send_to(&self, client: &Client, target: Endpoint, payload: Bytes) -> Result<()> {
        client.send_to(self.binding_id, target, payload).await
    }
}

impl Client {
    /// Connect to a SOCKS6 server
    pub async fn connect(server_addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(server_addr).await?;
        let (mut reader, writer) = stream.into_split();
        let writer = Arc::new(Mutex::new(writer));

        let control_tx: Arc<Mutex<Option<oneshot::Sender<ControlResponse>>>> =
            Arc::new(Mutex::new(None));
        let control_tx_clone = control_tx.clone();

        let (inbound_tx, inbound_rx) = mpsc::channel::<Packet>(100);

        tokio::spawn(async move {
            loop {
                match Frame::read_from(&mut reader).await {
                    Ok(Frame::Packet(pkt)) => {
                        if inbound_tx.send(pkt).await.is_err() {
                            break;
                        }
                    }
                    Ok(Frame::Capabilities { endpoints }) => {
                        let mut slot = control_tx_clone.lock().await;
                        if let Some(sender) = slot.take() {
                            let _ = sender.send(ControlResponse::Capabilities(endpoints));
                        }
                    }
                    Ok(Frame::BindResponse {
                        binding_id,
                        assigned_endpoint,
                    }) => {
                        let mut slot = control_tx_clone.lock().await;
                        if let Some(sender) = slot.take() {
                            let _ = sender.send(ControlResponse::BindResponse {
                                binding_id,
                                assigned_endpoint,
                            });
                        }
                    }
                    Ok(Frame::Pong) => {
                        let mut slot = control_tx_clone.lock().await;
                        if let Some(sender) = slot.take() {
                            let _ = sender.send(ControlResponse::Pong);
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            writer,
            control_tx,
            inbound_rx,
        })
    }

    /// Query available capabilities and endpoints supported by server
    pub async fn query_capabilities(&self) -> Result<Vec<Endpoint>> {
        let (tx, rx) = oneshot::channel();
        {
            let mut slot = self.control_tx.lock().await;
            *slot = Some(tx);
        }
        {
            let mut w = self.writer.lock().await;
            Frame::QueryCapabilities.write_to(&mut *w).await?;
        }
        match rx.await.map_err(|_| Error::ConnectionClosed)? {
            ControlResponse::Capabilities(eps) => Ok(eps),
            other => Err(Error::Protocol(format!("Unexpected response: {:?}", other))),
        }
    }

    /// Request a public ingress binding on the active connection
    pub async fn bind(&self, desired: Option<Endpoint>) -> Result<BoundSession> {
        let (tx, rx) = oneshot::channel();
        {
            let mut slot = self.control_tx.lock().await;
            *slot = Some(tx);
        }
        {
            let mut w = self.writer.lock().await;
            Frame::BindRequest { desired }.write_to(&mut *w).await?;
        }
        match rx.await.map_err(|_| Error::ConnectionClosed)? {
            ControlResponse::BindResponse {
                binding_id,
                assigned_endpoint,
            } => Ok(BoundSession {
                binding_id,
                assigned_endpoint,
            }),
            other => Err(Error::Protocol(format!("Unexpected response: {:?}", other))),
        }
    }

    /// Send ping to server and await pong
    pub async fn ping(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut slot = self.control_tx.lock().await;
            *slot = Some(tx);
        }
        {
            let mut w = self.writer.lock().await;
            Frame::Ping.write_to(&mut *w).await?;
        }
        match rx.await.map_err(|_| Error::ConnectionClosed)? {
            ControlResponse::Pong => Ok(()),
            other => Err(Error::Protocol(format!("Unexpected response: {:?}", other))),
        }
    }

    /// Send packet through a binding to target endpoint
    pub async fn send_to(&self, binding_id: u32, target: Endpoint, payload: Bytes) -> Result<()> {
        let frame = Frame::Packet(Packet {
            binding_id,
            peer_endpoint: target,
            payload,
        });
        let mut w = self.writer.lock().await;
        frame.write_to(&mut *w).await
    }

    /// Receive next inbound packet
    pub async fn recv_from(&mut self) -> Option<Packet> {
        self.inbound_rx.recv().await
    }
}
