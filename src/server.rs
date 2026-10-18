use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::frame::{Frame, Packet};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

struct BindingState {
    binding_id: u32,
    udp_socket: Arc<UdpSocket>,
}

pub struct Server {
    listen_addr: SocketAddr,
    capabilities: Vec<Endpoint>,
    bindings: Arc<RwLock<HashMap<u32, BindingState>>>,
    next_binding_id: AtomicU32,
}

impl Server {
    pub fn new(listen_addr: SocketAddr, capabilities: Vec<Endpoint>) -> Self {
        Self {
            listen_addr,
            capabilities,
            bindings: Arc::new(RwLock::new(HashMap::new())),
            next_binding_id: AtomicU32::new(1),
        }
    }

    pub async fn run(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        info!("SOCKS6 server listening on {}", self.listen_addr);
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            debug!("New SOCKS6 control connection from {}", peer_addr);
            let srv = self.clone();
            tokio::spawn(async move {
                if let Err(e) = srv.handle_client(stream).await {
                    debug!("Client {} error or disconnected: {}", peer_addr, e);
                }
            });
        }
    }

    async fn handle_client(self: Arc<Self>, stream: TcpStream) -> Result<()> {
        let (mut reader, mut writer) = stream.into_split();
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Frame>(100);
        let mut client_binding_ids = Vec::new();

        let write_task = async {
            while let Some(frame) = outgoing_rx.recv().await {
                frame.write_to(&mut writer).await?;
            }
            Ok::<(), Error>(())
        };

        let srv = self.clone();
        let read_task = async {
            loop {
                let frame = match Frame::read_from(&mut reader).await {
                    Ok(f) => f,
                    Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => return Err(e),
                };

                match frame {
                    Frame::QueryCapabilities => {
                        let resp = Frame::Capabilities {
                            endpoints: srv.capabilities.clone(),
                        };
                        outgoing_tx.send(resp).await.map_err(|_| Error::ConnectionClosed)?;
                    }
                    Frame::BindRequest { desired } => {
                        let (binding_id, assigned_endpoint, _udp_sock) = srv
                            .allocate_binding(desired, outgoing_tx.clone())
                            .await?;
                        client_binding_ids.push(binding_id);

                        let resp = Frame::BindResponse {
                            binding_id,
                            assigned_endpoint,
                        };
                        outgoing_tx.send(resp).await.map_err(|_| Error::ConnectionClosed)?;
                    }
                    Frame::Packet(pkt) => {
                        srv.forward_egress(pkt).await?;
                    }
                    Frame::Ping => {
                        outgoing_tx.send(Frame::Pong).await.map_err(|_| Error::ConnectionClosed)?;
                    }
                    Frame::Pong => {}
                    _ => {}
                }
            }
            Ok::<(), Error>(())
        };

        tokio::select! {
            r = read_task => { let _ = r; },
            w = write_task => { let _ = w; },
        };

        // Cleanup bindings
        let mut bindings = self.bindings.write().await;
        for id in client_binding_ids {
            bindings.remove(&id);
        }

        Ok(())
    }

    async fn allocate_binding(
        &self,
        desired: Option<Endpoint>,
        outgoing_tx: mpsc::Sender<Frame>,
    ) -> Result<(u32, Endpoint, Arc<UdpSocket>)> {
        let binding_id = self.next_binding_id.fetch_add(1, Ordering::Relaxed);

        let bind_target = match desired {
            Some(Endpoint::Socket(addr)) => addr,
            _ => "0.0.0.0:0".parse().unwrap(),
        };

        let udp = UdpSocket::bind(bind_target).await?;
        let local_addr = udp.local_addr()?;
        let assigned_endpoint = Endpoint::Socket(local_addr);
        let udp_arc = Arc::new(udp);

        let state = BindingState {
            binding_id,
            udp_socket: udp_arc.clone(),
        };

        self.bindings.write().await.insert(binding_id, state);

        // Spawn background listener for this UDP socket
        let s_udp = udp_arc.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 65535];
            loop {
                match s_udp.recv_from(&mut buf).await {
                    Ok((n, peer)) => {
                        let pkt = Packet {
                            binding_id,
                            peer_endpoint: Endpoint::Socket(peer),
                            payload: Bytes::copy_from_slice(&buf[..n]),
                        };
                        if outgoing_tx.send(Frame::Packet(pkt)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Ok((binding_id, assigned_endpoint, udp_arc))
    }

    async fn forward_egress(&self, pkt: Packet) -> Result<()> {
        let bindings = self.bindings.read().await;
        if let Some(state) = bindings.get(&pkt.binding_id) {
            match pkt.peer_endpoint {
                Endpoint::Socket(peer_addr) => {
                    state.udp_socket.send_to(&pkt.payload, peer_addr).await?;
                }
                _ => {
                    return Err(Error::UnsupportedEndpoint(format!("{:?}", pkt.peer_endpoint)));
                }
            }
        }
        Ok(())
    }
}
