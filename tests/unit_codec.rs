use socks6::{
    Capabilities, Endpoint, EndpointKind, ErrorCode, Frame, ServiceStatus, MAX_DATAGRAM_SIZE,
    MAX_FRAME_SIZE,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

fn endpoint_samples() -> Vec<Endpoint> {
    vec![
        Endpoint::Socket(SocketAddr::from(([192, 0, 2, 1], 1080))),
        Endpoint::Socket(SocketAddr::from((Ipv6Addr::LOCALHOST, 443))),
        Endpoint::Ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2))),
        Endpoint::Ip(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        Endpoint::Domain("example.test".to_owned(), 8443),
        Endpoint::Service {
            namespace: "mesh.example".to_owned(),
            address: vec![0x5a; 32],
            port: Some(9000),
        },
    ]
}

#[test]
fn endpoint_variants_round_trip() {
    for endpoint in endpoint_samples() {
        let frame = Frame::OpenTcp { target: endpoint };
        assert_eq!(
            Frame::from_bytes(&frame.to_bytes().unwrap()).unwrap(),
            frame
        );
    }
}

#[test]
fn all_protocol_frames_round_trip() {
    let frames = vec![
        Frame::Hello {
            version: 1,
            max_frame_size: MAX_FRAME_SIZE as u32,
        },
        Frame::Welcome {
            version: 1,
            max_frame_size: MAX_FRAME_SIZE as u32,
            capabilities: Capabilities {
                tcp_connect: true,
                udp_associate: true,
                udp_bind: false,
                endpoint_kinds: vec![
                    EndpointKind::Socket,
                    EndpointKind::Domain,
                    EndpointKind::Service,
                ],
                max_datagram_size: MAX_DATAGRAM_SIZE as u32,
                server_name: "test-backend".to_owned(),
            },
        },
        Frame::OpenTcp {
            target: endpoint_samples()[0].clone(),
        },
        Frame::TcpReady {
            source: Some(endpoint_samples()[1].clone()),
        },
        Frame::OpenUdp { bind: None },
        Frame::UdpReady {
            bound: Some(endpoint_samples()[0].clone()),
        },
        Frame::Datagram {
            endpoint: endpoint_samples()[4].clone(),
            payload: vec![0, 1, 2, 0xff],
        },
        Frame::StatusRequest,
        Frame::Status(ServiceStatus {
            connected: false,
            detail: Some("connecting".to_owned()),
        }),
        Frame::Ping,
        Frame::Pong,
        Frame::Close,
        Frame::Error {
            code: ErrorCode::Unavailable,
            message: "route not ready".to_owned(),
        },
    ];

    for frame in frames {
        assert_eq!(
            Frame::from_bytes(&frame.to_bytes().unwrap()).unwrap(),
            frame
        );
    }
}

#[test]
fn rejects_malformed_and_oversized_frames() {
    assert!(Frame::from_bytes(&[]).is_err());
    assert!(Frame::from_bytes(&[0xff]).is_err());
    assert!(Frame::from_bytes(&vec![0; MAX_FRAME_SIZE + 1]).is_err());
    assert!(Frame::Datagram {
        endpoint: Endpoint::Socket(SocketAddr::from(([127, 0, 0, 1], 53))),
        payload: vec![0; MAX_DATAGRAM_SIZE + 1],
    }
    .to_bytes()
    .is_err());
}
