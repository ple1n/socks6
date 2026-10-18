use binrw::{io::Cursor, BinRead, BinWrite};
use bytes::Bytes;
use socks6::{
    endpoint::Endpoint,
    frame::{Frame, Packet},
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

fn endpoint_samples() -> Vec<Endpoint> {
    vec![
        Endpoint::Socket(SocketAddr::from(([192, 0, 2, 1], 1080))),
        Endpoint::Socket(SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1], 443))),
        Endpoint::Ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2))),
        Endpoint::Ip(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        Endpoint::Lokinet([0x5a; 32]),
        Endpoint::Domain("example.test".to_owned(), 8443),
    ]
}

#[test]
fn endpoint_variants_round_trip() {
    for endpoint in endpoint_samples() {
        let encoded = endpoint.to_bytes().unwrap();
        assert_eq!(Endpoint::from_bytes(&encoded).unwrap(), endpoint);
    }
}

#[test]
fn frame_variants_round_trip() {
    let packet = Packet {
        binding_id: 0x1234_5678,
        peer_endpoint: endpoint_samples()[0].clone(),
        payload: Bytes::from_static(b"payload"),
    };
    let frames = vec![
        Frame::QueryCapabilities,
        Frame::Capabilities {
            endpoints: endpoint_samples(),
        },
        Frame::BindRequest { desired: None },
        Frame::BindRequest {
            desired: Some(Endpoint::Domain("bind.example".to_owned(), 5353)),
        },
        Frame::BindResponse {
            binding_id: 42,
            assigned_endpoint: Endpoint::Socket(SocketAddr::from(([203, 0, 113, 5], 9000))),
        },
        Frame::Packet(packet),
        Frame::Ping,
        Frame::Pong,
    ];

    for frame in frames {
        let encoded = frame.to_bytes().unwrap();
        assert_eq!(Frame::from_bytes(&encoded).unwrap(), frame);
    }
}

#[test]
fn packet_round_trip() {
    let packet = Packet {
        binding_id: 0xaabb_ccdd,
        peer_endpoint: Endpoint::Domain("peer.example".to_owned(), 1234),
        payload: Bytes::from_static(&[0, 1, 2, 0xff]),
    };
    let mut cursor = Cursor::new(Vec::new());
    packet.write_be(&mut cursor).unwrap();

    let mut cursor = Cursor::new(cursor.into_inner());
    assert_eq!(Packet::read_be(&mut cursor).unwrap(), packet);
}

#[test]
fn truncated_and_invalid_codec_inputs_return_errors() {
    assert!(Endpoint::from_bytes(&[0x01, 192, 0, 2]).is_err());
    assert!(Endpoint::from_bytes(&[0xff]).is_err());
    assert!(Endpoint::from_bytes(&[0x06, 0x01, 0xff, 0x00, 0x50]).is_err());

    assert!(Frame::from_bytes(&[]).is_err());
    assert!(Frame::from_bytes(&[0xff]).is_err());
    assert!(Frame::from_bytes(&[0x02, 0]).is_err());
}
