# SOCKS6 protocol

Version 1 is an experimental, backend-neutral protocol for requesting TCP
streams and UDP associations from a proxy service. The Rust crate is the
reference implementation; this document defines the wire format independently
of that implementation.

SOCKS6 is not tied to nsproxy, Lokinet, a particular operating system, or one
transport. A server may expose the protocol over any reliable byte stream. The
reference crate supports TCP and Unix-domain streams.

## Design contract

- **One connection, one operation.** Each control connection opens one TCP
  stream, one UDP association, or one short-lived status/ping request. Flow
  identity is the transport connection, so version 1 needs no request IDs or
  global flow table.
- **TCP stays a stream.** After a successful TCP-open response, the same
  transport connection carries unframed, bidirectional bytes. Closing either
  side's write half propagates as a TCP half-close.
- **UDP stays datagrams.** A UDP association uses SOCKS6 frames for each
  datagram, preserving message boundaries and the peer endpoint.
- **Backends own network semantics.** Endpoint resolution, routing, binding,
  and reachability belong to the backend. Capabilities describe what that
  backend actually implements.
- **Disconnect releases resources.** EOF or `Close` tears down the operation
  and its backend resources.

Version 1 does not define NAT discovery, hole punching, a public mapping
service, raw IP packet injection, authentication, or unreliable transport.
Those are separate extensions, not implied by endpoint encoding.

## Framing

Every control message is sent as:

```text
u32 body_length (big-endian)
byte[body_length] body
```

The body begins with:

```text
byte[2] magic = "S6"
u16 protocol_version (big-endian)
u8 opcode
opcode-specific fields
```

Version 1 uses the field encodings below. Integers are unsigned and
big-endian. Strings are UTF-8 preceded by a `u16` byte length. Byte strings
are preceded by a `u32` length. Booleans are one byte (`0` or `1`). Optional
values are a one-byte presence flag followed by the value when present.
Decoders reject truncated frames, unknown tags, invalid UTF-8, and trailing
bytes.

The maximum body size is 1 MiB. A peer advertises its limit in the handshake;
the negotiated limit is the smaller value. UDP payloads are limited to 65,507
bytes and must also fit in the frame limit.

## Handshake and capabilities

The client sends `Hello` (opcode `1`) with version in the header and
`u32 max_frame_size`.

The server replies with `Welcome` (opcode `2`) with the selected version in
the header, then:

```text
u32 max_frame_size
u8 tcp_connect
u8 udp_associate
u8 udp_bind
u32 max_datagram_size
string server_name
u16 endpoint_kind_count
u8[endpoint_kind_count] endpoint_kinds
```

Endpoint-kind tags are: socket `1`, IP `2`, domain `3`, service `4`.
Capabilities are descriptive; a request using an unsupported operation or
endpoint receives an `Error`.

## Operations

After `Welcome`, the client sends exactly one operation request.

### TCP connect

`OpenTcp` (opcode `3`) carries one endpoint. On success the server sends
`TcpReady` (opcode `4`) containing an optional source endpoint. From then on,
the connection carries raw TCP stream bytes until either side closes. If
connection setup fails, the server sends `Error` instead.

### UDP association

`OpenUdp` (opcode `5`) contains an optional requested local endpoint. The
server responds with `UdpReady` (opcode `6`) and an optional bound endpoint.
An absent bound endpoint means the backend does not expose a local address.

Each `Datagram` (opcode `7`) contains:

```text
Endpoint endpoint
u32 payload_length
byte[payload_length] payload
```

Client-to-server `endpoint` is the destination. Server-to-client `endpoint`
is the source. The client may send to multiple destinations through one
association. A backend may reject that behavior if it only supports a
destination-specific association. `Close` (opcode `12`) or EOF ends the
association.

### Status and keepalive

`StatusRequest` (opcode `8`) receives `Status` (opcode `9`), containing a
connected flag and optional detail string. `Ping` (opcode `10`) receives
`Pong` (opcode `11`).

### Errors

`Error` (opcode `13`) contains an error-code byte and a string message. Codes
are invalid request `1`, unsupported `2`, unavailable `3`, limit exceeded
`4`, internal `5`, and version mismatch `6`. The server closes the operation
after an error unless the error occurred during the initial handshake.

## Endpoint encoding

Each endpoint starts with a one-byte tag:

| Tag | Endpoint | Fields |
|---|---|---|
| `1` | IPv4 socket | 4 address bytes, `u16` port |
| `2` | IPv6 socket | 16 address bytes, `u16` port, `u32` flow info, `u32` scope ID |
| `3` | IPv4 address | 4 address bytes |
| `4` | IPv6 address | 16 address bytes |
| `5` | Domain socket | string host, `u16` port |
| `6` | Service endpoint | string namespace, `u32`-length opaque address, optional `u16` port |

Service endpoints let independent backends represent namespaced identifiers
without adding backend-specific variants to the core crate. For example, an
overlay backend may use a `Service` endpoint or a domain in its own namespace.
The namespace and address are opaque to the protocol.

## Security and deployment

The protocol itself does not authenticate peers. Local deployments must use
transport-level access control: filesystem permissions and peer credentials
for Unix sockets, or an authenticated secure transport for remote listeners.
Do not expose a TCP listener to an untrusted network without adding an
authentication and encryption profile.

The reference crate bounds frame and datagram sizes and uses bounded socket
resources per operation. Deployments should additionally set connection and
per-peer resource limits appropriate to their backend.
