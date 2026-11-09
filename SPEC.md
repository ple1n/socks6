# SOCKS6 protocol

Version 2 is an experimental, backend-neutral protocol for offering proxy
identities and leasing their capabilities. A client can use a lease for
outbound requests and for any inbound handling the offered identity supports.
Version 2 is a breaking change from version 1.

SOCKS6 is not tied to nsproxy, a particular operating system, or one network
backend. A server may expose the protocol over any reliable byte stream. The
reference crate supports TCP and Unix-domain streams.

## Semantics

The protocol's **stream** and **datagram** operations describe data semantics:
a stream is a reliable, ordered byte sequence; datagrams are discrete messages
that may be lost or reordered. They do **not** prescribe the network transport
used by the backend. A stream may be carried by a non-TCP overlay, and a
datagram service may be implemented over a reliable carrier.

## Identity negotiation

The server includes an identity-offer set in `Welcome`. A client may refresh
the set with `IdentityOffersRequest` at any time. Each offer has a stable
backend-defined ID, an optional address, and independent capabilities for
sending and receiving streams and datagrams. An absent address means the
backend can use an identity without exposing its concrete address in the
offer.

An identity address is distinct from a target endpoint:

| Tag | Identity address | Fields |
|---|---|---|
| `1` | IPv4 socket address | 4 address bytes, `u16` port |
| `2` | IPv6 socket address | 16 address bytes, `u16` port, `u32` flow info, `u32` scope ID |
| `3` | IPv4 address | 4 address bytes |
| `4` | IPv6 address | 16 address bytes |
| `5` | Domain / overlay identity | string |
| `6` | Service identity | string namespace, `u32`-length opaque address |

An address in an offer is a capability claim, not permission for the client to
spoof an arbitrary source address. For IP identities, the proxy must be able
to route or bind that address and account for any NAT or egress translation.
Inbound capability means the proxy can make incoming traffic reachable and
deliver it to the client; merely owning an address is not sufficient.

`LeaseIdentity` asks the server to lease a particular offered identity, or
lets the server choose when the ID is absent. `IdentityLeased` returns a
server-issued lease ID and the selected offer. A lease can be released with
`ReleaseIdentity`. Leases are valid across independent request connections
until released or the server exits.

An operation can select:

- an existing lease ID;
- an offered identity ID for this request; or
- `Any`, asking the server to select a suitable identity.

Offer and `Any` selections are scoped to that operation and do not create a
persistent lease. If selection or resource allocation fails, the server
returns `Error` rather than silently substituting an identity. Operation
replies include the selected offer and include a lease ID only when the request
used an existing lease.

The reference API uses a separate carrier connection for each request. The
client may keep an identity-negotiation connection open and repeat offer
queries, lease acquisitions, and releases on it. Stream and datagram
operations use separate carrier connections and can reuse leases acquired by
the negotiation connection.

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

Version 2 uses the field encodings below. Integers are unsigned and
big-endian. Strings are UTF-8 preceded by a `u16` byte length. Byte strings are
preceded by a `u32` length. Booleans are one byte (`0` or `1`). Optional
values are a one-byte presence flag followed by the value when present.
Decoders reject truncated frames, unknown tags, invalid UTF-8, and trailing
bytes.

The maximum body size is 1 MiB. A peer advertises its limit in the handshake;
the negotiated limit is the smaller value. The server offers at most 256
identities. Datagrams are limited to 65,507 bytes and must also fit in the
frame limit.

## Handshake and identity set

The client sends `Hello` (opcode `1`) with version in the header and
`u32 max_frame_size`.

The server replies with `Welcome` (opcode `2`) with the selected version in the
header, then:

```text
u32 max_frame_size
u32 max_datagram_size
string server_name
u16 identity_count
IdentityOffer[identity_count]
```

An `IdentityOffer` is encoded as:

```text
string identity_id
optional IdentityAddress
u8 send_streams
u8 send_datagrams
u8 receive_streams
u8 receive_datagrams
```

Identity-address tags are defined above. `IdentityOffersRequest` (opcode `3`)
returns a fresh `IdentityOffers` list (opcode `4`).

## Leases

`LeaseIdentity` (opcode `5`) contains an optional identity ID; absent means
the server selects from its current offers. `IdentityLeased` (opcode `6`)
contains:

```text
string lease_id
IdentityOffer identity
```

`ReleaseIdentity` (opcode `7`) contains a lease ID. `IdentityReleased`
(opcode `8`) confirms release. Releasing an unknown lease is an error.

Identity selection in stream and datagram requests is encoded as a tag:

| Tag | Selection | Fields |
|---|---|---|
| `0` | Server-selected identity | none |
| `1` | Offered identity for this request | string identity ID |
| `2` | Existing lease | string lease ID |

An `IdentityUse` in an operation response contains an `IdentityOffer` followed
by an optional lease ID.

## Operations

Each operation request uses one carrier connection. The client can open more
carrier connections and reuse the same lease.

### Reliable stream

`OpenStream` (opcode `9`) contains an identity selection and a target endpoint.
On success the server sends `StreamReady` (opcode `10`) with the identity use
and an optional source endpoint. From then on, the carrier
connection carries unframed, bidirectional stream bytes until either side
closes. If setup fails, the server sends `Error`.

`AcceptStream` (opcode `20`) contains an identity selection for an identity
whose offer has `receive_streams` enabled. The server waits for one incoming
stream and replies with `StreamAccepted` (opcode `21`), containing the effective
identity use and the incoming peer endpoint. The carrier then relays that stream until
either side closes. The client can issue another `AcceptStream` request with
the same lease to wait for another incoming stream.

### Unreliable datagrams

`OpenDatagrams` (opcode `11`) contains an identity selection. The server
responds with `DatagramsReady` (opcode `12`) containing the identity use and an
optional reachable endpoint for receiving datagrams.

Each `Datagram` (opcode `13`) contains:

```text
Endpoint endpoint
u32 payload_length
byte[payload_length] payload
```

Client-to-server endpoints are destinations; server-to-client endpoints are
sources. The client may send to multiple destinations through one datagram
channel. `Close` (opcode `18`) or EOF ends the channel.

### Status and keepalive

`StatusRequest` (opcode `14`) receives `Status` (opcode `15`), containing a
connected flag and optional detail string. `Ping` (opcode `16`) receives
`Pong` (opcode `17`).

### Errors

`Error` (opcode `19`) contains an error-code byte and a string message. Codes
are invalid request `1`, unsupported `2`, unavailable `3`, limit exceeded `4`,
internal `5`, and version mismatch `6`. The server closes the request after an
error unless it occurred during the initial handshake.

## Target endpoint encoding

Each target endpoint starts with a one-byte tag:

| Tag | Endpoint | Fields |
|---|---|---|
| `1` | IPv4 socket | 4 address bytes, `u16` port |
| `2` | IPv6 socket | 16 address bytes, `u16` port, `u32` flow info, `u32` scope ID |
| `3` | IPv4 address | 4 address bytes |
| `4` | IPv6 address | 16 address bytes |
| `5` | Domain endpoint | string host, `u16` port |
| `6` | Service endpoint | string namespace, `u32`-length opaque address, optional `u16` port |

Service endpoints let independent backends represent namespaced identifiers
without adding backend-specific variants to the core crate.

## Security and deployment

The protocol itself does not authenticate peers. Local deployments must use
transport-level access control: filesystem permissions and peer credentials
for Unix sockets, or an authenticated secure transport for remote listeners.
Do not expose a TCP listener to an untrusted network without adding an
authentication and encryption profile.

The reference crate bounds frame, identity-offer, and active-lease counts.
Deployments should additionally set connection and per-peer resource limits
appropriate to their backend.
