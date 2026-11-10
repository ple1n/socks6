# SOCKS6

SOCKS6 is an independent Rust crate and experimental wire protocol for
negotiating proxy identities and leasing their capabilities. Clients can use
leases to request reliable streams and unreliable datagrams through a proxy.
The protocol does not depend on nsproxy, Linux namespaces, or a particular
network backend.

The crate provides:

- A versioned, length-framed protocol that can run over TCP, Unix-domain
streams, or another reliable byte stream.
- Identity offers that describe the addresses and send/receive capabilities a
backend can actually provide.
- Explicit identity leases, which may be reused across requests and released
when no longer needed.
- A persistent identity negotiation session that can refresh offers and
acquire or release leases repeatedly; data operations use separate carrier
connections and can reuse those leases.
- A generic `Backend` interface for reliable streams, unreliable datagrams,
identity capabilities, and service status. Stream setup receives a deadline,
cancellation signal, and optional backend-defined progress reporting.
- A `Client` API for querying offers, leasing identities, and opening streams
or datagram channels with a selected lease, an offered identity, or a
server-selected identity. Stream clients may opt into connection-progress
callbacks while the server is still establishing the requested connection.
- TCP and Unix-domain server listeners.
- A direct-socket backend for examples and protocol tests.

Here, **stream** and **datagram** describe application-level behavior: a stream
is reliable and ordered; datagrams are discrete messages that may be lost or
reordered. Those semantics do not require the backend or carrier to use TCP or
UDP.

A backend adapter implements `Backend`, keeping the protocol reusable and
letting each backend advertise only the identities and capabilities it
actually supports. See [SPEC.md](SPEC.md) for the version-4 wire contract.

## Example

```rust,no_run
use socks6::{Client, Endpoint};
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxy = Client::connect("127.0.0.1:1086".parse::<SocketAddr>()?).await?;
    let mut stream = proxy
        .connect_stream(
            Endpoint::Domain("example.com".to_owned(), 443),
            socks6::IdentitySelection::Any,
        )
        .await?;
    stream.write_all(b"request").await?;
    Ok(())
}
```

Run the reference direct-socket server and client with:

```sh
cargo run --bin socks6_server
cargo run --bin socks6_client
```

The server defaults to `127.0.0.1:1086`. This reference backend opens ordinary
system TCP and UDP sockets.

On Unix, the same client API can use a local IPC socket instead:

```rust,no_run
use socks6::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxy = Client::connect_unix("/run/example-socks6/proxy.socks6").await?;
    println!("{:?}", proxy.query_identities().await?);
    Ok(())
}
```

SOCKS6 is an experimental protocol, not a claim of compatibility with another
SOCKS6 specification. See [SPEC.md](SPEC.md) for the wire contract and
non-goals.

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

## License

Licensed under the MIT license. See [LICENSE](LICENSE).
