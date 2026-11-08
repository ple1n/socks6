# SOCKS6

SOCKS6 is an independent Rust crate and experimental wire protocol for
requesting TCP streams and UDP associations from a proxy backend. It does not
depend on nsproxy, Linux namespaces, or a particular network transport.

The crate provides:

- A versioned, length-framed protocol that can run over TCP, Unix-domain
streams, or another reliable byte stream.
- A generic `Backend` interface for TCP connects, UDP associations, endpoint
capabilities, and service status.
- A `Client` API for opening TCP streams and bidirectional UDP associations.
- TCP and Unix-domain server listeners.
- A direct-socket backend for examples and protocol tests.

A backend adapter implements `Backend`, keeping the protocol reusable and
letting each backend advertise only the endpoint and operation types it
actually supports.

## Example

```rust,no_run
use socks6::{Client, Endpoint};
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxy = Client::connect("127.0.0.1:1086".parse::<SocketAddr>()?).await?;
    let mut stream = proxy
        .connect_tcp(Endpoint::Domain("example.com".to_owned(), 443))
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
    let proxy = Client::connect_unix("/run/example-socks6/control.sock").await?;
    println!("{:?}", proxy.query_capabilities().await?);
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
