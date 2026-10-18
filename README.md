# socks6

An experimental Rust library and pair of command-line programs implementing a
framed client/server control protocol with UDP packet forwarding.

The crate currently provides:

- Async client and server APIs built on Tokio.
- Length-prefixed TCP frames for capability queries, UDP binding requests,
  packet forwarding, and ping/pong.
- Endpoint and frame binary codecs.
- `socks6_client` and `socks6_server` example programs.

This is an early prototype, not a complete SOCKS6 or NAT-traversal
implementation. The server currently forwards only socket endpoints and binds
UDP sockets locally; it does not discover or allocate public NAT mappings.
Other endpoint variants are represented by the codec but are not implemented
for packet forwarding. See [SPEC.md](SPEC.md) for the draft design.

## Library usage

```rust,no_run
use bytes::Bytes;
use socks6::{Client, Endpoint};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server: SocketAddr = "127.0.0.1:1086".parse()?;
    let client = Client::connect(server).await?;
    let binding = client.bind(None).await?;
    let target = Endpoint::Socket("127.0.0.1:9000".parse()?);
    binding
        .send_to(&client, target, Bytes::from_static(b"hello"))
        .await?;
    Ok(())
}
```

The client and server programs can be started with:

```sh
cargo run --bin socks6_server
cargo run --bin socks6_client
```

Both default to the control address `127.0.0.1:1086`. Pass a different address
as the first argument to either program.

## License

Licensed under the MIT license. See [LICENSE](LICENSE).
