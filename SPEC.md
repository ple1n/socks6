# SOCKS6: Minimal Public Unreliable Packet Proxy Protocol
*Informal Architecture & Protocol Specification (Draft)*

## 1. Scope & Objective

**socks6** is an independent, minimal, public protocol designed to establish an application-to-application interface for **NAT-capable, unreliable packet proxying**.

Key principles:
1. **Completely Standalone:** No dependencies on `nsp3`, Linux kernel namespaces, or proprietary proxy backends.
2. **UDP-Analog Programming Model:** Works naturally like a datagram interface (`send_to` / `recv_from`) without forcing connection state or reliable stream overhead onto the wire.
3. **Peer Transport Agnostic:** The protocol does not mandate or implement any specific network transport (unlike WireGuard, OpenVPN, or custom tunneling overlays). socks6 defines strictly a clean local wire format and framing contract between application and proxy daemon. How packets transit over wide-area networks or tunnels is left entirely to VPN vendors, overlay daemons, or custom proxy implementations.
4. **Minimal Overhead at a Lower Layer:** Operates at a low layer directly on unreliable packets/datagrams. Eliminates unnecessary protocol layering, stream reassembly, and connection-tracking bloat, providing near-zero-copy framing.
5. **NAT Traversal & Inbound Reachability:** Designed specifically so applications behind NAT can acquire public/routable presence, enable peer-to-peer NAT hole-punching to remote hosts (e.g. UDP NAT traversal), and receive inbound unsolicited/solicited packets.
6. **General Packet / Protocol Agnostic Endpoints:** While UDP is a primary, first-class use case, endpoints are abstract network identifiers. The protocol natively extends to arbitrary network/transport layer addressing (bare IP packets, overlay networks like Lokinet, etc.).

---

## 2. Core Concepts: Endpoints & Bindings

### 2.1 What is an Endpoint?
An **Endpoint** is a typed address descriptor recognized mutually by the client application and the socks6 proxy daemon.

Endpoints describe destination targets or local source identities:
- **`UDP Socket Endpoint` (`IpAddr:Port`):** Standard IPv4/IPv6 socket target (e.g. `198.51.100.10:51820`).
- **`Bare IP Endpoint` (`IpAddr` / `CIDR`):** Raw L3 network endpoints for tunneling or routing raw IP packets (where L4 protocols such as ICMP, WireGuard, ESP, or custom protocols are encapsulated within the packet payload).
- **`Overlay / Cryptographic Endpoint` (e.g., `Lokinet Addr`, `.loki` / public key):** Decentralized onion-routing / overlay network identities.
- **`Domain Endpoint` (`FQDN:Port`):** Name-based endpoint deferred for remote resolution by the proxy.

### 2.2 Endpoint Advertisement (`Available Endpoints`)
Upon session initialization, the proxy **informs the client of available upstream endpoints and supported endpoint types**.
- The proxy advertises its capabilities (e.g., supported address families, overlay network bindings, allocated relay interfaces).
- The client learns what network realms it can route into and what external addresses the proxy can allocate on its behalf.

### 2.3 Bindings & Inbound Ingress
To support NAT traversal, an endpoint is not just an egress gateway—it is an **inbound binding**:
- The proxy opens or assigns an ingress interface/port on an advertised endpoint family.
- The proxy returns the allocated **External Endpoint Identifier** to the client.
- The client shares this external endpoint with peers (e.g. via signaling, out-of-band rendezvous, or STUN-style discovery) to facilitate direct NAT punching or relaying.

---

## 3. Protocol Operation

The interaction between application and proxy follows a lightweight four-phase lifecycle:

### Phase 1: Handshake & Capability Discovery
1. The client connects to the socks6 control channel (local IPC, UNIX domain socket, or TCP/UDP socket).
2. The proxy advertises its **Available Endpoints**:
   - Supported address types (IPv4, IPv6, Bare IP, Lokinet, etc.).
   - Available egress and ingress interfaces.
   - NAT capability flags (Full Cone, Restricted, Symmetric relaying, raw socket support).

### Phase 2: Ingress Allocation / Binding (Optional for egress-only)
1. The client requests a binding on a specified endpoint type:
   - *Example:* "Allocate an inbound UDP-capable endpoint" or "Bind Lokinet address".
2. The proxy allocates the binding and returns the **Public / External Endpoint** (`allocated_endpoint`).
3. The proxy configures its NAT table to associate incoming datagrams received on this binding back to the client's session.

### Phase 3: Packet Exchange (`send_to` / `recv_from`)
Once established, packet transfer is framed with minimal encapsulation:

```text
+-----------------------+-----------------------+-----------------------+
|  Source Endpoint ID   |  Target Endpoint ID   |     Payload Data      |
|     (or Flags)        |   (Type + Address)    |       (Binary)        |
+-----------------------+-----------------------+-----------------------+
```

- **Egress (`send_to`):**
  - Application supplies: `from_endpoint` (optional/default), `target_endpoint`, and raw payload.
  - Proxy transmits the packet out to the network through the corresponding physical/overlay interface.
  - For NAT hole-punching: Outbound packets prime the NAT state on both the proxy and any intermediate firewalls.

- **Ingress (`recv_from`):**
  - Remote peers transmit packets directed to the proxy's advertised binding.
  - Proxy demultiplexes the packet using the flow table / binding ID.
  - Proxy forwards the packet to the client along with the sender's origin `from_endpoint`.

### Phase 4: Teardown & Keep-alive
- Unreliable sessions maintain soft-state timers.
- Keep-alive pings can be emitted if needed to maintain NAT bindings across stateful firewalls.
- Explicit session closure releases all associated proxy sockets, NAT pins, and virtual addresses immediately.

---

## 4. NAT Traversal & Hole-Punching Mechanics

The primary motivator for socks6 over older protocols is high-performance, low-friction UDP-analog NAT traversal:
1. **Predictable Port / Address Allocation:**
   When the client requests an inbound association, the proxy can report the exact external mapped endpoint, eliminating guesswork.
2. **Hairpinning & Demultiplexing:**
   The proxy handles multiple incoming flows toward the same allocated endpoint and demultiplexes them to the client based on peer endpoint identity.
3. **Direct Peer Punching:**
   Because the client is in full control of `send_to(target)` and is explicitly told its public mapping, standard hole-punching routines (STUN/ICE primitives) run cleanly through the proxy without needing TUN kernel drivers on the host.

---

## 5. Extensibility & Future Wire Format

- **Binary Wire Encoding:** Framing can be implemented over shared memory, UNIX datagram sockets, or lightweight binary packet envelopes over UDP/TCP.
- **Bare IP & Layer 3:** When bare IP endpoints are selected, the payload is treated as a complete IP packet (`ip_hdr + l4_hdr + payload`), allowing socks6 to act as an unprivileged, user-space virtual network interface.
- **Cryptographic Routing:** Endpoints can directly reference cryptographic identifiers (such as Lokinet ed25519 public keys) without forcing the client application to bundle custom overlay network clients into its own process space.

