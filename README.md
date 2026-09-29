# BitTorrent Client in Go

A modular, high-performance BitTorrent client written in Go featuring dual-transport support (**TCP** and **μTP / Micro Transport Protocol** over UDP), multi-tracker discovery (HTTP, UDP BEP 15, and Local Service Discovery BEP 14), intelligent peer scoring, download resume support, adaptive retransmission timeouts, and congestion/flow control.

---

## Architecture Overview

```text
                     .torrent File / Magnet
                                │
                                ▼
                       Bencode & Metadata
                                │
                                ▼
              Tracker Discovery & LSD Multicast
              (HTTP, UDP BEP 15, LSD BEP 14)
                                │
                                ▼
                           Peer Manager
             (Deduplicate, Score, Backoff, Ban)
                                │
                                ▼
                  dial_with_fallback(peer)
                                │
             ┌──────────────────┴──────────────────┐
             ▼                                     ▼
      TCP Transport                          μTP Transport
      (net.DialTimeout)                     (UDP / BEP 29)
             │                                     │
             └──────────────────┬──────────────────┘
                                │
                                ▼
                       peer_conn Interface
                                │
                                ▼
                     BitTorrent Wire Protocol
                (Handshake, Bitfield, Messages)
                                │
                                ▼
                         Piece Scheduler
                  (Availability, Request Queue)
                                │
                                ▼
                       SHA-1 Verification
                                │
                                ▼
                   Disk Storage & Resume State
```

The BitTorrent protocol layer operates strictly against the `peer_conn` interface, completely unaware of whether the underlying link is TCP or μTP.

---

## Features

### 1. Dual Transport Layer
* **TCP Transport**: Standard TCP socket connections with timeout handling.
* **μTP (Micro Transport Protocol / BEP 29)**: Complete user-space reliable transport protocol over UDP:
  * 16-bit sequence numbers with modular wraparound math.
  * In-order packet delivery and out-of-order reassembly buffer.
  * Retransmission timer loop with configurable retry limits.
  * **Adaptive RTO** via Jacobson's algorithm (calculating Smoothed RTT and RTT Variation) with Karn's algorithm protecting against retransmission sample bias.
  * **Flow Control** with dynamic receiver buffer advertising and sender throttling.
  * **Congestion Control** with Slow Start (exponential window growth) and Congestion Avoidance (Additive Increase / Multiplicative Decrease upon loss).
  * **Graceful Shutdown** via `ST_FIN` and abnormal termination via `ST_RESET`.
  * **Automatic Fallback**: Attempts μTP first and gracefully degrades to TCP if the peer does not support μTP.

### 2. Peer Discovery
* **HTTP Trackers**: Standard HTTP GET announce with compact and binary dictionary response parsing.
* **UDP Trackers (BEP 15)**: Binary connection handshake, transaction ID verification, and announce scraping.
* **Announce Lists (BEP 12)**: Tiered multi-tracker failover.
* **Local Service Discovery (BEP 14)**: Multicast `BT-SEARCH` discovery on `239.192.152.143:6771` across all active network interfaces.

### 3. Peer Management & Scoring
* **Peer Table**: Unifies peers from all discovery sources without duplicate dials.
* **Adaptive Scoring**: Weighted scoring (`50% speed + 20% RTT + 30% reliability`).
* **Failure Handling**: Exponential backoff (30s, 60s, 90s up to 10m) and automatic banning after 3 corrupt piece deliveries.

### 4. Integrity, Download Resumption & Verification
* **SHA-1 Piece Hashing**: Rigorous piece validation preventing bad blocks from reaching disk.
* **Resume Support**: Generates `.resume` state and verifies existing `.part` blocks upon startup to avoid redownloading verified pieces.

---

## μTP State Machine & Packet Format

### State Machine

```text
              ┌───────────────┐
              │     CLOSED    │
              └───────┬───────┘
                      │ dial (sends ST_SYN)
                      ▼
              ┌───────────────┐
              │   SYN_SENT    │
              └───────┬───────┘
                      │ receives ST_STATE
                      ▼
              ┌───────────────┐
              │  ESTABLISHED  │
              └───────┬───────┘
                      │
          ┌───────────┴───────────┐
          │                       │
     sends/recvs DATA         sends ST_FIN
          │                       │
          ▼                       ▼
      TRANSFER                 CLOSING
                                  │
                                  ▼
                               CLOSED
```

### Packet Handling Summary

| Event | Action Taken |
|---|---|
| **SYN Lost** | Handshake read deadline expires; client drops connection or triggers TCP fallback. |
| **STATE Lost** | Retransmission loop re-sends unacknowledged packet. |
| **DATA Lost** | RTO expires; `checkRetransmit()` sends packet again, decreases `cwnd` (loss signal). |
| **ACK Lost** | Sender RTO expires and retransmits; cumulative ACK handles subsequent packets. |
| **Duplicate DATA** | Detected via serial difference; receiver generates ACK and discards payload. |
| **Out-of-Order DATA** | Buffered in `recvBuffer`; ACK is returned; flushed once missing gap arrives. |
| **Sequence Wrap (65535 → 0)** | 16-bit serial arithmetic handles continuous delivery across 0 boundary. |
| **Peer Sends RESET** | Connection transitions to closed immediately; pending calls return reset error. |
| **Peer Sends FIN** | Connection acknowledges FIN, drains remaining read buffer, returns `io.EOF`. |

---

## Adaptive RTO & Congestion Control

### Jacobson's Algorithm (RTT Estimation)

For each valid sample $R$ measured from non-retransmitted segments:

$$\text{Error} = R - \text{SRTT}$$
$$\text{RTTVAR} \leftarrow (1 - \beta) \cdot \text{RTTVAR} + \beta \cdot |\text{Error}| \quad (\beta = 0.25)$$
$$\text{SRTT} \leftarrow (1 - \alpha) \cdot \text{SRTT} + \alpha \cdot R \quad (\alpha = 0.125)$$
$$\text{RTO} \leftarrow \text{clamp}(\text{SRTT} + 4 \cdot \text{RTTVAR}, \text{minRTO}, \text{maxRTO})$$

### Congestion Window Dynamics

* **Slow Start ($cwnd < ssthresh$)**: $cwnd \leftarrow cwnd + \text{bytesAcked}$
* **Congestion Avoidance ($cwnd \ge ssthresh$)**: $cwnd \leftarrow cwnd + \frac{\text{bytesAcked} \cdot \text{MSS}}{cwnd}$
* **Packet Loss (RTO timeout)**: $ssthresh \leftarrow \max\left(\frac{cwnd}{2}, minCwnd\right)$, $cwnd \leftarrow minCwnd$

---

## Testing & Validation Suite

The test suite covers unit tests, integration tests, network impairment proxies, and benchmarks:

```bash
# Run all tests
go test -v ./...

# Run transport & fallback tests
go test -v -run "TestTransport|TestDialWithFallback"

# Run μTP unit & correctness tests
go test -v -run "TestSeqWrap|TestOrder|TestMalformed|TestShutdown|TestGoroutineLeak"

# Run impairment & network condition tests
go test -v -run "TestPacketLoss|TestLatency|TestFlowControl|TestCongestion"

# Run benchmarks
go test -bench "Benchmark" -benchmem
```

### Benchmark Results (Loopback Transfer)

| Metric | TCP | μTP |
|---|---|---|
| **Loopback Throughput** | ~70.0 MB/s | ~58.3 MB/s |
| **Packet Marshal** | — | 304 ns/op |
| **Packet Unmarshal** | — | 226 ns/op |
| **ACK Processing (1000 items)** | — | 104 μs |

---

## Building & Running

### Build

```bash
go build -o torrent-client.exe .
```

### Usage

```bash
# Download a file via torrent
.\torrent-client.exe test.torrent download.iso
```

---

## License

This project is created for educational, research, and high-performance networking exploration.
