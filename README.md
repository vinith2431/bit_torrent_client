# BitTorrent Client in Go

A lightweight BitTorrent client written in Go. This project implements the core parts of the BitTorrent protocol, including torrent parsing, tracker communication, peer discovery, peer handshakes, bitfield processing, piece downloading, and SHA-1 integrity verification.

## Features

* Parse `.torrent` files using Bencode
* Extract torrent metadata
* Calculate the torrent info hash
* Split and process piece hashes
* Communicate with HTTP trackers
* Discover peers from tracker responses
* Support compact and dictionary-style peer responses
* Establish TCP connections with peers
* Perform the BitTorrent handshake
* Receive and process peer bitfields
* Handle `have`, `choke`, `unchoke`, and `piece` messages
* Request file pieces from peers
* Download pieces concurrently using goroutines
* Verify downloaded pieces using SHA-1
* Assemble verified pieces into the output file
* Debug logging for torrent, tracker, peer, and piece operations

## How BitTorrent Downloading Works

The client follows this general workflow:

```text
              .torrent file
                    |
                    v
            Parse torrent metadata
                    |
                    v
              Calculate info hash
                    |
                    v
             Contact tracker
                    |
                    v
              Receive peers
                    |
                    v
            Connect to a peer
                    |
                    v
          BitTorrent handshake
                    |
                    v
              Receive bitfield
                    |
                    v
          Check piece availability
                    |
                    v
            Send interested
                    |
                    v
              Get unchoked
                    |
                    v
            Request pieces
                    |
                    v
             Receive blocks
                    |
                    v
             SHA-1 verification
                    |
                    v
              Write output file
```

## Project Structure

```text
torrent-client/
│
├── main.go
├── torrent.go
├── p2p.go
├── message.go
├── handshake.go
├── piece.go
├── debug.go
│
├── go.mod
├── go.sum
├── test.torrent
└── README.md
```

The exact filenames may vary depending on the current implementation.

### Torrent Parsing

The torrent parser reads the `.torrent` file and extracts information such as:

* Tracker URL
* File name
* File size
* Piece length
* Piece hashes
* Info hash

The torrent metadata is encoded using **Bencode**.

### Tracker Communication

The client sends an HTTP request to the tracker containing parameters such as:

```text
info_hash
peer_id
port
uploaded
downloaded
left
compact
numwant
```

The tracker returns information about available peers.

The client supports:

* Compact peer responses
* Dictionary-style peer responses

### Peer Discovery

A peer is represented by an IPv4 address and port:

```go
type peer struct {
    ip   [4]byte
    port uint16
}
```

The client converts the tracker response into a list of peers and attempts to establish TCP connections.

## BitTorrent Handshake

After connecting to a peer, the client performs the BitTorrent handshake.

The handshake allows both sides to exchange:

* Protocol identifier
* Reserved bytes
* Info hash
* Peer ID

The info hash identifies the torrent being requested.

## Bitfield

A peer's bitfield indicates which pieces of the file that peer has.

For example:

```text
10110010
```

Each bit represents a piece:

```text
1 → peer has the piece
0 → peer does not have the piece
```

The client checks this information before requesting a piece.

`have` messages can also update the peer's available pieces during a connection.

## Piece Downloading

The torrent is divided into pieces.

Each piece is divided into smaller blocks when requesting data from a peer.

A request contains:

```text
Piece Index
Block Offset
Block Length
```

These values are encoded using **big-endian byte order**, as required by the BitTorrent wire protocol.

Multiple pieces can be processed concurrently using Go goroutines and channels.

## Piece Verification

Every piece has a SHA-1 hash stored in the torrent metadata.

After downloading a piece:

```text
Downloaded piece
       |
       v
Calculate SHA-1
       |
       v
Compare with expected hash
       |
    +--+--+
    |     |
  Match  Fail
    |     |
 Accept  Retry
```

This prevents corrupted or incomplete pieces from being accepted.

## Concurrency

The client uses Go concurrency primitives to download pieces from peers.

The general structure is:

```text
              Piece Work Channel
                     |
        +------------+------------+
        |            |            |
        v            v            v
     Worker 1     Worker 2     Worker 3
        |            |            |
        +------------+------------+
                     |
                     v
              Results Channel
```

Workers receive piece work, check whether the peer has the requested piece, download it, verify its integrity, and return the result.

## Debugging

The project includes debug logging for troubleshooting the BitTorrent protocol.

Examples include:

```text
[STEP] TCP connection established
[STEP] BitTorrent handshake completed
[DEBUG] PIECE WORK
>>> CHECKING BITFIELD
>>> PEER DOES NOT HAVE PIECE
```

Debugging was particularly useful for identifying issues involving:

* Tracker responses
* Peer connections
* Handshake communication
* Bitfields
* Piece availability
* Piece requests
* Worker/channel behavior

## Important Protocol Details

### Network Byte Order

BitTorrent uses network byte order (big-endian) for integer fields in protocol messages.

For example:

```go
binary.BigEndian.PutUint32(...)
```

is used when constructing piece requests.

### Request Backlog

The client keeps track of outstanding block requests.

When a `piece` message is received, the number of outstanding requests is reduced so additional blocks can be requested.

### Bitfield Size

The bitfield is sized according to the total number of pieces in the torrent rather than the current number of pieces remaining in the work queue.

### Bounds Checking

Piece indexes received from peers are checked before modifying the bitfield to prevent invalid indexes from causing runtime panics.

## Running the Client

Make sure Go is installed.

Check the Go version:

```bash
go version
```

Download dependencies:

```bash
go mod download
```

Run the client:

```bash
go run . <torrent-file> <output-file>
```

Example:

```bash
go run . test.torrent download.iso
```

## Building

To create an executable:

```bash
go build -o torrent-client
```

On Windows:

```powershell
go build -o torrent-client.exe
```

Then:

```powershell
.\torrent-client.exe test.torrent download.iso
```

## Current Limitations

This implementation is primarily designed for learning and experimentation with the BitTorrent protocol.

Current limitations include:

* Primarily focused on single-file torrents
* HTTP tracker support
* UDP trackers are not currently handled
* Multi-file torrents are not currently supported
* Peer availability depends on the peers returned by the tracker
* More advanced peer management can be added
* Tracker re-announcing can be improved
* Robust recovery when all available peers fail can be improved

## Technologies Used

* **Go**
* **TCP/IP**
* **HTTP**
* **Bencode**
* **BitTorrent Protocol**
* **SHA-1**
* **Goroutines**
* **Channels**

## Learning Goals

This project was built to understand how BitTorrent works internally rather than treating it as a black-box application.

The main concepts explored are:

* Peer-to-peer networking
* Binary network protocols
* TCP connections
* HTTP tracker communication
* Bencode encoding and decoding
* SHA-1 hashing
* Bitfields
* Concurrent programming
* Goroutines
* Channels
* Network debugging
* Piece-based file transfer

## License

This project is intended for educational and research purposes.
