package main

import (
	"net"
	"testing"
	"time"
)

// A peer that sends only have messages and then goes quiet (no bitfield)
// must not be treated as a failed connection.
func TestRecvBitfieldTimeoutKeepsHaves(t *testing.T) {
	ours, theirs := net.Pipe()
	defer ours.Close()
	defer theirs.Close()

	go theirs.Write(format_have(3).serialize()) // then silence

	bf, choked, err := recv_bitfield(ours, 10)
	if err != nil {
		t.Fatalf("timeout without bitfield returned error: %v", err)
	}
	if !choked || !bf.has_piece(3) || bf.has_piece(2) {
		t.Fatalf("choked=%v bitfield=%08b", choked, bf)
	}
}

// A peer that closes the connection is a real failure.
func TestRecvBitfieldClosedIsError(t *testing.T) {
	ours, theirs := net.Pipe()
	defer ours.Close()

	go func() {
		theirs.Write(format_have(1).serialize())
		theirs.Close()
	}()

	if _, _, err := recv_bitfield(ours, 10); err == nil {
		t.Fatal("closed connection was not reported as an error")
	}
}

// fake_empty_peer completes the handshake, says it has no pieces, and idles.
func fake_empty_peer(t *testing.T, info_hash [20]byte, num_pieces int) peer {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if _, err := read_handshake(conn); err != nil {
					return
				}
				var id [20]byte
				conn.Write(new_handshake(info_hash, id).serialize())
				empty := make([]byte, (num_pieces+7)/8)
				conn.Write((&message{id: msg_bitfield, payload: empty}).serialize())
				for {
					if _, err := read_message(conn); err != nil {
						return
					}
				}
			}()
		}
	}()

	addr := ln.Addr().(*net.TCPAddr)
	return peer{ip: addr.IP.To4(), port: uint16(addr.Port)}
}

// A peer with none of the pieces we need must free its slot quickly, even
// for a torrent with thousands of pieces.
func TestWorkerGivesUpQuicklyOnUselessPeer(t *testing.T) {
	const n = 4000
	var info_hash, peer_id [20]byte
	copy(info_hash[:], "useless-peer-test!!!")

	pm := new_peer_manager()
	pm.add([]peer{fake_empty_peer(t, info_hash, n)}, "test")
	pi := pm.next()

	work_ch := make(chan *piece_work, n)
	for i := 0; i < n; i++ {
		work_ch <- &piece_work{index: i, length: 1}
	}
	done := make(chan struct{})
	defer close(done)

	start := time.Now()
	got := start_download_worker(pm, pi, info_hash, peer_id, n, work_ch, make(chan *piece_result), done)
	elapsed := time.Since(start)

	if got != outcome_useless {
		t.Fatalf("outcome = %v, want outcome_useless", got)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("took %v to give up on a useless peer", elapsed)
	}
	if len(work_ch) != n {
		t.Fatalf("work queue has %d pieces, want all %d put back", len(work_ch), n)
	}
}
