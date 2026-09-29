package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// fake_seeder serves every piece of data over the BitTorrent wire protocol.
// If corrupt is set it flips a byte in every block it sends.
func fake_seeder(t *testing.T, info_hash [20]byte, data []byte, num_pieces int, corrupt bool) peer {
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
			go serve_fake_peer(conn, info_hash, data, num_pieces, corrupt)
		}
	}()

	addr := ln.Addr().(*net.TCPAddr)
	return peer{ip: addr.IP.To4(), port: uint16(addr.Port)}
}

func serve_fake_peer(conn net.Conn, info_hash [20]byte, data []byte, num_pieces int, corrupt bool) {
	defer conn.Close()
	hs, err := read_handshake(conn)
	if err != nil || hs.info_hash != info_hash {
		return
	}
	var id [20]byte
	copy(id[:], "FAKE-SEEDER-00000000")
	conn.Write(new_handshake(info_hash, id).serialize())

	bf := make(bitfield, (num_pieces+7)/8)
	for i := 0; i < num_pieces; i++ {
		bf.set_piece(i)
	}
	conn.Write((&message{id: msg_bitfield, payload: bf}).serialize())

	for {
		msg, err := read_message(conn)
		if err != nil {
			return
		}
		if msg == nil {
			continue
		}
		switch msg.id {
		case msg_interested:
			conn.Write((&message{id: msg_unchoke}).serialize())
		case msg_request:
			index := binary.BigEndian.Uint32(msg.payload[0:4])
			begin := binary.BigEndian.Uint32(msg.payload[4:8])
			length := binary.BigEndian.Uint32(msg.payload[8:12])
			const piece_len = 32 * 1024
			start := int(index)*piece_len + int(begin)
			block := append([]byte(nil), data[start:start+int(length)]...)
			if corrupt {
				block[0] ^= 0xff
			}
			payload := make([]byte, 8+len(block))
			binary.BigEndian.PutUint32(payload[0:4], index)
			binary.BigEndian.PutUint32(payload[4:8], begin)
			copy(payload[8:], block)
			conn.Write((&message{id: msg_piece, payload: payload}).serialize())
		}
	}
}

func compact(ps ...peer) []byte {
	var b []byte
	for _, p := range ps {
		b = append(b, p.ip.To4()...)
		b = binary.BigEndian.AppendUint16(b, p.port)
	}
	return b
}

// TestDownloadEndToEnd runs the real download() against a fake UDP tracker
// that hands out a good seeder, a seeder that corrupts every block, and a
// dead address. The download must finish with correct data.
func TestDownloadEndToEnd(t *testing.T) {
	const piece_len = 32 * 1024
	data := make([]byte, 5*piece_len+1234) // 6 pieces, last one short
	rand.Read(data)

	tf := torrent_file{piece_length: piece_len, length: len(data), name: "e2e.bin"}
	copy(tf.info_hash[:], "e2e-test-info-hash!!")
	for begin := 0; begin < len(data); begin += piece_len {
		end := min(begin+piece_len, len(data))
		tf.piece_hashes = append(tf.piece_hashes, sha1.Sum(data[begin:end]))
	}
	n := len(tf.piece_hashes)

	good := fake_seeder(t, tf.info_hash, data, n, false)
	bad := fake_seeder(t, tf.info_hash, data, n, true)

	dead_ln, _ := net.Listen("tcp4", "127.0.0.1:0")
	dead := peer{ip: net.IPv4(127, 0, 0, 1).To4(), port: uint16(dead_ln.Addr().(*net.TCPAddr).Port)}
	dead_ln.Close() // nothing listens here any more

	tracker, _ := fake_udp_tracker(t, compact(bad, dead, good), "")
	tf.trackers = []string{tracker}

	type res struct {
		buf []byte
		err error
	}
	ch := make(chan res, 1)
	go func() {
		buf, err := tf.download()
		ch <- res{buf, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if !bytes.Equal(r.buf, data) {
			t.Fatal("downloaded data does not match")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("download timed out")
	}
}
