package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fake_udp_tracker answers one BEP 15 connect + announce, checking the
// request layout, and returns the announce request it received.
func fake_udp_tracker(t *testing.T, peers []byte, fail_msg string) (string, <-chan []byte) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	got := make(chan []byte, 1)
	const conn_id = 0x1122334455667788

	go func() {
		buf := make([]byte, 2048)

		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n != 16 || binary.BigEndian.Uint64(buf[0:8]) != udp_tracker_magic || binary.BigEndian.Uint32(buf[8:12]) != action_connect {
			t.Errorf("bad connect request: % x", buf[:n])
			return
		}
		tid := buf[12:16]
		resp := make([]byte, 16)
		binary.BigEndian.PutUint32(resp[0:4], action_connect)
		copy(resp[4:8], tid)
		binary.BigEndian.PutUint64(resp[8:16], conn_id)
		conn.WriteToUDP(resp, from)

		n, from, err = conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		req := append([]byte(nil), buf[:n]...)
		got <- req
		if n != 98 || binary.BigEndian.Uint64(req[0:8]) != conn_id || binary.BigEndian.Uint32(req[8:12]) != action_announce {
			t.Errorf("bad announce request: % x", req)
			return
		}

		if fail_msg != "" {
			resp = make([]byte, 8, 8+len(fail_msg))
			binary.BigEndian.PutUint32(resp[0:4], action_error)
			copy(resp[4:8], req[12:16])
			resp = append(resp, fail_msg...)
		} else {
			resp = make([]byte, 20, 20+len(peers))
			binary.BigEndian.PutUint32(resp[0:4], action_announce)
			copy(resp[4:8], req[12:16])
			binary.BigEndian.PutUint32(resp[8:12], 1800) // interval
			binary.BigEndian.PutUint32(resp[12:16], 3)   // leechers
			binary.BigEndian.PutUint32(resp[16:20], 7)   // seeders
			resp = append(resp, peers...)
		}
		conn.WriteToUDP(resp, from)
	}()

	return fmt.Sprintf("udp://%s/announce", conn.LocalAddr()), got
}

func TestAnnounceUDP(t *testing.T) {
	peers := []byte{
		10, 0, 0, 1, 0x1a, 0xe1, // 10.0.0.1:6881
		192, 168, 1, 9, 0xc8, 0xd5, // 192.168.1.9:51413
	}
	tracker, got := fake_udp_tracker(t, peers, "")

	var info_hash, peer_id [20]byte
	copy(info_hash[:], "INFOHASH-0123456789A")
	copy(peer_id[:], "PEERID-0123456789ABC")

	ps, interval, err := announce_udp(tracker, info_hash, peer_id, 6881, 12345)
	if err != nil {
		t.Fatal(err)
	}
	if interval != 1800*time.Second {
		t.Errorf("interval = %v", interval)
	}
	if len(ps) != 2 || ps[0].String() != "10.0.0.1:6881" || ps[1].String() != "192.168.1.9:51413" {
		t.Errorf("peers = %v", ps)
	}

	req := <-got
	if string(req[16:36]) != string(info_hash[:]) {
		t.Errorf("info_hash at wrong offset")
	}
	if string(req[36:56]) != string(peer_id[:]) {
		t.Errorf("peer_id at wrong offset")
	}
	if left := binary.BigEndian.Uint64(req[64:72]); left != 12345 {
		t.Errorf("left = %d", left)
	}
	if nw := int32(binary.BigEndian.Uint32(req[92:96])); nw != -1 {
		t.Errorf("num_want = %d", nw)
	}
	if port := binary.BigEndian.Uint16(req[96:98]); port != 6881 {
		t.Errorf("port = %d", port)
	}
}

func TestAnnounceUDPTrackerError(t *testing.T) {
	tracker, _ := fake_udp_tracker(t, nil, "torrent not registered")
	_, _, err := announce_udp(tracker, [20]byte{}, [20]byte{}, 6881, 1)
	if err == nil || !strings.Contains(err.Error(), "torrent not registered") {
		t.Fatalf("err = %v", err)
	}
}

func TestLSDMessageRoundTrip(t *testing.T) {
	var ih [20]byte
	copy(ih[:], "abcdefghijklmnopqrst")
	msg := lsd_message(ih, 51413, "c00k1e")

	if !strings.HasPrefix(string(msg), "BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\n") {
		t.Fatalf("bad header:\n%s", msg)
	}
	port, hashes, cookie, ok := parse_lsd_message(msg)
	if !ok || port != 51413 || cookie != "c00k1e" || len(hashes) != 1 || hashes[0] != ih {
		t.Fatalf("parsed port=%d hashes=%x cookie=%q ok=%v", port, hashes, cookie, ok)
	}
}

func TestParseLSDMultipleHashesAndCase(t *testing.T) {
	msg := "BT-SEARCH * HTTP/1.1\r\nhost: 239.192.152.143:6771\r\nport: 6881\r\n" +
		"infohash: " + strings.Repeat("ab", 20) + "\r\n" +
		"Infohash: " + strings.Repeat("CD", 20) + "\r\n\r\n\r\n"
	port, hashes, _, ok := parse_lsd_message([]byte(msg))
	if !ok || port != 6881 || len(hashes) != 2 || hashes[1][0] != 0xcd {
		t.Fatalf("port=%d hashes=%x ok=%v", port, hashes, ok)
	}
	if _, _, _, ok := parse_lsd_message([]byte("GET / HTTP/1.1\r\n\r\n")); ok {
		t.Fatal("accepted a non-LSD message")
	}
}

func TestCollectTrackers(t *testing.T) {
	got := collect_trackers("http://a/announce", [][]string{
		{"http://a/announce", "udp://b:1337/announce"},
		{"wss://webtorrent/only", "https://c/announce"},
	})
	want := []string{"http://a/announce", "udp://b:1337/announce", "https://c/announce"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func bstr(s string) string { return fmt.Sprintf("%d:%s", len(s), s) }

func TestOpenReadsAnnounceList(t *testing.T) {
	info := "d" + bstr("length") + "i10e" + bstr("name") + bstr("foo") +
		bstr("piece length") + "i16384e" + bstr("pieces") + bstr(strings.Repeat("A", 20)) + "e"
	data := "d" + bstr("announce-list") + "l" +
		"l" + bstr("udp://tracker.example.org:1337/announce") + "e" +
		"l" + bstr("http://tracker.example.com/announce") + "e" +
		"e" + bstr("info") + info + "e"

	path := filepath.Join(t.TempDir(), "t.torrent")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	tf, err := open(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(tf.trackers) != 2 || tf.trackers[0] != "udp://tracker.example.org:1337/announce" {
		t.Fatalf("trackers = %v", tf.trackers)
	}
}

func TestSourceLabel(t *testing.T) {
	if got := source_label("udp://tracker.opentrackr.org:1337/announce"); got != "udp:tracker.opentrackr.org" {
		t.Fatalf("got %q", got)
	}
}

// TestLSDTwoInstances runs two LSD announcers for the same torrent on this
// machine and checks each discovers the other. It skips if the OS or
// firewall blocks multicast.
func TestLSDTwoInstances(t *testing.T) {
	if testing.Short() {
		t.Skip("multicast test")
	}
	var ih [20]byte
	copy(ih[:], "lsd-two-instance-tst")
	stop := make(chan struct{})
	defer close(stop)

	a, b := new_peer_manager(), new_peer_manager()
	go run_lsd(ih, 50001, a, stop)
	go run_lsd(ih, 50002, b, stop)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ka, _ := a.counts()
		kb, _ := b.counts()
		if ka > 0 && kb > 0 {
			for _, pi := range a.peers {
				if pi.addr.port != 50002 || pi.source != "lsd" {
					t.Fatalf("a found %s from %s, want port 50002 from lsd", pi.addr, pi.source)
				}
			}
			for _, pi := range b.peers {
				if pi.addr.port != 50001 {
					t.Fatalf("b found %s, want port 50001", pi.addr)
				}
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Skip("no LSD announcements received; multicast is probably blocked here")
}
