package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

func test_peer(ip string, port uint16) peer {
	return peer{ip: net.ParseIP(ip), port: port}
}

func TestAddDeduplicates(t *testing.T) {
	pm := new_peer_manager()
	n := pm.add([]peer{
		test_peer("1.2.3.4", 6881),
		test_peer("1.2.3.4", 6881), // duplicate in same batch
		test_peer("0.0.0.0", 6881), // unspecified
		test_peer("1.2.3.5", 0),    // no port
	}, "http:a")
	if n != 1 {
		t.Fatalf("added %d, want 1", n)
	}
	// Same peer from another source is still a duplicate; compact 4-byte IPs
	// and 16-byte parsed IPs must match.
	if n := pm.add([]peer{{ip: net.IP{1, 2, 3, 4}, port: 6881}}, "udp:b"); n != 0 {
		t.Fatalf("added %d duplicates from second source", n)
	}
	if known, _ := pm.counts(); known != 1 {
		t.Fatalf("known = %d, want 1", known)
	}
	if src := pm.peers["1.2.3.4:6881"].source; src != "http:a" {
		t.Fatalf("source = %q, want first source", src)
	}
}

func TestAddCopiesIP(t *testing.T) {
	pm := new_peer_manager()
	buf := []byte{1, 2, 3, 4, 0x1a, 0xe1}
	peers, _ := unmarshal_peers(buf)
	pm.add(peers, "x")
	buf[0] = 9 // caller reuses its buffer
	if _, ok := pm.peers["1.2.3.4:6881"]; !ok || pm.peers["1.2.3.4:6881"].addr.ip.String() != "1.2.3.4" {
		t.Fatal("peer table aliases the caller's buffer")
	}
}

func TestNextPrefersHigherScore(t *testing.T) {
	pm := new_peer_manager()
	pm.add([]peer{test_peer("10.0.0.1", 1), test_peer("10.0.0.2", 2)}, "x")
	slow := pm.peers["10.0.0.1:1"]
	fast := pm.peers["10.0.0.2:2"]
	pm.report_connect(slow, 200*time.Millisecond)
	pm.report_piece(slow, 1<<20, 4*time.Second, true) // 256 KB/s
	pm.report_connect(fast, 20*time.Millisecond)
	pm.report_piece(fast, 4<<20, time.Second, true) // 4 MB/s

	if got := pm.next(); got != fast {
		t.Fatalf("first pick = %s, want fast peer", got.addr)
	}
	if got := pm.next(); got != slow {
		t.Fatalf("second pick = %v, want slow peer (fast is active)", got)
	}
	if got := pm.next(); got != nil {
		t.Fatalf("third pick = %s, want nil (all active)", got.addr)
	}
}

func TestUntestedPeersBeatFailedPeers(t *testing.T) {
	pm := new_peer_manager()
	pm.add([]peer{test_peer("10.0.0.1", 1), test_peer("10.0.0.2", 2)}, "x")
	bad := pm.peers["10.0.0.1:1"]
	bad.failed = 2 // failed before, backoff already expired

	if got := pm.next(); got == bad {
		t.Fatal("picked a failed peer over an untested one")
	}
}

func TestReleaseBackoff(t *testing.T) {
	pm := new_peer_manager()
	pm.add([]peer{test_peer("10.0.0.1", 1)}, "x")
	pi := pm.next()
	pm.release(pi, outcome_failed)

	if got := pm.next(); got != nil {
		t.Fatal("failed peer was picked again during backoff")
	}
	if pi.failed != 1 || pi.state(time.Now()) != "backoff" {
		t.Fatalf("failed=%d state=%s", pi.failed, pi.state(time.Now()))
	}

	pi.retry_after = time.Now().Add(-time.Second) // backoff expired
	if got := pm.next(); got != pi {
		t.Fatal("peer not retried after backoff expired")
	}
	pm.release(pi, outcome_failed)
	if wait := time.Until(pi.retry_after); wait < base_backoff {
		t.Fatalf("second backoff %v should be longer than the first", wait)
	}
}

func TestReleaseUselessDoesNotHurtReliability(t *testing.T) {
	pm := new_peer_manager()
	pm.add([]peer{test_peer("10.0.0.1", 1)}, "x")
	pi := pm.next()
	pm.release(pi, outcome_useless)
	if pi.failed != 0 {
		t.Fatal("useless peer counted as failure")
	}
	if pm.next() != nil {
		t.Fatal("useless peer should back off")
	}
}

func TestHashFailuresBan(t *testing.T) {
	pm := new_peer_manager()
	pm.add([]peer{test_peer("10.0.0.1", 1)}, "x")
	pi := pm.next()
	for i := 1; i <= max_hash_fails; i++ {
		banned := pm.report_piece(pi, 100, time.Millisecond, false)
		if banned != (i == max_hash_fails) {
			t.Fatalf("after %d bad pieces banned=%v", i, banned)
		}
	}
	pm.release(pi, outcome_failed)
	pi.retry_after = time.Time{}
	if pm.next() != nil {
		t.Fatal("banned peer was picked")
	}
}

func TestScoreRange(t *testing.T) {
	perfect := &peer_info{rtt: time.Millisecond, ok: 10, bytes: 100 << 20, busy: time.Second}
	if s := perfect.score(); s < 99 || s > 100 {
		t.Fatalf("perfect score = %.1f", s)
	}
	awful := &peer_info{failed: 5}
	if s := awful.score(); s != 0 {
		t.Fatalf("awful score = %.1f, want 0", s)
	}
	if s := (&peer_info{}).score(); s != 50 {
		t.Fatalf("untested score = %.1f, want 50", s)
	}
}

func TestTable(t *testing.T) {
	pm := new_peer_manager()
	pm.add([]peer{test_peer("10.0.0.1", 1), test_peer("10.0.0.2", 2), test_peer("10.0.0.3", 3)}, "udp:tracker")
	fast := pm.peers["10.0.0.2:2"]
	pm.report_connect(fast, 18*time.Millisecond)
	pm.report_piece(fast, 4<<20, time.Second, true)

	out := pm.table(2)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if !strings.HasPrefix(lines[2], "10.0.0.2:2") {
		t.Fatalf("best peer not first:\n%s", out)
	}
	for _, want := range []string{"18 ms", "4.0 MB/s", "100%", "udp:tracker", "... and 1 more"} {
		if !strings.Contains(out, want) {
			t.Fatalf("table missing %q:\n%s", want, out)
		}
	}
}
