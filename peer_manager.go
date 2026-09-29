package main

import (
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	max_hash_fails  = 3                // peers sending this many bad pieces are never used again
	base_backoff    = 30 * time.Second // wait after 1st failure; grows with each failure
	max_backoff     = 10 * time.Minute
	useless_backoff = 2 * time.Minute // peer connected fine but had nothing we needed

	good_speed = 5 << 20                // 5 MB/s counts as a perfect speed score
	bad_rtt    = 500 * time.Millisecond // RTT at or above this gets zero RTT score
)

// worker_outcome is how a download worker's session with a peer ended.
type worker_outcome int

const (
	outcome_done    worker_outcome = iota // download finished, peer behaved
	outcome_failed                        // could not connect, or the transfer broke
	outcome_useless                       // connected but had no pieces we needed
)

// peer_info is everything we know about one peer.
type peer_info struct {
	addr        peer
	source      string
	rtt         time.Duration
	bytes       int64
	busy        time.Duration
	ok          int
	failed      int
	hash_fails  int
	active      bool
	retry_after time.Time

	// Bitfield tells us which pieces this peer has.
	bitfield bitfield
}

// speed is the average verified download speed in bytes/sec.
func (pi *peer_info) speed() float64 {
	if pi.busy <= 0 {
		return 0
	}
	return float64(pi.bytes) / pi.busy.Seconds()
}

func (pi *peer_info) attempts() int {
	return pi.ok + pi.failed + pi.hash_fails
}

// reliability is the fraction of attempts (pieces or connections) that succeeded.
func (pi *peer_info) reliability() float64 {
	if pi.attempts() == 0 {
		return 0.5 // unknown
	}
	return float64(pi.ok) / float64(pi.attempts())
}

// score ranks peers from 0 to 100. Untested peers get a neutral 50, so new
// peers are tried before peers that have already failed.
func (pi *peer_info) score() float64 {
	if pi.attempts() == 0 {
		return 50
	}
	speed := math.Min(pi.speed()/good_speed, 1)
	rtt := 0.0
	if pi.rtt > 0 {
		rtt = 1 - math.Min(float64(pi.rtt)/float64(bad_rtt), 1)
	}
	return 100 * (0.5*speed + 0.2*rtt + 0.3*pi.reliability())
}

func (pi *peer_info) banned() bool {
	return pi.hash_fails >= max_hash_fails
}

// peer_manager holds the peer table, fed by all discovery sources.
type peer_manager struct {
	mu    sync.Mutex
	peers map[string]*peer_info // keyed by "ip:port", which de-duplicates peers
}

func new_peer_manager() *peer_manager {
	return &peer_manager{peers: map[string]*peer_info{}}
}

// add merges peers from any source into the table and returns how many were new.
func (pm *peer_manager) add(ps []peer, source string) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	added := 0
	for _, p := range ps {
		if p.port == 0 || p.ip == nil || p.ip.IsUnspecified() || p.ip.IsMulticast() {
			continue
		}
		key := p.String()
		if _, dup := pm.peers[key]; dup {
			continue
		}
		ip := make(net.IP, len(p.ip)) // don't alias the caller's buffer
		copy(ip, p.ip)
		pm.peers[key] = &peer_info{addr: peer{ip: ip, port: p.port}, source: source}
		added++
	}
	return added
}

// next picks the best-scoring peer that is not connected, backing off, or
// banned, and marks it active. It returns nil if there is none.
func (pm *peer_manager) next() *peer_info {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()
	var best *peer_info
	for _, pi := range pm.peers {
		if pi.active || pi.banned() || now.Before(pi.retry_after) {
			continue
		}
		if best == nil || pi.score() > best.score() {
			best = pi
		}
	}
	if best != nil {
		best.active = true
	}
	return best
}

func (pm *peer_manager) report_connect(pi *peer_info, rtt time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pi.rtt = rtt
}

// report_piece records one downloaded piece. It returns true if the peer is
// now banned for sending too many corrupt pieces.
func (pm *peer_manager) report_piece(pi *peer_info, n int, d time.Duration, hash_ok bool) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !hash_ok {
		pi.hash_fails++
		return pi.banned()
	}
	pi.ok++
	pi.bytes += int64(n)
	pi.busy += d
	return false
}

// release is called when a worker stops using a peer, so its slot can be
// given to the next best peer.
func (pm *peer_manager) release(pi *peer_info, outcome worker_outcome) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pi.active = false
	switch outcome {
	case outcome_failed:
		pi.failed++
		backoff := time.Duration(pi.failed) * base_backoff
		if backoff > max_backoff {
			backoff = max_backoff
		}
		pi.retry_after = time.Now().Add(backoff)
	case outcome_useless:
		pi.retry_after = time.Now().Add(useless_backoff)
	}
}

// counts returns how many peers are known and how many are connected.
func (pm *peer_manager) counts() (known, active int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, pi := range pm.peers {
		if pi.active {
			active++
		}
	}
	return len(pm.peers), active
}

// rarest_piece_for returns the rarest piece that this peer has
// among the pieces that are still pending.
//
// The returned index is -1 if this peer has no useful piece.
func (pm *peer_manager) rarest_piece_for(
	pi *peer_info,
	pending []bool,
) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	best := -1
	bestCount := int(^uint(0) >> 1)

	for piece := range pending {
		if !pending[piece] {
			continue
		}

		// The selected peer must actually have this piece.
		if !pi.bitfield.has_piece(piece) {
			continue
		}

		// Count how many known peers have this piece.
		count := 0

		for _, other := range pm.peers {
			if other.bitfield.has_piece(piece) {
				count++
			}
		}

		if count < bestCount {
			best = piece
			bestCount = count
		}
	}

	return best
}


// table renders the top peers by score.
func (pm *peer_manager) table(limit int) string {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	list := make([]*peer_info, 0, len(pm.peers))
	for _, pi := range pm.peers {
		list = append(list, pi)
	}
	sort.Slice(list, func(i, j int) bool {
		if si, sj := list[i].score(), list[j].score(); si != sj {
			return si > sj
		}
		return list[i].addr.String() < list[j].addr.String()
	})

	var b strings.Builder
	fmt.Fprintf(&b, "%-22s %8s %11s %12s %6s  %-8s %s\n", "Peer", "RTT", "Speed", "Reliability", "Score", "State", "Source")
	fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 90))

	now := time.Now()
	for i, pi := range list {
		if i == limit {
			fmt.Fprintf(&b, "... and %d more\n", len(list)-limit)
			break
		}
		fmt.Fprintf(&b, "%-22s %8s %11s %12s %6.0f  %-8s %s\n",
			pi.addr.String(), fmt_rtt(pi.rtt), fmt_speed(pi.speed()), fmt_reliability(pi),
			pi.score(), pi.state(now), pi.source)
	}
	return b.String()
}

func (pi *peer_info) state(now time.Time) string {
	switch {
	case pi.banned():
		return "banned"
	case pi.active:
		return "active"
	case now.Before(pi.retry_after):
		return "backoff"
	}
	return "idle"
}

func fmt_rtt(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	return fmt.Sprintf("%d ms", d.Milliseconds())
}

func fmt_speed(bps float64) string {
	switch {
	case bps <= 0:
		return "-"
	case bps >= 1<<20:
		return fmt.Sprintf("%.1f MB/s", bps/(1<<20))
	}
	return fmt.Sprintf("%.0f KB/s", bps/(1<<10))
}

func fmt_reliability(pi *peer_info) string {
	if pi.attempts() == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f%%", 100*pi.reliability())
}
