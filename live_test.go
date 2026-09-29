package main

import (
	"os"
	"testing"
)

// TestLiveDiscovery talks to real trackers. It only runs when LIVE_TORRENT
// points at a .torrent file, e.g.
//
//	LIVE_TORRENT=ubuntu.torrent go test -run TestLiveDiscovery -v
func TestLiveDiscovery(t *testing.T) {
	path := os.Getenv("LIVE_TORRENT")
	if path == "" {
		t.Skip("set LIVE_TORRENT=<file.torrent> to run against real trackers")
	}
	tf, err := open(path)
	if err != nil {
		t.Fatal(err)
	}
	peer_id, _ := new_peer_id()
	pm := new_peer_manager()

	trackers := append(tf.trackers,
		"udp://tracker.opentrackr.org:1337/announce",
		"udp://open.stealth.si:80/announce",
		"udp://tracker.torrent.eu.org:451/announce",
		"udp://exodus.desync.com:6969/announce",
		"udp://open.demonii.com:1337/announce",
	)
	for _, tr := range trackers {
		var peers []peer
		if tr[:4] == "udp:" {
			peers, _, err = announce_udp(tr, tf.info_hash, peer_id, listen_port, tf.length)
		} else {
			peers, _, err = tf.request_peers_http(tr, peer_id, listen_port)
		}
		if err != nil {
			t.Logf("%s: %v", tr, err)
			continue
		}
		added := pm.add(peers, source_label(tr))
		t.Logf("%s: %d peers, %d new", tr, len(peers), added)
	}
	known, _ := pm.counts()
	if known == 0 {
		t.Fatal("no peers from any tracker")
	}
	t.Logf("peer table:\n%s", pm.table(5))
}
