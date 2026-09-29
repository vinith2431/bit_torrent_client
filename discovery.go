package main

import (
	"bufio"
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	mrand "math/rand/v2"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/ipv4"
)

const (
	listen_port = 6881

	default_announce_interval = 30 * time.Minute
	min_announce_interval     = 1 * time.Minute
	retry_announce_interval   = 5 * time.Minute // after a failed announce

	// UDP tracker protocol (BEP 15)
	udp_tracker_magic = 0x41727101980
	action_connect    = 0
	action_announce   = 1
	action_error      = 3

	// Local Service Discovery (BEP 14)
	lsd_group          = "239.192.152.143:6771"
	lsd_interval       = 5 * time.Minute
	lsd_startup_repeat = 3 * time.Second
)

// run_discovery announces to every tracker and to the local network, and
// feeds every peer found into the peer manager until stop is closed.
func (t *torrent_file) run_discovery(pm *peer_manager, peer_id [20]byte, stop <-chan struct{}) {
	for _, tr := range t.trackers {
		go t.announce_loop(tr, pm, peer_id, stop)
	}
	go run_lsd(t.info_hash, listen_port, pm, stop)
}

// announce_loop re-announces to one tracker on the interval it asks for.
func (t *torrent_file) announce_loop(tracker string, pm *peer_manager, peer_id [20]byte, stop <-chan struct{}) {
	source := source_label(tracker)
	for {
		var peers []peer
		var interval time.Duration
		var err error
		if strings.HasPrefix(tracker, "udp://") {
			peers, interval, err = announce_udp(tracker, t.info_hash, peer_id, listen_port, t.length)
		} else {
			peers, interval, err = t.request_peers_http(tracker, peer_id, listen_port)
		}

		if err != nil {
			log.Printf("tracker %s: %v\n", source, err)
			interval = retry_announce_interval
		} else {
			added := pm.add(peers, source)
			log.Printf("tracker %s: %d peers (%d new)\n", source, len(peers), added)
		}
		if interval < min_announce_interval {
			interval = min_announce_interval
		}

		select {
		case <-time.After(interval):
		case <-stop:
			return
		}
	}
}

// source_label shortens a tracker URL to "scheme:host" for the peer table.
func source_label(tracker string) string {
	u, err := url.Parse(tracker)
	if err != nil {
		return tracker
	}
	return u.Scheme + ":" + u.Hostname()
}

// announce_udp performs a BEP 15 connect + announce against a udp:// tracker.
func announce_udp(tracker string, info_hash, peer_id [20]byte, port uint16, left int) ([]peer, time.Duration, error) {
	u, err := url.Parse(tracker)
	if err != nil {
		return nil, 0, err
	}
	// udp4 so the tracker answers with 6-byte IPv4 peers.
	conn, err := net.DialTimeout("udp4", u.Host, 10*time.Second)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()

	// connect request:  magic(8) action(4) transaction_id(4)
	// connect response: action(4) transaction_id(4) connection_id(8)
	tid := mrand.Uint32()
	req := make([]byte, 16)
	binary.BigEndian.PutUint64(req[0:8], udp_tracker_magic)
	binary.BigEndian.PutUint32(req[8:12], action_connect)
	binary.BigEndian.PutUint32(req[12:16], tid)

	resp, err := udp_round_trip(conn, req, action_connect, tid, 16)
	if err != nil {
		return nil, 0, err
	}
	conn_id := binary.BigEndian.Uint64(resp[8:16])

	// announce request (98 bytes):
	//   connection_id(8) action(4) transaction_id(4) info_hash(20) peer_id(20)
	//   downloaded(8) left(8) uploaded(8) event(4) ip(4) key(4) num_want(4) port(2)
	// announce response:
	//   action(4) transaction_id(4) interval(4) leechers(4) seeders(4) peers(6*n)
	tid = mrand.Uint32()
	req = make([]byte, 98)
	binary.BigEndian.PutUint64(req[0:8], conn_id)
	binary.BigEndian.PutUint32(req[8:12], action_announce)
	binary.BigEndian.PutUint32(req[12:16], tid)
	copy(req[16:36], info_hash[:])
	copy(req[36:56], peer_id[:])
	binary.BigEndian.PutUint64(req[56:64], 0)            // downloaded
	binary.BigEndian.PutUint64(req[64:72], uint64(left)) // left
	binary.BigEndian.PutUint64(req[72:80], 0)            // uploaded
	binary.BigEndian.PutUint32(req[80:84], 0)            // event: none
	binary.BigEndian.PutUint32(req[84:88], 0)            // ip: use sender address
	binary.BigEndian.PutUint32(req[88:92], mrand.Uint32())
	binary.BigEndian.PutUint32(req[92:96], 0xFFFFFFFF) // num_want: -1 = tracker default
	binary.BigEndian.PutUint16(req[96:98], port)

	resp, err = udp_round_trip(conn, req, action_announce, tid, 20)
	if err != nil {
		return nil, 0, err
	}
	interval := time.Duration(binary.BigEndian.Uint32(resp[8:12])) * time.Second
	if interval <= 0 {
		interval = default_announce_interval
	}

	peer_bytes := resp[20:]
	peer_bytes = peer_bytes[:len(peer_bytes)/6*6] // ignore a trailing partial entry
	peers, err := unmarshal_peers(peer_bytes)
	return peers, interval, err
}

// udp_round_trip sends req and waits for a response with a matching
// transaction id, resending with growing timeouts since UDP can drop packets.
func udp_round_trip(conn net.Conn, req []byte, action, tid uint32, min_len int) ([]byte, error) {
	buf := make([]byte, 64*1024)
	for try := 0; try < 3; try++ {
		if _, err := conn.Write(req); err != nil {
			return nil, err
		}
		conn.SetReadDeadline(time.Now().Add(time.Duration(4<<try) * time.Second))
		for {
			n, err := conn.Read(buf)
			if err != nil {
				break // timeout or ICMP error: resend
			}
			if n < 8 || binary.BigEndian.Uint32(buf[4:8]) != tid {
				continue // stray packet
			}
			switch got := binary.BigEndian.Uint32(buf[0:4]); {
			case got == action_error:
				return nil, fmt.Errorf("tracker error: %s", buf[8:n])
			case got != action:
				return nil, fmt.Errorf("expected action %d, got %d", action, got)
			case n < min_len:
				return nil, fmt.Errorf("response too short: %d bytes", n)
			}
			out := make([]byte, n)
			copy(out, buf[:n])
			return out, nil
		}
	}
	return nil, fmt.Errorf("no response from %s", conn.RemoteAddr())
}

// lsd_message builds a BEP 14 Local Service Discovery announcement.
func lsd_message(info_hash [20]byte, port uint16, cookie string) []byte {
	return []byte(fmt.Sprintf(
		"BT-SEARCH * HTTP/1.1\r\nHost: %s\r\nPort: %d\r\nInfohash: %X\r\ncookie: %s\r\n\r\n\r\n",
		lsd_group, port, info_hash[:], cookie))
}

// parse_lsd_message parses a BEP 14 announcement. A message may carry
// several Infohash headers.
func parse_lsd_message(data []byte) (port uint16, info_hashes [][20]byte, cookie string, ok bool) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	if !sc.Scan() || !strings.HasPrefix(sc.Text(), "BT-SEARCH ") {
		return 0, nil, "", false
	}
	for sc.Scan() {
		name, value, found := strings.Cut(sc.Text(), ":")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "port":
			n, err := strconv.ParseUint(value, 10, 16)
			if err != nil {
				return 0, nil, "", false
			}
			port = uint16(n)
		case "infohash":
			raw, err := hex.DecodeString(value)
			if err != nil || len(raw) != 20 {
				continue
			}
			var ih [20]byte
			copy(ih[:], raw)
			info_hashes = append(info_hashes, ih)
		case "cookie":
			cookie = value
		}
	}
	return port, info_hashes, cookie, port != 0 && len(info_hashes) > 0
}

// run_lsd announces this torrent on the local network every lsd_interval and
// adds any local peer announcing the same torrent.
func run_lsd(info_hash [20]byte, port uint16, pm *peer_manager, stop <-chan struct{}) {
	group, err := net.ResolveUDPAddr("udp4", lsd_group)
	if err != nil {
		log.Printf("lsd: %v\n", err)
		return
	}

	var c [8]byte
	crand.Read(c[:])
	cookie := hex.EncodeToString(c[:]) // lets us ignore our own announcements

	// A machine can have several network interfaces (Wi-Fi, Ethernet, VPN,
	// virtual adapters) and the OS default for multicast is often the wrong
	// one, so join the group and announce on every one of them.
	ifaces := multicast_interfaces()

	if listener, err := net.ListenMulticastUDP("udp4", nil, group); err != nil {
		log.Printf("lsd: cannot listen, local peers will not be discovered: %v\n", err)
	} else {
		pc := ipv4.NewPacketConn(listener)
		for i := range ifaces {
			pc.JoinGroup(&ifaces[i], group) // "already joined" errors are fine
		}
		// Go disables loopback on multicast listeners; on Windows that also
		// hides announcements from other clients on this same machine.
		pc.SetMulticastLoopback(true)

		go func() {
			<-stop
			listener.Close()
		}()
		go lsd_listen(listener, info_hash, cookie, pm)
	}

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		log.Printf("lsd: cannot announce: %v\n", err)
		return
	}
	defer sender.Close()
	sender_pc := ipv4.NewPacketConn(sender)
	sender_pc.SetMulticastLoopback(true)

	msg := lsd_message(info_hash, port, cookie)
	// The first announcement is repeated after a few seconds, in case other
	// local clients started at the same moment and were not listening yet.
	wait := lsd_startup_repeat
	for {
		sent := 0
		for i := range ifaces {
			if sender_pc.SetMulticastInterface(&ifaces[i]) != nil {
				continue
			}
			if _, err := sender.WriteToUDP(msg, group); err == nil {
				sent++
			}
		}
		if sent == 0 {
			log.Printf("lsd: announce failed on all %d interfaces\n", len(ifaces))
		}
		select {
		case <-time.After(wait):
		case <-stop:
			return
		}
		wait = lsd_interval
	}
}

// multicast_interfaces returns the up, multicast-capable interfaces that have
// an IPv4 address.
func multicast_interfaces() []net.Interface {
	all, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []net.Interface
	for _, ifi := range all {
		if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagMulticast == 0 {
			continue
		}
		addrs, _ := ifi.Addrs()
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				out = append(out, ifi)
				break
			}
		}
	}
	return out
}

func lsd_listen(conn *net.UDPConn, info_hash [20]byte, own_cookie string, pm *peer_manager) {
	buf := make([]byte, 2048)
	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			return // closed
		}
		port, hashes, cookie, ok := parse_lsd_message(buf[:n])
		if !ok || cookie == own_cookie {
			continue
		}
		for _, ih := range hashes {
			if ih == info_hash {
				p := peer{ip: from.IP, port: port}
				if pm.add([]peer{p}, "lsd") > 0 {
					log.Printf("lsd: found local peer %s\n", p)
				}
				break
			}
		}
	}
}
