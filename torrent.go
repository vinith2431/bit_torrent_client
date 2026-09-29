package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackpal/bencode-go"
)

type bencode_info struct {
	Pieces      string `bencode:"pieces"`
	PieceLength int    `bencode:"piece length"`
	Length      int    `bencode:"length"`
	Name        string `bencode:"name"`
}

type bencode_torrent struct {
	Announce     string       `bencode:"announce"`
	AnnounceList [][]string   `bencode:"announce-list"`
	Info         bencode_info `bencode:"info"`
}

type torrent_file struct {
	trackers     []string
	info_hash    [20]byte
	piece_hashes [][20]byte
	piece_length int
	length       int
	name         string
}

func open(path string) (torrent_file, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return torrent_file{}, err
	}

	raw := &bencode_torrent{}
	if err := bencode.Unmarshal(bytes.NewReader(data), raw); err != nil {
		return torrent_file{}, err
	}

	ri, err := raw_info(data)
	if err != nil {
		return torrent_file{}, err
	}

	return raw.to_torrent_file(sha1.Sum(ri))
}

func (b *bencode_torrent) to_torrent_file(info_hash [20]byte) (torrent_file, error) {
	if b.Info.Length == 0 {
		return torrent_file{}, fmt.Errorf("multi-file torrents not supported")
	}
	trackers := collect_trackers(b.Announce, b.AnnounceList)
	if len(trackers) == 0 {
		return torrent_file{}, fmt.Errorf("no supported trackers (need http, https or udp)")
	}

	piece_hashes, err := b.Info.split_piece_hashes()
	if err != nil {
		return torrent_file{}, err
	}

	return torrent_file{
		trackers:     trackers,
		info_hash:    info_hash,
		piece_hashes: piece_hashes,
		piece_length: b.Info.PieceLength,
		length:       b.Info.Length,
		name:         b.Info.Name,
	}, nil
}

// collect_trackers merges announce and announce-list (BEP 12) into one
// de-duplicated list, keeping only trackers we can talk to.
func collect_trackers(announce string, announce_list [][]string) []string {
	seen := map[string]bool{}
	var trackers []string
	add := func(t string) {
		t = strings.TrimSpace(t)
		if seen[t] {
			return
		}
		if strings.HasPrefix(t, "http://") || strings.HasPrefix(t, "https://") || strings.HasPrefix(t, "udp://") {
			seen[t] = true
			trackers = append(trackers, t)
		}
	}
	add(announce)
	for _, tier := range announce_list {
		for _, t := range tier {
			add(t)
		}
	}
	return trackers
}

func skip_value(data []byte, i int) (int, error) {
	if i >= len(data) {
		return 0, fmt.Errorf("truncated bencode")
	}
	c := data[i]
	switch {
	case c == 'i':
		e := bytes.IndexByte(data[i:], 'e')
		if e < 0 {
			return 0, fmt.Errorf("unterminated int")
		}
		return i + e + 1, nil
	case c == 'l' || c == 'd':
		i++
		for i < len(data) && data[i] != 'e' {
			var err error
			if i, err = skip_value(data, i); err != nil {
				return 0, err
			}
		}
		if i >= len(data) {
			return 0, fmt.Errorf("unterminated list/dict")
		}
		return i + 1, nil
	case c >= '0' && c <= '9':
		colon := bytes.IndexByte(data[i:], ':')
		if colon < 0 {
			return 0, fmt.Errorf("bad string length")
		}
		n, err := strconv.Atoi(string(data[i : i+colon]))
		if err != nil {
			return 0, err
		}
		end := i + colon + 1 + n
		if end > len(data) {
			return 0, fmt.Errorf("string overruns data")
		}
		return end, nil
	}
	return 0, fmt.Errorf("bad bencode at %d", i)
}

func raw_info(data []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != 'd' {
		return nil, fmt.Errorf("torrent is not a dict")
	}
	i := 1
	for i < len(data) && data[i] != 'e' {
		ke, err := skip_value(data, i)
		if err != nil {
			return nil, err
		}
		colon := bytes.IndexByte(data[i:], ':')
		key := string(data[i+colon+1 : ke])
		ve, err := skip_value(data, ke)
		if err != nil {
			return nil, err
		}
		if key == "info" {
			return data[ke:ve], nil
		}
		i = ve
	}
	return nil, fmt.Errorf("no info dict")
}

func (i *bencode_info) split_piece_hashes() ([][20]byte, error) {
	buf := []byte(i.Pieces)

	if len(buf)%20 != 0 {
		return nil, fmt.Errorf("malformed pieces: length %d not a multiple of 20", len(buf))
	}

	n := len(buf) / 20
	hashes := make([][20]byte, n)

	for j := 0; j < n; j++ {
		copy(hashes[j][:], buf[j*20:(j+1)*20])
	}
	return hashes, nil
}

type peer struct {
	ip   net.IP
	port uint16
}

func unmarshal_peers(data []byte) ([]peer, error) {
	if len(data)%6 != 0 {
		return nil, fmt.Errorf("malformed peers: length %d not a multiple of 6", len(data))
	}

	n := len(data) / 6
	peers := make([]peer, n)

	for i := 0; i < n; i++ {
		peers[i].ip = net.IP(data[i*6 : i*6+4])
		peers[i].port = binary.BigEndian.Uint16(data[i*6+4 : i*6+6])
	}
	return peers, nil
}

func (p peer) String() string {
	return net.JoinHostPort(p.ip.String(), strconv.Itoa(int(p.port)))
}

// request_peers_http announces to an HTTP(S) tracker and returns its peers
// and how long to wait before announcing again.
func (t *torrent_file) request_peers_http(tracker string, peer_id [20]byte, port uint16) ([]peer, time.Duration, error) {
	base, err := url.Parse(tracker)
	if err != nil {
		return nil, 0, err
	}

	params := url.Values{
		"info_hash":  []string{string(t.info_hash[:])},
		"peer_id":    []string{string(peer_id[:])},
		"port":       []string{strconv.Itoa(int(port))},
		"uploaded":   []string{"0"},
		"downloaded": []string{"0"},
		"compact":    []string{"1"},
		"left":       []string{strconv.Itoa(t.length)},
		"numwant":    []string{"100"},
	}

	base.RawQuery = strings.ReplaceAll(params.Encode(), "+", "%20")

	http_client := &http.Client{Timeout: 15 * time.Second}

	response, err := http_client.Get(base.String())
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, 0, err
	}

	// Check for tracker failure reason first, and pick up the re-announce interval.
	var meta_resp struct {
		FailureReason string `bencode:"failure reason"`
		Interval      int    `bencode:"interval"`
	}
	interval := default_announce_interval
	if err := bencode.Unmarshal(bytes.NewReader(body), &meta_resp); err == nil {
		if meta_resp.FailureReason != "" {
			return nil, 0, fmt.Errorf("tracker error: %s", meta_resp.FailureReason)
		}
		if meta_resp.Interval > 0 {
			interval = time.Duration(meta_resp.Interval) * time.Second
		}
	}

	// First try compact peer format.
	var compact_resp struct {
		Peers string `bencode:"peers"`
	}

	if err := bencode.Unmarshal(bytes.NewReader(body), &compact_resp); err == nil {
		if len(compact_resp.Peers) > 0 {
			peers, err := unmarshal_peers([]byte(compact_resp.Peers))
			return peers, interval, err
		}
	}

	// If compact format was not returned, try dictionary/list format.
	var list_resp struct {
		Peers []struct {
			IP   string `bencode:"ip"`
			Port int    `bencode:"port"`
		} `bencode:"peers"`
	}

	if err := bencode.Unmarshal(bytes.NewReader(body), &list_resp); err != nil {
		return nil, 0, err
	}

	peers := make([]peer, 0, len(list_resp.Peers))

	for _, p := range list_resp.Peers {
		ip := net.ParseIP(p.IP)
		if ip == nil {
			continue
		}

		peers = append(peers, peer{
			ip:   ip,
			port: uint16(p.Port),
		})
	}

	return peers, interval, nil
}

func new_peer_id() ([20]byte, error) {
	var id [20]byte
	_, err := rand.Read(id[:])
	return id, err
}
