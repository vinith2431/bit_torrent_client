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
	Announce string       `bencode:"announce"`
	Info     bencode_info `bencode:"info"`
}

type torrent_file struct {
	announce     string
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
	if !strings.HasPrefix(b.Announce, "http") {
		return torrent_file{}, fmt.Errorf("unsupported tracker %q (need http/https)", b.Announce)
	}

	piece_hashes, err := b.Info.split_piece_hashes()
	if err != nil {
		return torrent_file{}, err
	}

	return torrent_file{
		announce:     b.Announce,
		info_hash:    info_hash,
		piece_hashes: piece_hashes,
		piece_length: b.Info.PieceLength,
		length:       b.Info.Length,
		name:         b.Info.Name,
	}, nil
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

func (t *torrent_file) request_peers(peer_id [20]byte, port uint16) ([]peer, error) {
	base, err := url.Parse(t.announce)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Check for tracker failure reason first.
	var failure_resp struct {
		FailureReason string `bencode:"failure reason"`
	}
	if err := bencode.Unmarshal(bytes.NewReader(body), &failure_resp); err == nil {
		if failure_resp.FailureReason != "" {
			return nil, fmt.Errorf("tracker error: %s", failure_resp.FailureReason)
		}
	}

	// First try compact peer format.
	var compact_resp struct {
		Peers string `bencode:"peers"`
	}

	if err := bencode.Unmarshal(bytes.NewReader(body), &compact_resp); err == nil {
		if len(compact_resp.Peers) > 0 {
			return unmarshal_peers([]byte(compact_resp.Peers))
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
		return nil, err
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

	return peers, nil
}

func new_peer_id() ([20]byte, error) {
	var id [20]byte
	_, err := rand.Read(id[:])
	return id, err
}
