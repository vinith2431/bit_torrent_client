package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

const block_size = 16384
const max_backlog = 5

type piece_work struct {
	index  int
	hash   [20]byte
	length int
}

type piece_result struct {
	index int
	data  []byte
}

type piece_progress struct {
	index      int
	buf        []byte
	downloaded int
	requested  int
	backlog    int
}

func (pp *piece_progress) fill_requests(c *client, piece_length int) error {
	for pp.backlog < max_backlog && pp.requested < piece_length {
		block_len := block_size
		if piece_length-pp.requested < block_size {
			block_len = piece_length - pp.requested
		}
		if err := c.send_request(pp.index, pp.requested, block_len); err != nil {
			return err
		}
		pp.backlog++
		pp.requested += block_len
	}
	return nil
}

func (pp *piece_progress) handle_message(c *client) error {
	msg, err := c.read()
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}

	switch msg.id {

	case msg_unchoke:
		c.choked = false
		log.Printf("unchoked by peer\n")

	case msg_choke:
		c.choked = true
		pp.requested = pp.downloaded
		pp.backlog = 0

	case msg_bitfield:
		c.bitfield = msg.payload

	case msg_have:
		index, err := parse_have(msg)
		if err != nil {
			return err
		}
		c.bitfield.set_piece(index)

	case msg_piece:
		n, err := parse_piece(pp.index, pp.buf, msg)
		if err != nil {
			return err
		}
		pp.downloaded += n
		pp.backlog--
	}

	return nil
}

func download_piece(c *client, pw *piece_work) ([]byte, error) {
	pp := &piece_progress{
		index: pw.index,
		buf:   make([]byte, pw.length),
	}

	c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.conn.SetDeadline(time.Time{})

	for pp.downloaded < pw.length {
		if !c.choked {
			if err := pp.fill_requests(c, pw.length); err != nil {
				return nil, err
			}
		}
		if err := pp.handle_message(c); err != nil {
			return nil, err
		}
	}

	return pp.buf, nil
}

func check_integrity(pw *piece_work, data []byte) error {
	hash := sha1.Sum(data)
	if !bytes.Equal(hash[:], pw.hash[:]) {
		return fmt.Errorf("piece %d failed integrity check", pw.index)
	}
	return nil
}

func start_download_worker(p peer, info_hash [20]byte, peer_id [20]byte, num_pieces int, work_ch chan *piece_work, results_ch chan *piece_result) {
	c, err := new_client(p, info_hash, peer_id, num_pieces)
	if err != nil {
		log.Printf("could not connect to peer %s: %v\n", p, err)
		return
	}
	defer c.conn.Close()

	log.Printf("connected to peer %s\n", p)

	if err := c.send_unchoke(); err != nil {
		return
	}
	if err := c.send_interested(); err != nil {
		return
	}
	log.Printf("sent interested to %s, choked=%v\n", p, c.choked)

	misses := 0
	for pw := range work_ch {
		if !c.bitfield.has_piece(pw.index) {
			work_ch <- pw
			if misses++; misses > 100 {
				return
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		misses = 0

		data, err := download_piece(c, pw)
		if err != nil {
			work_ch <- pw
			log.Printf("failed to download piece %d from %s: %v\n", pw.index, p, err)
			return
		}

		if err := check_integrity(pw, data); err != nil {
			work_ch <- pw
			log.Printf("piece %d from %s failed integrity check\n", pw.index, p)
			continue
		}

		results_ch <- &piece_result{pw.index, data}
	}
}

func (t *torrent_file) download() ([]byte, error) {
	log.Println("starting download for", t.name)

	peer_id, err := new_peer_id()
	if err != nil {
		return nil, err
	}

	peers, err := t.request_peers(peer_id, 6881)
	if err != nil {
		return nil, err
	}
	log.Printf("got %d peers from tracker\n", len(peers))
	if len(peers) == 0 {
		return nil, fmt.Errorf("tracker returned no peers")
	}

	work_ch := make(chan *piece_work, len(t.piece_hashes))
	results_ch := make(chan *piece_result)

	for index, hash := range t.piece_hashes {
		length := t.piece_length_at(index)
		work_ch <- &piece_work{index, hash, length}
	}

	alive := int32(len(peers))
	dead := make(chan struct{})
	for _, p := range peers {
		go func(p peer) {
			defer func() {
				if atomic.AddInt32(&alive, -1) == 0 {
					close(dead)
				}
			}()
			start_download_worker(p, t.info_hash, peer_id, len(t.piece_hashes), work_ch, results_ch)
		}(p)
	}

	buf := make([]byte, t.length)
	done_pieces := 0

	for done_pieces < len(t.piece_hashes) {
		var result *piece_result
		select {
		case result = <-results_ch:
		case <-dead:
			return nil, fmt.Errorf("all peers exhausted at %d/%d pieces", done_pieces, len(t.piece_hashes))
		}

		begin := result.index * t.piece_length
		end := begin + len(result.data)
		copy(buf[begin:end], result.data)
		done_pieces++

		percent := float64(done_pieces) / float64(len(t.piece_hashes)) * 100
		log.Printf("%.2f%% done — piece %d downloaded, %d peers active\n", percent, result.index, atomic.LoadInt32(&alive))
	}

	return buf, nil
}

func (t *torrent_file) piece_length_at(index int) int {
	begin := index * t.piece_length
	end := begin + t.piece_length
	if end > t.length {
		end = t.length
	}
	return end - begin
}
