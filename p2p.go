package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"time"
)

const block_size = 16384
const max_backlog = 5

const (
	max_active_peers = 30               // peers downloaded from at once
	stall_timeout    = 2 * time.Minute  // give up if no piece arrives for this long
	table_interval   = 15 * time.Second // how often to log the peer table
	table_rows       = 10
)

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

// start_download_worker downloads pieces from one peer until the download
// is done or the peer stops being useful, and reports how it went.
func start_download_worker(pm *peer_manager, pi *peer_info, info_hash [20]byte, peer_id [20]byte, num_pieces int, work_ch chan *piece_work, results_ch chan *piece_result, done <-chan struct{}) worker_outcome {
	p := pi.addr
	c, err := new_client(p, info_hash, peer_id, num_pieces)
	if err != nil {
		log.Printf("could not connect to peer %s: %v\n", p, err)
		return outcome_failed
	}
	defer c.conn.Close()
	pm.report_connect(pi, c.rtt)

	log.Printf("connected to peer %s (rtt %v)\n", p, c.rtt.Round(time.Millisecond))

	if err := c.send_unchoke(); err != nil {
		return outcome_failed
	}
	if err := c.send_interested(); err != nil {
		return outcome_failed
	}
	log.Printf("sent interested to %s, choked=%v\n", p, c.choked)

	misses := 0
	for {
		var pw *piece_work
		select {
		case pw = <-work_ch:
		case <-done:
			return outcome_done
		}

		if !c.bitfield.has_piece(pw.index) {
			work_ch <- pw
			if misses++; misses > 100 {
				return outcome_useless
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		misses = 0

		start := time.Now()
		data, err := download_piece(c, pw)
		if err != nil {
			work_ch <- pw
			log.Printf("failed to download piece %d from %s: %v\n", pw.index, p, err)
			return outcome_failed
		}
		elapsed := time.Since(start)

		if err := check_integrity(pw, data); err != nil {
			work_ch <- pw
			log.Printf("piece %d from %s failed integrity check\n", pw.index, p)
			if pm.report_piece(pi, len(data), elapsed, false) {
				log.Printf("banning peer %s after %d corrupt pieces\n", p, max_hash_fails)
				return outcome_failed
			}
			continue
		}
		pm.report_piece(pi, len(data), elapsed, true)

		select {
		case results_ch <- &piece_result{pw.index, data}:
		case <-done:
			return outcome_done
		}
	}
}

// fill_peer_slots keeps up to max_active_peers workers running, always
// handing a free slot to the best-scoring peer. When a peer fails or turns
// out useless its slot is freed and the next best peer replaces it.
func (t *torrent_file) fill_peer_slots(pm *peer_manager, peer_id [20]byte, work_ch chan *piece_work, results_ch chan *piece_result, done <-chan struct{}) {
	slots := make(chan struct{}, max_active_peers)
	for {
		select {
		case slots <- struct{}{}:
		case <-done:
			return
		}

		pi := pm.next()
		if pi == nil {
			// Nobody available right now; discovery may find more.
			<-slots
			select {
			case <-time.After(time.Second):
			case <-done:
				return
			}
			continue
		}

		go func(pi *peer_info) {
			defer func() { <-slots }()
			outcome := start_download_worker(pm, pi, t.info_hash, peer_id, len(t.piece_hashes), work_ch, results_ch, done)
			pm.release(pi, outcome)
		}(pi)
	}
}

func (t *torrent_file) download() ([]byte, error) {
	log.Println("starting download for", t.name)
	log.Printf("%d trackers: %v\n", len(t.trackers), t.trackers)

	peer_id, err := new_peer_id()
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	defer close(done)

	pm := new_peer_manager()
	t.run_discovery(pm, peer_id, done)

	work_ch := make(chan *piece_work, len(t.piece_hashes))
	results_ch := make(chan *piece_result)

	for index, hash := range t.piece_hashes {
		length := t.piece_length_at(index)
		work_ch <- &piece_work{index, hash, length}
	}

	go t.fill_peer_slots(pm, peer_id, work_ch, results_ch, done)

	buf := make([]byte, t.length)
	done_pieces := 0

	stall := time.NewTimer(stall_timeout)
	defer stall.Stop()
	table := time.NewTicker(table_interval)
	defer table.Stop()

	for done_pieces < len(t.piece_hashes) {
		select {
		case result := <-results_ch:
			begin := result.index * t.piece_length
			end := begin + len(result.data)
			copy(buf[begin:end], result.data)
			done_pieces++
			stall.Reset(stall_timeout)

			known, active := pm.counts()
			percent := float64(done_pieces) / float64(len(t.piece_hashes)) * 100
			log.Printf("%.2f%% done — piece %d downloaded, %d/%d peers active\n", percent, result.index, active, known)

		case <-table.C:
			log.Printf("peer table:\n%s", pm.table(table_rows))

		case <-stall.C:
			known, _ := pm.counts()
			return nil, fmt.Errorf("no progress for %v at %d/%d pieces (%d peers known)", stall_timeout, done_pieces, len(t.piece_hashes), known)
		}
	}

	log.Printf("final peer table:\n%s", pm.table(table_rows))
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
