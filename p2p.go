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

// fill_requests keeps up to max_backlog block requests outstanding.
func (pp *piece_progress) fill_requests(c *client, piece_length int) error {
	for pp.backlog < max_backlog && pp.requested < piece_length {
		block_len := block_size

		remaining := piece_length - pp.requested
		if remaining < block_size {
			block_len = remaining
		}

		if err := c.send_request(
			pp.index,
			pp.requested,
			block_len,
		); err != nil {
			return err
		}

		pp.backlog++
		pp.requested += block_len
	}

	return nil
}

// handle_message processes messages received from the peer.
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
		log.Printf("unchoked by peer")

	case msg_choke:
		c.choked = true

		// Outstanding requests are no longer useful.
		pp.requested = pp.downloaded
		pp.backlog = 0

	case msg_bitfield:
		// A peer can send a bitfield after the handshake.
		c.bitfield = msg.payload

	case msg_have:
		index, err := parse_have(msg)
		if err != nil {
			return err
		}

		c.bitfield.set_piece(index)

	case msg_piece:
		n, err := parse_piece(
			pp.index,
			pp.buf,
			msg,
		)
		if err != nil {
			return err
		}

		pp.downloaded += n

		if pp.backlog > 0 {
			pp.backlog--
		}
	}

	return nil
}

// download_piece downloads one complete torrent piece.
func download_piece(c *client, pw *piece_work) ([]byte, error) {
	pp := &piece_progress{
		index: pw.index,
		buf:   make([]byte, pw.length),
	}

	// Prevent a dead peer from blocking forever.
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

// check_integrity verifies the SHA-1 hash of a downloaded piece.
func check_integrity(pw *piece_work, data []byte) error {
	hash := sha1.Sum(data)

	if !bytes.Equal(hash[:], pw.hash[:]) {
		return fmt.Errorf(
			"piece %d failed integrity check",
			pw.index,
		)
	}

	return nil
}

// start_download_worker handles downloading pieces from one peer.
func start_download_worker(
	p peer,
	info_hash [20]byte,
	peer_id [20]byte,
	num_pieces int,
	work_ch chan *piece_work,
	results_ch chan *piece_result,
) {
	c, err := new_client(
		p,
		info_hash,
		peer_id,
		num_pieces,
	)

	if err != nil {
		log.Printf(
			"could not connect to peer %s: %v",
			p,
			err,
		)
		return
	}

	defer c.conn.Close()

	log.Printf("connected to peer %s", p)

	if err := c.send_unchoke(); err != nil {
		return
	}

	if err := c.send_interested(); err != nil {
		return
	}

	log.Printf(
		"sent interested to %s, choked=%v",
		p,
		c.choked,
	)

	// Prevent a worker from endlessly cycling through
	// pieces that this particular peer does not have.
	misses := 0

	for pw := range work_ch {

		// Check whether this peer owns the piece.
		if !c.bitfield.has_piece(pw.index) {
			work_ch <- pw

			misses++

			if misses > num_pieces {
				log.Printf(
					"peer %s has too few requested pieces; leaving",
					p,
				)
				return
			}

			time.Sleep(200 * time.Millisecond)
			continue
		}

		misses = 0

		data, err := download_piece(c, pw)
		if err != nil {
			work_ch <- pw

			log.Printf(
				"failed to download piece %d from %s: %v",
				pw.index,
				p,
				err,
			)

			return
		}

		// Verify the piece before giving it to the downloader.
		if err := check_integrity(pw, data); err != nil {
			work_ch <- pw

			log.Printf(
				"piece %d from %s failed integrity check",
				pw.index,
				p,
			)

			continue
		}

		results_ch <- &piece_result{
			index: pw.index,
			data:  data,
		}
	}
}

// printProgress displays the current download progress.
func printProgress(done, total int, peers int32) {
	if total == 0 {
		return
	}

	percent := float64(done) / float64(total) * 100

	barWidth := 30
	filled := int(percent / 100 * float64(barWidth))

	if filled > barWidth {
		filled = barWidth
	}

	bar := ""

	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar += "█"
		} else {
			bar += "░"
		}
	}

	fmt.Printf(
		"\rDownloading: [%s] %6.2f%% | %d/%d pieces | %d peers",
		bar,
		percent,
		done,
		total,
		peers,
	)

	if done == total {
		fmt.Println()
	}
}

// download downloads the complete torrent.
func (t *torrent_file) download() ([]byte, error) {
	log.Println("starting download for", t.name)

	peerID, err := new_peer_id()
	if err != nil {
		return nil, err
	}

	// Ask the tracker for peers.
	peers, err := t.request_peers(peerID, 6881)
	if err != nil {
		return nil, err
	}

	log.Printf(
		"got %d peers from tracker",
		len(peers),
	)

	if len(peers) == 0 {
		return nil, fmt.Errorf(
			"tracker returned no peers",
		)
	}

	// Work queue containing every piece.
	workCh := make(chan *piece_work, len(t.piece_hashes))

	// Results from workers.
	resultsCh := make(chan *piece_result)

	// Add all pieces to the work queue.
	for index, hash := range t.piece_hashes {
		length := t.piece_length_at(index)

		workCh <- &piece_work{
			index:  index,
			hash:   hash,
			length: length,
		}
	}

	// Number of workers still running.
	alive := int32(len(peers))

	// Closed when every worker has exited.
	dead := make(chan struct{})

	// Start one worker per peer.
	for _, p := range peers {
		go func(p peer) {

			defer func() {
				if atomic.AddInt32(&alive, -1) == 0 {
					close(dead)
				}
			}()

			start_download_worker(
				p,
				t.info_hash,
				peerID,
				len(t.piece_hashes),
				workCh,
				resultsCh,
			)

		}(p)
	}

	// Allocate the complete output file.
	buf := make([]byte, t.length)

	donePieces := 0
	totalPieces := len(t.piece_hashes)

	for donePieces < totalPieces {

		select {

		// A worker successfully downloaded a piece.
		case result := <-resultsCh:

			if result == nil {
				continue
			}

			begin := result.index * t.piece_length
			end := begin + len(result.data)

			// Safety check for the piece index.
			if begin < 0 || begin >= len(buf) {
				return nil, fmt.Errorf(
					"invalid piece index %d",
					result.index,
				)
			}

			if end > len(buf) {
				end = len(buf)
			}

			// Copy the piece into its correct position.
			copy(
				buf[begin:end],
				result.data,
			)

			donePieces++

			printProgress(
				donePieces,
				totalPieces,
				atomic.LoadInt32(&alive),
			)

		// Every peer has failed or exited.
		case <-dead:

			return nil, fmt.Errorf(
				"all peers exhausted at %d/%d pieces",
				donePieces,
				totalPieces,
			)
		}
	}

	log.Printf(
		"download complete: %d/%d pieces",
		donePieces,
		totalPieces,
	)

	return buf, nil
}

// piece_length_at returns the actual size of a piece.
// The final piece can be smaller than piece_length.
func (t *torrent_file) piece_length_at(index int) int {
	begin := index * t.piece_length
	end := begin + t.piece_length

	if end > t.length {
		end = t.length
	}

	return end - begin
}
