package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const block_size = 16384
const max_backlog = 5

const (
	max_active_peers = 30
	stall_timeout    = 2 * time.Minute
	table_interval   = 15 * time.Second
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

type piece_scheduler struct {
	mu           sync.Mutex
	availability []int
	complete     []bool
	active       []bool
}

func new_piece_scheduler(num_pieces int) *piece_scheduler {
	return &piece_scheduler{
		availability: make([]int, num_pieces),
		complete:     make([]bool, num_pieces),
		active:       make([]bool, num_pieces),
	}
}

func (ps *piece_scheduler) add_peer(bf bitfield) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for i := range ps.availability {
		if bf.has_piece(i) {
			ps.availability[i]++
		}
	}
}

func (ps *piece_scheduler) remove_peer(bf bitfield) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for i := range ps.availability {
		if bf.has_piece(i) && ps.availability[i] > 0 {
			ps.availability[i]--
		}
	}
}

func (ps *piece_scheduler) set_complete(index int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if index < 0 || index >= len(ps.complete) {
		return
	}

	ps.complete[index] = true
	ps.active[index] = false
}

func (ps *piece_scheduler) is_complete(index int) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if index < 0 || index >= len(ps.complete) {
		return false
	}

	return ps.complete[index]
}

func (ps *piece_scheduler) next(bf bitfield) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	best := -1
	bestAvailability := int(^uint(0) >> 1)

	for i := range ps.availability {
		if ps.complete[i] {
			continue
		}

		if ps.active[i] {
			continue
		}

		if !bf.has_piece(i) {
			continue
		}

		if ps.availability[i] == 0 {
			continue
		}

		if ps.availability[i] < bestAvailability {
			best = i
			bestAvailability = ps.availability[i]
		}
	}

	if best == -1 {
		return -1
	}

	ps.active[best] = true
	log.Printf("scheduler selected piece %d (availability %d)", best, bestAvailability)
	return best
}

func (ps *piece_scheduler) release(index int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if index < 0 || index >= len(ps.active) {
		return
	}

	if !ps.complete[index] {
		ps.active[index] = false
	}
}

type resume_state struct {
	InfoHash    string `json:"info_hash"`
	Length      int    `json:"length"`
	PieceLength int    `json:"piece_length"`
	Complete    []bool `json:"complete"`
}

func resume_paths(t *torrent_file) (string, string) {
	return t.name + ".part", t.name + ".resume"
}

func load_resume_state(t *torrent_file) ([]bool, error) {
	_, resumePath := resume_paths(t)

	data, err := os.ReadFile(resumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return make([]bool, len(t.piece_hashes)), nil
		}
		return nil, err
	}

	var state resume_state

	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("invalid resume file, starting fresh: %v", err)
		return make([]bool, len(t.piece_hashes)), nil
	}

	expectedHash := fmt.Sprintf("%x", t.info_hash)
	if (state.InfoHash != "" && state.InfoHash != expectedHash) ||
		(state.Length != 0 && state.Length != t.length) ||
		(state.PieceLength != 0 && state.PieceLength != t.piece_length) ||
		len(state.Complete) != len(t.piece_hashes) {
		log.Printf("resume file does not match torrent, starting fresh")
		return make([]bool, len(t.piece_hashes)), nil
	}

	return state.Complete, nil
}

func save_resume_state(t *torrent_file, complete []bool) error {
	_, resumePath := resume_paths(t)

	state := resume_state{
		InfoHash:    fmt.Sprintf("%x", t.info_hash),
		Length:      t.length,
		PieceLength: t.piece_length,
		Complete:    complete,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := resumePath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	if err := os.Remove(resumePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return os.Rename(tmpPath, resumePath)
}

func verify_saved_pieces(t *torrent_file, file *os.File, complete []bool) (int, error) {
	verified := 0

	for index := range complete {
		if !complete[index] {
			continue
		}

		length := t.piece_length_at(index)
		data := make([]byte, length)
		offset := int64(index * t.piece_length)

		n, err := file.ReadAt(data, offset)
		if err != nil || n != length {
			complete[index] = false
			continue
		}

		hash := sha1.Sum(data)

		if !bytes.Equal(hash[:], t.piece_hashes[index][:]) {
			log.Printf("resume: piece %d failed verification, redownloading", index)
			complete[index] = false
			continue
		}

		verified++
	}

	return verified, nil
}

type piece_tracker struct {
	mu       sync.Mutex
	complete []bool
}

func new_piece_tracker(n int) *piece_tracker {
	return &piece_tracker{
		complete: make([]bool, n),
	}
}

func (pt *piece_tracker) done(index int) bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if index < 0 || index >= len(pt.complete) {
		return false
	}

	return pt.complete[index]
}

func (pt *piece_tracker) mark_done(index int) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if index >= 0 && index < len(pt.complete) {
		pt.complete[index] = true
	}
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
		blockLen := block_size
		remaining := piece_length - pp.requested

		if remaining < block_size {
			blockLen = remaining
		}

		if blockLen <= 0 {
			break
		}

		if err := c.send_request(pp.index, pp.requested, blockLen); err != nil {
			return err
		}

		pp.backlog++
		pp.requested += blockLen
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
		log.Printf("unchoked by peer")

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

		if pp.backlog > 0 {
			pp.backlog--
		}
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

func (t *torrent_file) start_download_worker(
	pm *peer_manager,
	pi *peer_info,
	scheduler *piece_scheduler,
	peerID [20]byte,
	resultsCh chan *piece_result,
	done <-chan struct{},
) worker_outcome {
	p := pi.addr

	c, err := new_client(p, t.info_hash, peerID, len(t.piece_hashes))
	if err != nil {
		log.Printf("could not connect to peer %s: %v", p, err)
		return outcome_failed
	}

	defer c.conn.Close()

	pm.mu.Lock()
	pi.bitfield = append(bitfield(nil), c.bitfield...)
	pm.mu.Unlock()

	scheduler.add_peer(c.bitfield)
	defer scheduler.remove_peer(c.bitfield)

	pm.report_connect(pi, c.rtt)

	log.Printf(
		"connected to peer %s (rtt %v)",
		p,
		c.rtt.Round(time.Millisecond),
	)

	if err := c.send_unchoke(); err != nil {
		return outcome_failed
	}

	if err := c.send_interested(); err != nil {
		return outcome_failed
	}

	log.Printf(
		"sent interested to %s, choked=%v",
		p,
		c.choked,
	)

	for {
		select {
		case <-done:
			return outcome_done
		default:
		}

		index := scheduler.next(c.bitfield)

		if index == -1 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		pw := &piece_work{
			index:  index,
			hash:   t.piece_hashes[index],
			length: t.piece_length_at(index),
		}

		start := time.Now()

		data, err := download_piece(c, pw)
		if err != nil {
			scheduler.release(index)

			log.Printf(
				"failed to download piece %d from %s: %v",
				index,
				p,
				err,
			)

			return outcome_failed
		}

		elapsed := time.Since(start)

		if err := check_integrity(pw, data); err != nil {
			scheduler.release(index)

			log.Printf(
				"piece %d from %s failed integrity check: %v",
				index,
				p,
				err,
			)

			if pm.report_piece(pi, len(data), elapsed, false) {
				log.Printf("banning peer %s after %d corrupt pieces", p, max_hash_fails)
				return outcome_failed
			}
			continue
		}

		select {
		case resultsCh <- &piece_result{
			index: index,
			data:  data,
		}:
			pm.report_piece(
				pi,
				len(data),
				elapsed,
				true,
			)

		case <-done:
			scheduler.release(index)
			return outcome_done
		}
	}
}

func (t *torrent_file) fill_peer_slots(
	pm *peer_manager,
	peerID [20]byte,
	scheduler *piece_scheduler,
	resultsCh chan *piece_result,
	done <-chan struct{},
) {
	slots := make(chan struct{}, max_active_peers)

	for {
		select {
		case slots <- struct{}{}:
		case <-done:
			return
		}

		pi := pm.next()

		if pi == nil {
			<-slots

			select {
			case <-time.After(time.Second):
			case <-done:
				return
			}

			continue
		}

		go func(pi *peer_info) {
			defer func() {
				<-slots
			}()

			outcome := t.start_download_worker(
				pm,
				pi,
				scheduler,
				peerID,
				resultsCh,
				done,
			)

			pm.release(pi, outcome)
		}(pi)
	}
}

func (t *torrent_file) download() ([]byte, error) {
	log.Println("starting download for", t.name)
	log.Printf("%d trackers: %v", len(t.trackers), t.trackers)

	peerID, err := new_peer_id()
	if err != nil {
		return nil, err
	}

	partPath, resumePath := resume_paths(t)

	complete, err := load_resume_state(t)
	if err != nil {
		return nil, fmt.Errorf("could not load resume state: %w", err)
	}

	partFile, err := os.OpenFile(
		partPath,
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("could not open partial file: %w", err)
	}
	defer partFile.Close()

	if err := partFile.Truncate(int64(t.length)); err != nil {
		return nil, fmt.Errorf("could not prepare partial file: %w", err)
	}

	verified, err := verify_saved_pieces(
		t,
		partFile,
		complete,
	)
	if err != nil {
		return nil, fmt.Errorf("could not verify resume data: %w", err)
	}

	donePieces := verified

	log.Printf(
		"resume: %d/%d pieces already complete",
		donePieces,
		len(t.piece_hashes),
	)

	if err := save_resume_state(t, complete); err != nil {
		return nil, fmt.Errorf("could not save resume state: %w", err)
	}

	if donePieces == len(t.piece_hashes) {
		log.Println("all pieces already downloaded")

		buf := make([]byte, t.length)

		if _, err := partFile.ReadAt(buf, 0); err != nil {
			return nil, err
		}

		return buf, nil
	}

	done := make(chan struct{})
	defer close(done)

	pm := new_peer_manager()

	t.run_discovery(
		pm,
		peerID,
		done,
	)

	scheduler := new_piece_scheduler(
		len(t.piece_hashes),
	)

	for index, value := range complete {
		if value {
			scheduler.set_complete(index)
		}
	}

	resultsCh := make(chan *piece_result, max_active_peers)

	go t.fill_peer_slots(
		pm,
		peerID,
		scheduler,
		resultsCh,
		done,
	)

	stall := time.NewTimer(stall_timeout)
	defer stall.Stop()

	table := time.NewTicker(table_interval)
	defer table.Stop()

	for donePieces < len(t.piece_hashes) {
		select {
		case result := <-resultsCh:
			if result == nil {
				continue
			}

			if result.index < 0 ||
				result.index >= len(t.piece_hashes) {
				continue
			}

			if complete[result.index] {
				scheduler.set_complete(result.index)
				continue
			}

			expectedLength := t.piece_length_at(result.index)

			if len(result.data) != expectedLength {
				scheduler.release(result.index)

				log.Printf(
					"piece %d has invalid length: got %d, expected %d",
					result.index,
					len(result.data),
					expectedLength,
				)

				continue
			}

			pw := &piece_work{
				index:  result.index,
				hash:   t.piece_hashes[result.index],
				length: expectedLength,
			}

			if err := check_integrity(pw, result.data); err != nil {
				scheduler.release(result.index)

				log.Printf(
					"piece %d failed final integrity check: %v",
					result.index,
					err,
				)

				continue
			}

			offset := int64(
				result.index * t.piece_length,
			)

			n, err := partFile.WriteAt(
				result.data,
				offset,
			)
			if err != nil {
				scheduler.release(result.index)

				return nil, fmt.Errorf(
					"could not save piece %d: %w",
					result.index,
					err,
				)
			}

			if n != len(result.data) {
				scheduler.release(result.index)

				return nil, fmt.Errorf(
					"short write for piece %d",
					result.index,
				)
			}

			if err := partFile.Sync(); err != nil {
				scheduler.release(result.index)

				return nil, fmt.Errorf(
					"could not sync piece %d: %w",
					result.index,
					err,
				)
			}

			complete[result.index] = true
			scheduler.set_complete(result.index)
			donePieces++

			if err := save_resume_state(
				t,
				complete,
			); err != nil {
				return nil, fmt.Errorf(
					"could not save resume state: %w",
					err,
				)
			}

			if !stall.Stop() {
				select {
				case <-stall.C:
				default:
				}
			}

			stall.Reset(stall_timeout)

			known, active := pm.counts()

			percent :=
				float64(donePieces) /
					float64(len(t.piece_hashes)) *
					100

			log.Printf(
				"%.2f%% done — piece %d saved — %d/%d peers active",
				percent,
				result.index,
				active,
				known,
			)

		case <-table.C:
			log.Printf(
				"peer table:\n%s",
				pm.table(table_rows),
			)

		case <-stall.C:
			known, _ := pm.counts()

			return nil, fmt.Errorf(
				"no progress for %v at %d/%d pieces (%d peers known)",
				stall_timeout,
				donePieces,
				len(t.piece_hashes),
				known,
			)
		}
	}

	log.Println("all pieces downloaded successfully")

	log.Printf(
		"final peer table:\n%s",
		pm.table(table_rows),
	)

	buf := make([]byte, t.length)

	if _, err := partFile.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf(
			"could not read completed file: %w",
			err,
		)
	}

	log.Printf("download complete: %s", partPath)
	log.Printf("resume state: %s", resumePath)

	return buf, nil
}

func format_duration(d time.Duration) string {
	if d <= 0 {
		return "calculating..."
	}

	d = d.Round(time.Second)

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf(
			"%dh %dm %ds",
			hours,
			minutes,
			seconds,
		)
	}

	if minutes > 0 {
		return fmt.Sprintf(
			"%dm %ds",
			minutes,
			seconds,
		)
	}

	return fmt.Sprintf(
		"%ds",
		seconds,
	)
}

func (t *torrent_file) piece_length_at(index int) int {
	begin := index * t.piece_length
	end := begin + t.piece_length

	if end > t.length {
		end = t.length
	}

	return end - begin
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
			if misses++; misses > len(work_ch) {
				return outcome_useless
			}
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
