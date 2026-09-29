package main

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestUTPAckTracking(t *testing.T) {
	u := &utpConn{
		unacked: make(map[uint16]*utpSegment),
	}

	u.unacked[100] = &utpSegment{packet: &utpPacket{SequenceNumber: 100}, sentAt: time.Now(), attempts: 1}
	u.unacked[101] = &utpSegment{packet: &utpPacket{SequenceNumber: 101}, sentAt: time.Now(), attempts: 1}
	u.unacked[102] = &utpSegment{packet: &utpPacket{SequenceNumber: 102}, sentAt: time.Now(), attempts: 1}

	if len(u.unacked) != 3 {
		t.Fatalf("expected 3 unacked packets, got %d", len(u.unacked))
	}

	// Cumulative ACK for 101 should remove 100 and 101, leaving 102
	u.processAck(101)

	if len(u.unacked) != 1 {
		t.Fatalf("expected 1 unacked packet left, got %d", len(u.unacked))
	}

	if _, exists := u.unacked[102]; !exists {
		t.Fatalf("expected packet 102 to still be unacked")
	}

	// ACK for 102 clears remaining
	u.processAck(102)

	if len(u.unacked) != 0 {
		t.Fatalf("expected 0 unacked packets, got %d", len(u.unacked))
	}
}

func TestUTPAckWraparound(t *testing.T) {
	u := &utpConn{
		unacked: make(map[uint16]*utpSegment),
	}

	u.unacked[65535] = &utpSegment{packet: &utpPacket{SequenceNumber: 65535}, sentAt: time.Now(), attempts: 1}
	u.unacked[0] = &utpSegment{packet: &utpPacket{SequenceNumber: 0}, sentAt: time.Now(), attempts: 1}
	u.unacked[1] = &utpSegment{packet: &utpPacket{SequenceNumber: 1}, sentAt: time.Now(), attempts: 1}

	// ACK for 0 should clear 65535 and 0, leaving 1
	u.processAck(0)

	if len(u.unacked) != 1 {
		t.Fatalf("expected 1 unacked packet after wraparound ACK, got %d", len(u.unacked))
	}

	if _, exists := u.unacked[1]; !exists {
		t.Fatalf("expected packet 1 to remain unacked")
	}
}

func TestUTPOutOfOrderDelivery(t *testing.T) {
	u := &utpConn{
		expectedSeq: 100,
		recvBuffer:  make(map[uint16][]byte),
	}

	p100 := &utpPacket{SequenceNumber: 100, Payload: []byte("hello ")}
	p101 := &utpPacket{SequenceNumber: 101, Payload: []byte("world ")}
	p102 := &utpPacket{SequenceNumber: 102, Payload: []byte("from uTP")}

	// Receive 100 (in order)
	d100, err := u.handleDataPacket(p100)
	if err != nil || !bytes.Equal(d100, []byte("hello ")) {
		t.Fatalf("expected 'hello ', got '%s' (err: %v)", d100, err)
	}
	if u.expectedSeq != 101 {
		t.Fatalf("expected expectedSeq=101, got %d", u.expectedSeq)
	}

	// Receive 102 (out of order, arrives before 101)
	d102, err := u.handleDataPacket(p102)
	if err != nil || len(d102) != 0 {
		t.Fatalf("expected empty delivered for out-of-order packet 102, got '%s'", d102)
	}
	if len(u.recvBuffer) != 1 {
		t.Fatalf("expected packet 102 to be buffered in recvBuffer")
	}

	// Receive duplicate of 100 (should be dropped, not re-delivered)
	dup100, err := u.handleDataPacket(p100)
	if err != nil || len(dup100) != 0 {
		t.Fatalf("expected duplicate packet to return nil delivered data")
	}

	// Receive missing 101 (should deliver both 101 and buffered 102 together)
	d101_102, err := u.handleDataPacket(p101)
	if err != nil {
		t.Fatalf("unexpected error delivering 101: %v", err)
	}

	expectedCombined := []byte("world from uTP")
	if !bytes.Equal(d101_102, expectedCombined) {
		t.Fatalf("expected '%s', got '%s'", expectedCombined, d101_102)
	}

	if u.expectedSeq != 103 {
		t.Fatalf("expected expectedSeq=103, got %d", u.expectedSeq)
	}

	if len(u.recvBuffer) != 0 {
		t.Fatalf("expected recvBuffer to be empty after flushing consecutive packets")
	}
}

func TestRetransmit_AckArrivesNormally(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		rto:        100 * time.Millisecond,
		maxRetries: 3,
	}

	u.unacked[10] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 10},
		sentAt:   time.Now(),
		attempts: 1,
	}

	// ACK arrives normally before timeout
	u.processAck(10)

	if len(u.unacked) != 0 {
		t.Fatalf("expected packet 10 to be acknowledged, got %d unacked", len(u.unacked))
	}

	// checkRetransmit should do nothing
	if err := u.checkRetransmit(); err != nil {
		t.Fatalf("expected no error from checkRetransmit: %v", err)
	}
}

func TestRetransmit_AckLost(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		rto:        50 * time.Millisecond,
		maxRetries: 3,
	}

	// Packet sent in the past exceeding RTO
	past := time.Now().Add(-100 * time.Millisecond)
	u.unacked[20] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 20},
		sentAt:   past,
		attempts: 1,
	}

	// Trigger retransmission check
	if err := u.checkRetransmit(); err != nil {
		t.Fatalf("expected no error during retransmission: %v", err)
	}

	seg := u.unacked[20]
	if seg == nil {
		t.Fatalf("expected packet 20 to still be in unacked")
	}

	if seg.attempts != 2 {
		t.Fatalf("expected attempts=2 after retransmit, got %d", seg.attempts)
	}

	if !seg.sentAt.After(past) {
		t.Fatalf("expected sentAt to be updated upon retransmission")
	}
}

func TestRetransmit_AckAfterRetransmit(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		rto:        50 * time.Millisecond,
		maxRetries: 3,
	}

	u.unacked[20] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 20},
		sentAt:   time.Now().Add(-100 * time.Millisecond),
		attempts: 1,
	}

	// Retransmit once
	if err := u.checkRetransmit(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.unacked[20].attempts != 2 {
		t.Fatalf("expected attempts=2")
	}

	// ACK arrives after retransmission
	u.processAck(20)

	if len(u.unacked) != 0 {
		t.Fatalf("expected unacked to be empty after receiving ACK")
	}
}

func TestRetransmit_RepeatedFailure(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		rto:        50 * time.Millisecond,
		maxRetries: 3,
	}

	// Segment already reached maxRetries
	u.unacked[30] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 30},
		sentAt:   time.Now().Add(-100 * time.Millisecond),
		attempts: 3,
	}

	err := u.checkRetransmit()
	if err == nil {
		t.Fatalf("expected timeout error when exceeding max attempts")
	}

	if !u.isClosed {
		t.Fatalf("expected connection to be marked closed on repeated failure")
	}
}

func TestAdaptiveRTO_FastAndSlowPeers(t *testing.T) {
	// Fast peer: RTT ~30ms
	uFast := &utpConn{
		minRTO: 50 * time.Millisecond,
		maxRTO: 3 * time.Second,
	}
	uFast.updateRTT(30 * time.Millisecond)

	// Slow peer: RTT ~300ms
	uSlow := &utpConn{
		minRTO: 50 * time.Millisecond,
		maxRTO: 3 * time.Second,
	}
	uSlow.updateRTT(300 * time.Millisecond)

	if uFast.rto >= uSlow.rto {
		t.Fatalf("expected fast peer RTO (%v) to be smaller than slow peer RTO (%v)", uFast.rto, uSlow.rto)
	}

	if uFast.rto < 50*time.Millisecond || uFast.rto > 200*time.Millisecond {
		t.Fatalf("unexpected fast peer RTO: %v", uFast.rto)
	}

	if uSlow.rto < 500*time.Millisecond {
		t.Fatalf("unexpected slow peer RTO: %v", uSlow.rto)
	}
}

func TestAdaptiveRTO_KarnAlgorithm(t *testing.T) {
	u := &utpConn{
		unacked: make(map[uint16]*utpSegment),
		rto:     500 * time.Millisecond,
		minRTO:  50 * time.Millisecond,
		maxRTO:  3 * time.Second,
	}

	// Segment that was retransmitted (attempts = 2)
	u.unacked[50] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 50},
		sentAt:   time.Now().Add(-200 * time.Millisecond),
		attempts: 2,
	}

	// ACK arrives for retransmitted segment
	u.processAck(50)

	// Segment must be removed
	if len(u.unacked) != 0 {
		t.Fatalf("expected packet 50 to be deleted from unacked")
	}

	// But SRTT must NOT be updated from this retransmission
	if u.srtt != 0 {
		t.Fatalf("expected srtt to remain 0 under Karn's algorithm, got %v", u.srtt)
	}
	if u.rto != 500*time.Millisecond {
		t.Fatalf("expected rto to remain default 500ms, got %v", u.rto)
	}
}

func TestAdaptiveRTO_Bounds(t *testing.T) {
	u := &utpConn{
		minRTO: 100 * time.Millisecond,
		maxRTO: 2 * time.Second,
	}

	// Sub-millisecond or very small RTT
	u.updateRTT(2 * time.Millisecond)
	if u.rto != 100*time.Millisecond {
		t.Fatalf("expected RTO to be clamped to minRTO (100ms), got %v", u.rto)
	}

	// Enormous RTT sample
	u.updateRTT(10 * time.Second)
	if u.rto != 2*time.Second {
		t.Fatalf("expected RTO to be clamped to maxRTO (2s), got %v", u.rto)
	}
}

func TestFlowControl_AdvertisedWindow(t *testing.T) {
	u := &utpConn{
		recvWindowMax: 64 * 1024,
		recvBuffer:    make(map[uint16][]byte),
	}

	// Initially empty: should advertise 64 KB
	if win := u.advertisedWindowLocked(); win != 64*1024 {
		t.Fatalf("expected empty buffer window 65536, got %d", win)
	}

	// Add 14 KB to readBuf
	u.readBuf = make([]byte, 14*1024)
	if win := u.advertisedWindowLocked(); win != 50*1024 {
		t.Fatalf("expected 50 KB remaining window, got %d", win)
	}

	// Add 20 KB to recvBuffer
	u.recvBuffer[10] = make([]byte, 20*1024)
	if win := u.advertisedWindowLocked(); win != 30*1024 {
		t.Fatalf("expected 30 KB remaining window, got %d", win)
	}

	// Fill buffer completely
	u.readBuf = make([]byte, 64*1024)
	if win := u.advertisedWindowLocked(); win != 0 {
		t.Fatalf("expected 0 window when full, got %d", win)
	}
}

func TestFlowControl_SenderThrottling(t *testing.T) {
	u := &utpConn{
		peerWindow: 100,
		unacked:    make(map[uint16]*utpSegment),
	}
	u.windowCond = sync.NewCond(&u.mu)

	// Fill peerWindow with 100 bytes unacked
	u.unacked[1] = &utpSegment{
		packet: &utpPacket{
			SequenceNumber: 1,
			Payload:        make([]byte, 100),
		},
		sentAt:   time.Now(),
		attempts: 1,
	}

	unblocked := make(chan struct{})
	go func() {
		// Attempt to write 50 more bytes (should block until window freed)
		_, _ = u.Write(make([]byte, 50))
		close(unblocked)
	}()

	select {
	case <-unblocked:
		t.Fatalf("expected Write to block when peer window is exhausted")
	case <-time.After(50 * time.Millisecond):
		// Write correctly blocked
	}

	// Acknowledge packet 1, freeing the 100 bytes
	u.processAck(1)

	select {
	case <-unblocked:
		// Successfully unblocked after ACK
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for Write to unblock after ACK")
	}
}

func TestCongestionControl_WindowIncrease(t *testing.T) {
	u := &utpConn{
		cwnd:     3000,
		minCwnd:  3000,
		ssthresh: 64 * 1024,
		unacked:  make(map[uint16]*utpSegment),
	}

	// 1000-byte unacked segment
	u.unacked[1] = &utpSegment{
		packet: &utpPacket{
			SequenceNumber: 1,
			Payload:        make([]byte, 1000),
		},
		sentAt:   time.Now(),
		attempts: 1,
	}

	u.processAck(1)

	// In slow start (cwnd < ssthresh), cwnd should increase by ackedBytes (1000)
	if u.cwnd != 4000 {
		t.Fatalf("expected cwnd=4000 after slow-start ACK, got %d", u.cwnd)
	}
}

func TestCongestionControl_LossDecrease(t *testing.T) {
	u := &utpConn{
		cwnd:     10000,
		minCwnd:  3000,
		ssthresh: 64 * 1024,
		rto:      50 * time.Millisecond,
		unacked:  make(map[uint16]*utpSegment),
	}

	// Segment that times out
	u.unacked[1] = &utpSegment{
		packet: &utpPacket{
			SequenceNumber: 1,
			Payload:        make([]byte, 1000),
		},
		sentAt:   time.Now().Add(-100 * time.Millisecond),
		attempts: 1,
	}

	err := u.checkRetransmit()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ssthresh should be halved (10000 / 2 = 5000)
	if u.ssthresh != 5000 {
		t.Fatalf("expected ssthresh=5000 on loss, got %d", u.ssthresh)
	}

	// cwnd should drop to minCwnd (3000)
	if u.cwnd != 3000 {
		t.Fatalf("expected cwnd to drop to minCwnd=3000, got %d", u.cwnd)
	}
}

func TestCongestionControl_EffectiveWindow(t *testing.T) {
	u := &utpConn{
		peerWindow: 10000,
		cwnd:       2000,
	}

	// effectiveWindow is min(peerWindow, cwnd)
	if eff := u.effectiveWindowLocked(); eff != 2000 {
		t.Fatalf("expected effective window 2000, got %d", eff)
	}

	u.peerWindow = 1500
	u.cwnd = 5000
	if eff := u.effectiveWindowLocked(); eff != 1500 {
		t.Fatalf("expected effective window 1500, got %d", eff)
	}
}

func TestShutdown_CloseIdempotent(t *testing.T) {
	u := &utpConn{
		done: make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	if err := u.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	if err := u.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	// Writes after close should fail
	_, err := u.Write([]byte("test"))
	if err == nil {
		t.Fatalf("expected write after close to fail")
	}
}

func TestShutdown_RecvEOF(t *testing.T) {
	u := &utpConn{
		recvEOF: true,
	}

	buf := make([]byte, 10)
	n, err := u.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("expected Read to return (0, io.EOF), got (%d, %v)", n, err)
	}
}

func TestShutdown_RecvEOFWithRemainingData(t *testing.T) {
	u := &utpConn{
		recvEOF: false,
		readBuf: []byte("remaining"),
	}

	buf := make([]byte, 20)
	n, err := u.Read(buf)
	if err != nil || string(buf[:n]) != "remaining" {
		t.Fatalf("expected to read remaining data first, got (%d, %v, %s)", n, err, string(buf[:n]))
	}

	u.recvEOF = true
	n, err = u.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("expected io.EOF on subsequent read, got (%d, %v)", n, err)
	}
}

func TestUTPPacketMarshalUnmarshal(t *testing.T) {
	types := []uint8{utpTypeST_DATA, utpTypeST_FIN, utpTypeST_STATE, utpTypeST_RESET, utpTypeST_SYN}

	for _, pt := range types {
		orig := &utpPacket{
			Type:           pt,
			Version:        utpVersion,
			Extension:      0,
			ConnectionID:   12345,
			Timestamp:      999999,
			TimestampDiff:  1234,
			WindowSize:     65536,
			SequenceNumber: 42,
			AckNumber:      41,
			Payload:        []byte("payload-data-test"),
		}

		raw := orig.marshal()
		decoded, err := unmarshalUTPPacket(raw)
		if err != nil {
			t.Fatalf("failed to unmarshal type %d: %v", pt, err)
		}

		if decoded.Type != orig.Type ||
			decoded.Version != orig.Version ||
			decoded.ConnectionID != orig.ConnectionID ||
			decoded.SequenceNumber != orig.SequenceNumber ||
			decoded.AckNumber != orig.AckNumber ||
			decoded.WindowSize != orig.WindowSize ||
			!bytes.Equal(decoded.Payload, orig.Payload) {
			t.Fatalf("mismatch for packet type %d: orig %+v vs decoded %+v", pt, orig, decoded)
		}
	}
}

func TestUTPHandshake(t *testing.T) {
	udpListener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to listen UDP: %v", err)
	}
	defer udpListener.Close()

	addr := udpListener.LocalAddr().(*net.UDPAddr)

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 1500)
		n, remote, err := udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}

		pkt, err := unmarshalUTPPacket(buf[:n])
		if err != nil {
			return
		}

		if pkt.Type != utpTypeST_SYN {
			return
		}

		// Respond with STATE
		resp := &utpPacket{
			Type:           utpTypeST_STATE,
			Version:        utpVersion,
			ConnectionID:   pkt.ConnectionID,
			Timestamp:      uint32(time.Now().UnixMicro()),
			WindowSize:     64 * 1024,
			SequenceNumber: 100,
			AckNumber:      pkt.SequenceNumber,
		}
		udpListener.WriteToUDP(resp.marshal(), remote)
	}()

	p := peer{
		ip:   addr.IP.To4(),
		port: uint16(addr.Port),
	}

	conn, err := dialUTP(p)
	if err != nil {
		t.Fatalf("dialUTP handshake failed: %v", err)
	}
	defer conn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for handshake test peer")
	}
}

func TestUTPReadWriteLoop(t *testing.T) {
	udpListener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to listen UDP: %v", err)
	}
	defer udpListener.Close()

	addr := udpListener.LocalAddr().(*net.UDPAddr)

	go func() {
		buf := make([]byte, 1500)
		// 1. Handshake SYN -> STATE
		n, remote, err := udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		pkt, _ := unmarshalUTPPacket(buf[:n])
		resp := &utpPacket{
			Type:           utpTypeST_STATE,
			Version:        utpVersion,
			ConnectionID:   pkt.ConnectionID,
			WindowSize:     64 * 1024,
			SequenceNumber: 100,
			AckNumber:      pkt.SequenceNumber,
		}
		udpListener.WriteToUDP(resp.marshal(), remote)

		// 2. Read DATA "ping"
		n, remote, err = udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		dataPkt, _ := unmarshalUTPPacket(buf[:n])
		if string(dataPkt.Payload) == "ping" {
			// Reply with DATA "pong"
			pongPkt := &utpPacket{
				Type:           utpTypeST_DATA,
				Version:        utpVersion,
				ConnectionID:   pkt.ConnectionID,
				WindowSize:     64 * 1024,
				SequenceNumber: 101,
				AckNumber:      dataPkt.SequenceNumber,
				Payload:        []byte("pong"),
			}
			udpListener.WriteToUDP(pongPkt.marshal(), remote)
		}
	}()

	p := peer{
		ip:   addr.IP.To4(),
		port: uint16(addr.Port),
	}

	conn, err := dialUTP(p)
	if err != nil {
		t.Fatalf("dialUTP failed: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("conn.Write failed: %v", err)
	}

	recvBuf := make([]byte, 10)
	n, err := conn.Read(recvBuf)
	if err != nil {
		t.Fatalf("conn.Read failed: %v", err)
	}

	if string(recvBuf[:n]) != "pong" {
		t.Fatalf("expected 'pong', got '%s'", string(recvBuf[:n]))
	}
}

func TestBitTorrentOverUTP(t *testing.T) {
	udpListener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to listen UDP: %v", err)
	}
	defer udpListener.Close()

	addr := udpListener.LocalAddr().(*net.UDPAddr)

	var fakeInfoHash [20]byte
	copy(fakeInfoHash[:], "0123456789abcdefghij")

	var fakePeerID [20]byte
	copy(fakePeerID[:], "-UT2026-fakepeer1234")

	go func() {
		buf := make([]byte, 1500)
		// 1. Handshake SYN -> STATE
		n, remote, err := udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		synPkt, _ := unmarshalUTPPacket(buf[:n])
		stateResp := &utpPacket{
			Type:           utpTypeST_STATE,
			Version:        utpVersion,
			ConnectionID:   synPkt.ConnectionID,
			WindowSize:     64 * 1024,
			SequenceNumber: 50,
			AckNumber:      synPkt.SequenceNumber,
		}
		udpListener.WriteToUDP(stateResp.marshal(), remote)

		// 2. Read BitTorrent handshake over uTP DATA
		n, remote, err = udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		dataPkt, _ := unmarshalUTPPacket(buf[:n])
		payload := dataPkt.Payload
		// Parse handshake: 1 byte pstrlen, pstrlen bytes pstr, 8 bytes reserved, 20 bytes infohash, 20 bytes peerid
		if len(payload) < 1 {
			return
		}
		pstrLen := int(payload[0])
		if len(payload) < 1+pstrLen+48 {
			return
		}
		var clientInfoHash [20]byte
		copy(clientInfoHash[:], payload[1+pstrLen+8:1+pstrLen+28])
		if clientInfoHash != fakeInfoHash {
			return
		}

		// Reply with server BitTorrent handshake over uTP DATA
		peerHs := new_handshake(fakeInfoHash, fakePeerID)
		hsDataPkt := &utpPacket{
			Type:           utpTypeST_DATA,
			Version:        utpVersion,
			ConnectionID:   synPkt.ConnectionID,
			WindowSize:     64 * 1024,
			SequenceNumber: 51,
			AckNumber:      dataPkt.SequenceNumber,
			Payload:        peerHs.serialize(),
		}
		udpListener.WriteToUDP(hsDataPkt.marshal(), remote)
	}()

	p := peer{
		ip:   addr.IP.To4(),
		port: uint16(addr.Port),
	}

	c, err := new_client(p, fakeInfoHash, fakePeerID, 5, transport_utp)
	if err != nil {
		t.Fatalf("BitTorrent over uTP handshake failed: %v", err)
	}
	defer c.conn.Close()

	if c.transport != transport_utp {
		t.Fatalf("expected client transport to be transport_utp, got %v", c.transport)
	}
}

// ─── 9. Packet loss simulation ────────────────────────────────────────────────

// TestPacketLoss_DroppedDataRetransmits verifies that when a DATA packet is
// silently dropped the sender retransmits it and the receiver eventually
// delivers the payload.
func TestPacketLoss_DroppedDataRetransmits(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		recvBuffer: make(map[uint16][]byte),
		rto:        50 * time.Millisecond,
		maxRetries: 5,
		done:       make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	// Simulate a sent but not yet ACKed segment (the "lost" packet)
	seg := &utpSegment{
		packet:   &utpPacket{SequenceNumber: 10, Payload: []byte("hello")},
		sentAt:   time.Now().Add(-100 * time.Millisecond), // already past RTO
		attempts: 1,
	}
	u.unacked[10] = seg

	// One retransmission cycle should increment attempts (no real UDP conn here)
	err := u.checkRetransmit()
	if err != nil {
		t.Fatalf("unexpected error on first retransmit check: %v", err)
	}

	u.mu.Lock()
	if u.unacked[10].attempts != 2 {
		t.Fatalf("expected attempts=2 after retransmit, got %d", u.unacked[10].attempts)
	}
	u.mu.Unlock()

	// Simulate ACK arriving after retransmission — packet removed
	u.processAck(10)

	u.mu.Lock()
	if len(u.unacked) != 0 {
		t.Fatalf("expected unacked to be empty after ACK, got %d entries", len(u.unacked))
	}
	u.mu.Unlock()
}

// TestPacketLoss_MaxRetriesExceeded verifies the connection is marked failed
// after exceeding maxRetries.
func TestPacketLoss_MaxRetriesExceeded(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		recvBuffer: make(map[uint16][]byte),
		rto:        10 * time.Millisecond,
		maxRetries: 3,
		done:       make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	u.unacked[20] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 20, Payload: []byte("data")},
		sentAt:   time.Now().Add(-50 * time.Millisecond),
		attempts: 3, // already at limit
	}

	err := u.checkRetransmit()
	if err == nil {
		t.Fatal("expected connection error after max retries, got nil")
	}

	u.mu.Lock()
	if !u.isClosed {
		t.Fatal("expected isClosed=true after max retries")
	}
	u.mu.Unlock()
}

// TestPacketLoss_NoDuplicateDelivery verifies that a retransmitted DATA packet
// already received is not delivered twice by the ordering layer.
func TestPacketLoss_NoDuplicateDelivery(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 5,
	}

	pkt := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		SequenceNumber: 5,
		Payload:        []byte("once"),
	}

	// First delivery
	delivered1, err := u.handleDataPacket(pkt)
	if err != nil || string(delivered1) != "once" {
		t.Fatalf("first delivery failed: %v / %q", err, delivered1)
	}

	// Retransmit (duplicate) — should return nil payload, no duplicate
	delivered2, err := u.handleDataPacket(pkt)
	if err != nil {
		t.Fatalf("unexpected error on duplicate: %v", err)
	}
	if len(delivered2) != 0 {
		t.Fatalf("duplicate packet should not be delivered, got %q", delivered2)
	}
}

// ─── 10. Packet reordering simulation ────────────────────────────────────────

// TestReorder_OutOfOrderThenFill verifies that out-of-order packets are buffered
// and flushed in order once the missing packet arrives.
func TestReorder_OutOfOrderThenFill(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 1,
	}

	pkt := func(seq uint16, payload string) *utpPacket {
		return &utpPacket{
			Type:           utpTypeST_DATA,
			Version:        utpVersion,
			SequenceNumber: seq,
			Payload:        []byte(payload),
		}
	}

	// Deliver seq=1 (in order)
	d, _ := u.handleDataPacket(pkt(1, "A"))
	if string(d) != "A" {
		t.Fatalf("seq=1: expected 'A', got %q", d)
	}

	// seq=3 arrives before seq=2 — buffered
	d, _ = u.handleDataPacket(pkt(3, "C"))
	if len(d) != 0 {
		t.Fatalf("seq=3 should be buffered, got %q", d)
	}

	// seq=4 arrives before seq=2 — also buffered
	d, _ = u.handleDataPacket(pkt(4, "D"))
	if len(d) != 0 {
		t.Fatalf("seq=4 should be buffered, got %q", d)
	}

	// seq=2 arrives — should flush 2+3+4
	d, _ = u.handleDataPacket(pkt(2, "B"))
	if string(d) != "BCD" {
		t.Fatalf("after seq=2: expected 'BCD', got %q", d)
	}

	if u.expectedSeq != 5 {
		t.Fatalf("expected expectedSeq=5, got %d", u.expectedSeq)
	}
}

// TestReorder_FullReverseOrder verifies packets arriving in reverse order are
// all buffered and then flushed correctly when the first packet arrives last.
func TestReorder_FullReverseOrder(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 10,
	}

	pkt := func(seq uint16, payload string) *utpPacket {
		return &utpPacket{
			Type: utpTypeST_DATA, Version: utpVersion,
			SequenceNumber: seq, Payload: []byte(payload),
		}
	}

	// Arrive in reverse: 12, 11, 10
	for _, p := range []*utpPacket{pkt(12, "C"), pkt(11, "B")} {
		d, _ := u.handleDataPacket(p)
		if len(d) != 0 {
			t.Fatalf("seq=%d: expected buffered (empty), got %q", p.SequenceNumber, d)
		}
	}

	// seq=10 arrives last — flushes all
	d, _ := u.handleDataPacket(pkt(10, "A"))
	if string(d) != "ABC" {
		t.Fatalf("expected 'ABC', got %q", d)
	}
}

// TestReorder_SeqWraparound verifies out-of-order delivery works across the
// uint16 sequence number wraparound boundary.
func TestReorder_SeqWraparound(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 65534,
	}

	pkt := func(seq uint16, payload string) *utpPacket {
		return &utpPacket{
			Type: utpTypeST_DATA, Version: utpVersion,
			SequenceNumber: seq, Payload: []byte(payload),
		}
	}

	// seq=65535 before 65534
	d, _ := u.handleDataPacket(pkt(65535, "Y"))
	if len(d) != 0 {
		t.Fatalf("seq=65535 should be buffered")
	}

	// seq=65534 arrives — should flush 65534 and 65535
	d, _ = u.handleDataPacket(pkt(65534, "X"))
	if string(d) != "XY" {
		t.Fatalf("expected 'XY' across wraparound, got %q", d)
	}
}

// ─── 11. Full round-trip data exchange over real UDP ─────────────────────────

// TestUTPFullRoundTrip_MultipleMessages verifies multiple independent Write/Read
// cycles over a live loopback uTP connection.
func TestUTPFullRoundTrip_MultipleMessages(t *testing.T) {
	udpListener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer udpListener.Close()

	addr := udpListener.LocalAddr().(*net.UDPAddr)
	messages := []string{"hello", "world", "utp", "works"}

	// Fake echo server: SYN → STATE, then echo each DATA back
	go func() {
		buf := make([]byte, 1500)
		var remote *net.UDPAddr
		serverSeq := uint16(200)

		// Handshake
		n, rem, err := udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		remote = rem
		syn, _ := unmarshalUTPPacket(buf[:n])
		state := &utpPacket{
			Type: utpTypeST_STATE, Version: utpVersion,
			ConnectionID: syn.ConnectionID, WindowSize: 64 * 1024,
			SequenceNumber: serverSeq, AckNumber: syn.SequenceNumber,
		}
		udpListener.WriteToUDP(state.marshal(), remote)
		serverSeq++

		// Echo each DATA packet back
		for i := 0; i < len(messages); i++ {
			n, _, err = udpListener.ReadFromUDP(buf)
			if err != nil {
				return
			}
			dataPkt, err := unmarshalUTPPacket(buf[:n])
			if err != nil || dataPkt.Type != utpTypeST_DATA {
				i-- // not a data packet, retry
				continue
			}
			echo := &utpPacket{
				Type: utpTypeST_DATA, Version: utpVersion,
				ConnectionID: syn.ConnectionID, WindowSize: 64 * 1024,
				SequenceNumber: serverSeq, AckNumber: dataPkt.SequenceNumber,
				Payload: dataPkt.Payload,
			}
			udpListener.WriteToUDP(echo.marshal(), remote)
			serverSeq++
		}
	}()

	p := peer{ip: addr.IP.To4(), port: uint16(addr.Port)}
	conn, err := dialUTP(p)
	if err != nil {
		t.Fatalf("dialUTP: %v", err)
	}
	defer conn.Close()

	rbuf := make([]byte, 256)
	for _, msg := range messages {
		if _, err := conn.Write([]byte(msg)); err != nil {
			t.Fatalf("Write(%q): %v", msg, err)
		}
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		n, err := conn.Read(rbuf)
		conn.SetDeadline(time.Time{})
		if err != nil {
			t.Fatalf("Read for %q: %v", msg, err)
		}
		if string(rbuf[:n]) != msg {
			t.Fatalf("echo mismatch: sent %q, got %q", msg, string(rbuf[:n]))
		}
	}
}

// ─── 12. Performance benchmarks ──────────────────────────────────────────────

// BenchmarkUTPPacketMarshal measures marshal throughput.
func BenchmarkUTPPacketMarshal(b *testing.B) {
	pkt := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		ConnectionID:   1234,
		WindowSize:     64 * 1024,
		SequenceNumber: 42,
		AckNumber:      41,
		Payload:        bytes.Repeat([]byte("x"), 1400),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = pkt.marshal()
	}
}

// BenchmarkUTPPacketUnmarshal measures unmarshal throughput.
func BenchmarkUTPPacketUnmarshal(b *testing.B) {
	pkt := &utpPacket{
		Type:    utpTypeST_DATA,
		Version: utpVersion, ConnectionID: 1234,
		WindowSize: 64 * 1024, SequenceNumber: 42, AckNumber: 41,
		Payload: bytes.Repeat([]byte("x"), 1400),
	}
	raw := pkt.marshal()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = unmarshalUTPPacket(raw)
	}
}

// BenchmarkUTPProcessAck measures processAck throughput with 1000 unacked segments.
func BenchmarkUTPProcessAck(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		u := &utpConn{
			unacked:  make(map[uint16]*utpSegment, 1000),
			cwnd:     64 * 1024,
			ssthresh: 64 * 1024,
		}
		u.windowCond = sync.NewCond(&u.mu)
		now := time.Now()
		for s := uint16(0); s < 1000; s++ {
			u.unacked[s] = &utpSegment{
				packet:   &utpPacket{SequenceNumber: s, Payload: make([]byte, 100)},
				sentAt:   now,
				attempts: 1,
			}
		}
		u.processAck(999)
	}
}

// BenchmarkUTPHandleDataPacket measures handleDataPacket in-order throughput.
func BenchmarkUTPHandleDataPacket(b *testing.B) {
	b.ReportAllocs()
	payload := bytes.Repeat([]byte("d"), 1400)
	for i := 0; i < b.N; i++ {
		u := &utpConn{
			unacked:     make(map[uint16]*utpSegment),
			recvBuffer:  make(map[uint16][]byte),
			expectedSeq: 0,
		}
		for s := uint16(0); s < 1000; s++ {
			pkt := &utpPacket{
				Type: utpTypeST_DATA, Version: utpVersion,
				SequenceNumber: s, Payload: payload,
			}
			u.handleDataPacket(pkt) //nolint:errcheck
		}
	}
}

// ─── 13. Congestion under packet loss ────────────────────────────────────────

// TestCongestion_WindowDropsOnLoss verifies that cwnd is reduced and ssthresh
// is set when a packet times out (loss signal).
func TestCongestion_WindowDropsOnLoss(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		recvBuffer: make(map[uint16][]byte),
		rto:        10 * time.Millisecond,
		maxRetries: 5,
		cwnd:       32 * 1024,
		minCwnd:    3000,
		maxCwnd:    1024 * 1024,
		ssthresh:   64 * 1024,
		done:       make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	u.unacked[1] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 1, Payload: make([]byte, 1400)},
		sentAt:   time.Now().Add(-50 * time.Millisecond), // past rto
		attempts: 1,
	}

	prevCwnd := u.cwnd
	u.checkRetransmit()

	u.mu.Lock()
	if u.cwnd >= prevCwnd {
		t.Fatalf("expected cwnd to decrease on loss, was %d now %d", prevCwnd, u.cwnd)
	}
	if u.ssthresh >= prevCwnd {
		t.Fatalf("expected ssthresh < prevCwnd, got ssthresh=%d prevCwnd=%d", u.ssthresh, prevCwnd)
	}
	u.mu.Unlock()
}

// TestCongestion_RecoverAfterLoss verifies that after a loss event the cwnd
// grows again with subsequent ACKs.
func TestCongestion_RecoverAfterLoss(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		recvBuffer: make(map[uint16][]byte),
		cwnd:       3000,
		minCwnd:    3000,
		maxCwnd:    1024 * 1024,
		ssthresh:   64 * 1024,
	}
	u.windowCond = sync.NewCond(&u.mu)

	// Simulate a normal ACK (attempts=1) — cwnd should grow
	u.unacked[5] = &utpSegment{
		packet:   &utpPacket{SequenceNumber: 5, Payload: make([]byte, 1400)},
		sentAt:   time.Now().Add(-10 * time.Millisecond),
		attempts: 1,
	}

	cwndBefore := u.cwnd
	u.processAck(5)

	u.mu.Lock()
	if u.cwnd <= cwndBefore {
		t.Fatalf("expected cwnd to grow after ACK, was %d now %d", cwndBefore, u.cwnd)
	}
	u.mu.Unlock()
}

// TestCongestion_NoGrowthBeyondMax verifies cwnd is capped at maxCwnd.
func TestCongestion_NoGrowthBeyondMax(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		recvBuffer: make(map[uint16][]byte),
		cwnd:       1024*1024 - 100,
		minCwnd:    3000,
		maxCwnd:    1024 * 1024,
		ssthresh:   1024 * 1024,
	}
	u.windowCond = sync.NewCond(&u.mu)

	for s := uint16(0); s < 10; s++ {
		u.unacked[s] = &utpSegment{
			packet:   &utpPacket{SequenceNumber: s, Payload: make([]byte, 1400)},
			sentAt:   time.Now().Add(-10 * time.Millisecond),
			attempts: 1,
		}
	}
	u.processAck(9)

	u.mu.Lock()
	if u.cwnd > u.maxCwnd {
		t.Fatalf("cwnd %d exceeds maxCwnd %d", u.cwnd, u.maxCwnd)
	}
	u.mu.Unlock()
}

// TestCongestion_SlowStartToAvoidance verifies the transition from slow-start
// (exponential growth) to congestion avoidance (linear growth) at ssthresh.
func TestCongestion_SlowStartToAvoidance(t *testing.T) {
	ssthresh := uint32(8 * 1024)
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		recvBuffer: make(map[uint16][]byte),
		cwnd:       3000,
		minCwnd:    3000,
		maxCwnd:    1024 * 1024,
		ssthresh:   ssthresh,
	}
	u.windowCond = sync.NewCond(&u.mu)

	seq := uint16(0)
	addAndAck := func(payloadSize int) {
		u.mu.Lock()
		u.unacked[seq] = &utpSegment{
			packet:   &utpPacket{SequenceNumber: seq, Payload: make([]byte, payloadSize)},
			sentAt:   time.Now().Add(-10 * time.Millisecond),
			attempts: 1,
		}
		u.mu.Unlock()
		u.processAck(seq)
		seq++
	}

	// Drive cwnd into slow start until it crosses ssthresh
	for u.cwnd < ssthresh {
		addAndAck(1400)
	}

	cwndAtThresh := u.cwnd

	// A few more ACKs — growth should now be slower (congestion avoidance)
	addAndAck(1400)
	growthCA := u.cwnd - cwndAtThresh

	// In slow start one ack of 1400 bytes would add ~1400 to cwnd.
	// In congestion avoidance the increase is (1400*1400)/cwnd which is << 1400.
	if growthCA >= 1400 {
		t.Fatalf("expected slow (CA) growth < 1400 bytes, got %d", growthCA)
	}
}
