package main

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ─── 1. Sequence number wraparound extended ──────────────────────────────────

func TestSeqWrap_DATADeliveryAcrossWrap(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 65535,
	}

	pkt := func(seq uint16, payload string) *utpPacket {
		return &utpPacket{
			Type:           utpTypeST_DATA,
			Version:        utpVersion,
			SequenceNumber: seq,
			Payload:        []byte(payload),
		}
	}

	// Deliver seq=65535
	d1, err := u.handleDataPacket(pkt(65535, "end"))
	if err != nil || string(d1) != "end" {
		t.Fatalf("expected 'end', got %q, err: %v", d1, err)
	}

	// Deliver seq=0 (normal wraparound progression)
	d2, err := u.handleDataPacket(pkt(0, "start"))
	if err != nil || string(d2) != "start" {
		t.Fatalf("expected 'start', got %q, err: %v", d2, err)
	}

	// Deliver seq=1
	d3, err := u.handleDataPacket(pkt(1, "next"))
	if err != nil || string(d3) != "next" {
		t.Fatalf("expected 'next', got %q, err: %v", d3, err)
	}

	if u.expectedSeq != 2 {
		t.Fatalf("expected expectedSeq=2, got %d", u.expectedSeq)
	}
}

func TestSeqWrap_DuplicateDetectionAtWrap(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 65535,
	}

	pkt := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		SequenceNumber: 65535,
		Payload:        []byte("data"),
	}

	d1, _ := u.handleDataPacket(pkt)
	if string(d1) != "data" {
		t.Fatalf("expected 'data', got %q", d1)
	}

	// Duplicate 65535 after expectedSeq advanced to 0
	d2, _ := u.handleDataPacket(pkt)
	if len(d2) != 0 {
		t.Fatalf("duplicate at wraparound should return nil payload, got %q", d2)
	}
}

func TestSeqWrap_RetransmitTrackingAcrossWrap(t *testing.T) {
	u := &utpConn{
		unacked: make(map[uint16]*utpSegment),
	}
	u.windowCond = sync.NewCond(&u.mu)

	now := time.Now()
	u.unacked[65534] = &utpSegment{packet: &utpPacket{SequenceNumber: 65534}, sentAt: now, attempts: 1}
	u.unacked[65535] = &utpSegment{packet: &utpPacket{SequenceNumber: 65535}, sentAt: now, attempts: 1}
	u.unacked[0] = &utpSegment{packet: &utpPacket{SequenceNumber: 0}, sentAt: now, attempts: 1}
	u.unacked[1] = &utpSegment{packet: &utpPacket{SequenceNumber: 1}, sentAt: now, attempts: 1}

	if len(u.unacked) != 4 {
		t.Fatalf("expected 4 unacked packets, got %d", len(u.unacked))
	}

	// Cumulative ACK for 1 should acknowledge 65534, 65535, 0, and 1
	u.processAck(1)

	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.unacked) != 0 {
		t.Fatalf("expected 0 unacked packets after wraparound ACK, got %d", len(u.unacked))
	}
}

// ─── 2. Aggressive packet ordering ───────────────────────────────────────────

func TestOrder_Scrambled_3_1_5_2_4(t *testing.T) {
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

	var collected []byte
	deliveryOrder := []struct {
		seq uint16
		val string
	}{
		{3, "C"},
		{1, "A"},
		{5, "E"},
		{2, "B"},
		{4, "D"},
	}

	for _, step := range deliveryOrder {
		d, err := u.handleDataPacket(pkt(step.seq, step.val))
		if err != nil {
			t.Fatalf("handleDataPacket(seq=%d) error: %v", step.seq, err)
		}
		if len(d) > 0 {
			collected = append(collected, d...)
		}
	}

	if string(collected) != "ABCDE" {
		t.Fatalf("expected delivered stream 'ABCDE', got %q", string(collected))
	}
	if u.expectedSeq != 6 {
		t.Fatalf("expected expectedSeq=6, got %d", u.expectedSeq)
	}
}

func TestOrder_DuplicateInMiddle_1_2_2_3_4(t *testing.T) {
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

	var collected []byte
	seqs := []struct {
		seq uint16
		val string
	}{
		{1, "A"},
		{2, "B"},
		{2, "B"}, // duplicate
		{3, "C"},
		{4, "D"},
	}

	for _, s := range seqs {
		d, _ := u.handleDataPacket(pkt(s.seq, s.val))
		if len(d) > 0 {
			collected = append(collected, d...)
		}
	}

	if string(collected) != "ABCD" {
		t.Fatalf("expected 'ABCD', got %q", string(collected))
	}
}

func TestOrder_LateArrival_GapThenFill(t *testing.T) {
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

	d1, _ := u.handleDataPacket(pkt(1, "1"))
	d2, _ := u.handleDataPacket(pkt(2, "2"))
	if string(d1)+string(d2) != "12" {
		t.Fatalf("expected 12, got %q", string(d1)+string(d2))
	}

	// 4 and 5 arrive early
	d4, _ := u.handleDataPacket(pkt(4, "4"))
	d5, _ := u.handleDataPacket(pkt(5, "5"))
	if len(d4) != 0 || len(d5) != 0 {
		t.Fatalf("4 and 5 should be buffered")
	}

	// 3 arrives late — flushes 3, 4, 5
	d3, _ := u.handleDataPacket(pkt(3, "3"))
	if string(d3) != "345" {
		t.Fatalf("expected '345' flushed, got %q", string(d3))
	}
}

// ─── 3. Malformed packet tests ───────────────────────────────────────────────

func TestMalformed_TooShort(t *testing.T) {
	_, err := unmarshalUTPPacket([]byte{0x11, 0x00, 0x00})
	if err == nil {
		t.Fatal("expected error for packet too short, got nil")
	}
}

func TestMalformed_EmptyPacket(t *testing.T) {
	_, err := unmarshalUTPPacket([]byte{})
	if err == nil {
		t.Fatal("expected error for empty packet, got nil")
	}
}

func TestMalformed_WrongVersion(t *testing.T) {
	raw := make([]byte, 20)
	raw[0] = (utpTypeST_DATA << 4) | 0x09 // version 9
	_, err := unmarshalUTPPacket(raw)
	if err == nil {
		t.Fatal("expected error for unsupported version 9, got nil")
	}
}

func TestMalformed_ValidHeaderNoPayload(t *testing.T) {
	pkt := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		SequenceNumber: 10,
	}
	raw := pkt.marshal()
	parsed, err := unmarshalUTPPacket(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parsed.Payload) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(parsed.Payload))
	}
}

func TestMalformed_TruncatedPayload(t *testing.T) {
	pkt := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		SequenceNumber: 10,
		Payload:        bytes.Repeat([]byte("A"), 100),
	}
	raw := pkt.marshal() // 120 bytes
	// Truncate to 70 bytes
	parsed, err := unmarshalUTPPacket(raw[:70])
	if err != nil {
		t.Fatalf("unexpected error on truncated payload: %v", err)
	}
	if len(parsed.Payload) != 50 {
		t.Fatalf("expected 50 bytes payload, got %d", len(parsed.Payload))
	}
}

// ─── 4. Connection shutdown verification ─────────────────────────────────────

func TestShutdown_CloseSetsError(t *testing.T) {
	u := &utpConn{
		done: make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	err := u.Close()
	if err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.isClosed {
		t.Fatal("expected isClosed=true")
	}
	if u.closeErr == nil {
		t.Fatal("expected non-nil closeErr")
	}
}

func TestShutdown_WriteAfterClose(t *testing.T) {
	u := &utpConn{
		done: make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)
	u.Close()

	_, err := u.Write([]byte("hello"))
	if err == nil {
		t.Fatal("expected Write after Close to fail, got nil")
	}
}

func TestShutdown_ResetSetsError(t *testing.T) {
	u := &utpConn{
		done: make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	u.mu.Lock()
	u.isClosed = true
	u.closeErr = fmt.Errorf("uTP connection reset by peer")
	u.mu.Unlock()

	_, err := u.Write([]byte("test"))
	if err == nil || err.Error() != "uTP connection reset by peer" {
		t.Fatalf("expected reset error on Write, got: %v", err)
	}
}

// ─── 5. Goroutine leak verification ──────────────────────────────────────────

func TestGoroutineLeak_ConnectionClose(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	u := &utpConn{
		unacked: make(map[uint16]*utpSegment),
		rto:     500 * time.Millisecond,
		done:    make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	go u.retransmissionLoop()

	time.Sleep(50 * time.Millisecond)
	u.Close()
	time.Sleep(150 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	// Allow small slack for test runner goroutines
	if finalGoroutines > initialGoroutines+2 {
		t.Fatalf("possible goroutine leak: initial=%d, final=%d", initialGoroutines, finalGoroutines)
	}
}
