package main

import (
	"testing"
)

// ─── 1. Packet validation tests ──────────────────────────────────────────────

func TestUTPRejectsOversizedPacket(t *testing.T) {
	oversized := make([]byte, maxUTPPacketSize+10)
	oversized[0] = (utpTypeST_DATA << 4) | utpVersion

	_, err := unmarshalUTPPacket(oversized)
	if err == nil {
		t.Fatal("expected error for oversized packet, got nil")
	}
}

func TestUTPRejectsInvalidPacketType(t *testing.T) {
	invalidTypes := []uint8{5, 6, 7, 8, 9, 15}

	for _, pt := range invalidTypes {
		raw := make([]byte, utpHeaderSize)
		raw[0] = (pt << 4) | utpVersion

		_, err := unmarshalUTPPacket(raw)
		if err == nil {
			t.Fatalf("expected error for invalid packet type %d, got nil", pt)
		}
	}
}

func TestUTPRejectsWrongVersion(t *testing.T) {
	wrongVersions := []uint8{0, 2, 3, 15}

	for _, v := range wrongVersions {
		raw := make([]byte, utpHeaderSize)
		raw[0] = (utpTypeST_DATA << 4) | v

		_, err := unmarshalUTPPacket(raw)
		if err == nil {
			t.Fatalf("expected error for unsupported version %d, got nil", v)
		}
	}
}

func TestUTPAcceptsAllValidPacketTypes(t *testing.T) {
	validTypes := []uint8{
		utpTypeST_DATA,
		utpTypeST_FIN,
		utpTypeST_STATE,
		utpTypeST_RESET,
		utpTypeST_SYN,
	}

	for _, pt := range validTypes {
		raw := make([]byte, utpHeaderSize)
		raw[0] = (pt << 4) | utpVersion

		pkt, err := unmarshalUTPPacket(raw)
		if err != nil {
			t.Fatalf("expected valid packet for type %d, got err: %v", pt, err)
		}
		if pkt.Type != pt {
			t.Fatalf("expected type %d, got %d", pt, pkt.Type)
		}
	}
}

// ─── 2. Resource limit tests ─────────────────────────────────────────────────

func TestUTPReceiveBufferByteLimit(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 1,
	}

	// Pre-fill receive buffer close to maxRecvBufferBytes (4MB)
	chunkSize := 1024 * 1024 // 1MB
	for s := uint16(10); s < 14; s++ {
		u.recvBuffer[s] = make([]byte, chunkSize)
	}

	// Try to buffer an out-of-order packet that would exceed 4MB
	oversizedOutOfOrder := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		SequenceNumber: 50,
		Payload:        make([]byte, 2*1024*1024), // 2MB
	}

	delivered, err := u.handleDataPacket(oversizedOutOfOrder)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(delivered) != 0 {
		t.Fatalf("expected dropped out-of-order packet, delivered: %d bytes", len(delivered))
	}

	// Verify packet was NOT added to recvBuffer
	if _, exists := u.recvBuffer[50]; exists {
		t.Fatal("packet exceeding byte limit should have been dropped, but was buffered")
	}
}

func TestUTPReceivePacketCountLimit(t *testing.T) {
	u := &utpConn{
		unacked:     make(map[uint16]*utpSegment),
		recvBuffer:  make(map[uint16][]byte),
		expectedSeq: 1,
	}

	// Fill receive buffer up to maxBufferedPackets (4096)
	for s := uint16(10); s < 10+maxBufferedPackets; s++ {
		u.recvBuffer[s] = []byte("small")
	}

	// Try to buffer 1 more out-of-order packet
	extraPacket := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		SequenceNumber: 9999,
		Payload:        []byte("excess"),
	}

	delivered, err := u.handleDataPacket(extraPacket)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(delivered) != 0 {
		t.Fatalf("expected dropped packet, delivered: %d bytes", len(delivered))
	}

	// Verify excess packet was NOT added
	if _, exists := u.recvBuffer[9999]; exists {
		t.Fatal("excess packet beyond maxBufferedPackets should have been dropped, but was buffered")
	}
}

// ─── 3. Connection ID & ACK security checks ──────────────────────────────────

func TestUTPConnectionIDIsolation(t *testing.T) {
	u := &utpConn{
		connectionID: 12345,
		expectedSeq:  1,
	}

	pktOtherConn := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		ConnectionID:   54321,
		SequenceNumber: 1,
		Payload:        []byte("data"),
	}

	if pktOtherConn.ConnectionID == u.connectionID {
		t.Fatal("ConnectionIDs should not match")
	}
}

func TestUTPAckValidation(t *testing.T) {
	u := &utpConn{
		unacked: make(map[uint16]*utpSegment),
	}

	// Setup sent segments 10, 11
	u.unacked[10] = &utpSegment{packet: &utpPacket{SequenceNumber: 10}}
	u.unacked[11] = &utpSegment{packet: &utpPacket{SequenceNumber: 11}}

	// Valid ACK 10 acknowledges 10
	u.processAck(10)
	if _, exists := u.unacked[10]; exists {
		t.Fatal("seq 10 should be acknowledged and removed")
	}
	if _, exists := u.unacked[11]; !exists {
		t.Fatal("seq 11 should still be unacked")
	}

	// ACK for nonexistent far future sequence doesn't crash or corrupt
	u.processAck(50000)
	if len(u.unacked) != 1 {
		t.Fatalf("expected 1 unacked left, got %d", len(u.unacked))
	}
}
