package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// utpDebug controls verbose uTP packet logging. Set to true to enable.
var utpDebug = false

func utpLog(format string, args ...interface{}) {
	if utpDebug {
		log.Printf("[uTP] "+format, args...)
	}
}

const (
	maxUTPPacketSize   = 64 * 1024
	maxRecvBufferBytes = 4 * 1024 * 1024
	maxBufferedPackets = 4096
)

const (
	utpVersion = 1

	utpTypeST_DATA  uint8 = 0
	utpTypeST_FIN   uint8 = 1
	utpTypeST_STATE uint8 = 2
	utpTypeST_RESET uint8 = 3
	utpTypeST_SYN   uint8 = 4
)

const utpHeaderSize = 20

type utpPacket struct {
	Type         uint8
	Version      uint8
	Extension    uint8
	ConnectionID uint16

	Timestamp      uint32
	TimestampDiff  uint32
	WindowSize     uint32
	SequenceNumber uint16
	AckNumber      uint16

	Payload []byte
}

func (p *utpPacket) marshal() []byte {
	buf := make([]byte, utpHeaderSize+len(p.Payload))

	buf[0] = (p.Type << 4) | (p.Version & 0x0f)
	buf[1] = p.Extension

	binary.BigEndian.PutUint16(buf[2:4], p.ConnectionID)
	binary.BigEndian.PutUint32(buf[4:8], p.Timestamp)
	binary.BigEndian.PutUint32(buf[8:12], p.TimestampDiff)
	binary.BigEndian.PutUint32(buf[12:16], p.WindowSize)
	binary.BigEndian.PutUint16(buf[16:18], p.SequenceNumber)
	binary.BigEndian.PutUint16(buf[18:20], p.AckNumber)

	copy(buf[utpHeaderSize:], p.Payload)

	return buf
}

func unmarshalUTPPacket(buf []byte) (*utpPacket, error) {
	if len(buf) < utpHeaderSize {
		return nil, fmt.Errorf(
			"uTP packet too short: %d bytes",
			len(buf),
		)
	}

	if len(buf) > maxUTPPacketSize {
		return nil, fmt.Errorf(
			"uTP packet too large: %d bytes (max: %d)",
			len(buf),
			maxUTPPacketSize,
		)
	}

	version := buf[0] & 0x0f
	if version != utpVersion {
		return nil, fmt.Errorf(
			"unsupported uTP version: %d",
			version,
		)
	}

	pktType := buf[0] >> 4
	switch pktType {
	case utpTypeST_DATA, utpTypeST_FIN, utpTypeST_STATE, utpTypeST_RESET, utpTypeST_SYN:
		// Valid type
	default:
		return nil, fmt.Errorf("invalid uTP packet type: %d", pktType)
	}

	p := &utpPacket{
		Type:           pktType,
		Version:        version,
		Extension:      buf[1],
		ConnectionID:   binary.BigEndian.Uint16(buf[2:4]),
		Timestamp:      binary.BigEndian.Uint32(buf[4:8]),
		TimestampDiff:  binary.BigEndian.Uint32(buf[8:12]),
		WindowSize:     binary.BigEndian.Uint32(buf[12:16]),
		SequenceNumber: binary.BigEndian.Uint16(buf[16:18]),
		AckNumber:      binary.BigEndian.Uint16(buf[18:20]),
		Payload:        append([]byte(nil), buf[utpHeaderSize:]...),
	}

	return p, nil
}

type utpSegment struct {
	packet   *utpPacket
	sentAt   time.Time
	attempts int
}

type utpConn struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr

	connectionID uint16
	mu           sync.Mutex
	sendSeq      uint16
	recvSeq      uint16
	expectedSeq  uint16
	ackNumber    uint16
	unacked      map[uint16]*utpSegment
	recvBuffer   map[uint16][]byte
	readBuf      []byte

	// Flow control
	recvWindowMax uint32
	peerWindow    uint32
	windowCond    *sync.Cond

	// Congestion control
	cwnd     uint32
	minCwnd  uint32
	maxCwnd  uint32
	ssthresh uint32

	srtt       time.Duration
	rttvar     time.Duration
	rto        time.Duration
	minRTO     time.Duration
	maxRTO     time.Duration
	maxRetries int
	isClosed   bool
	recvEOF    bool
	closeErr   error
	done       chan struct{}
	closeOnce  sync.Once
}

func (u *utpConn) advertisedWindowLocked() uint32 {
	maxWin := u.recvWindowMax
	if maxWin <= 0 {
		maxWin = 64 * 1024
	}

	buffered := uint32(len(u.readBuf))
	for _, p := range u.recvBuffer {
		buffered += uint32(len(p))
	}

	if buffered >= maxWin {
		return 0
	}
	return maxWin - buffered
}

func (u *utpConn) bytesInFlightLocked() uint32 {
	var total uint32
	for _, seg := range u.unacked {
		if seg.packet != nil {
			total += uint32(len(seg.packet.Payload))
		}
	}
	return total
}

func (u *utpConn) effectiveWindowLocked() uint32 {
	peerWin := u.peerWindow
	if peerWin == 0 && u.srtt == 0 {
		peerWin = 64 * 1024
	}

	cwnd := u.cwnd
	if cwnd == 0 {
		cwnd = 3000
	}

	if peerWin < cwnd {
		return peerWin
	}
	return cwnd
}

func (u *utpConn) updateRTTLocked(sample time.Duration) {
	if sample <= 0 {
		return
	}

	if u.srtt == 0 {
		// First RTT measurement
		u.srtt = sample
		u.rttvar = sample / 2
	} else {
		diff := u.srtt - sample
		if diff < 0 {
			diff = -diff
		}
		// RTTVAR = (1 - beta)*RTTVAR + beta*|SRTT - R|, beta = 0.25
		u.rttvar = time.Duration(float64(u.rttvar)*0.75 + float64(diff)*0.25)
		// SRTT = (1 - alpha)*SRTT + alpha*R, alpha = 0.125
		u.srtt = time.Duration(float64(u.srtt)*0.875 + float64(sample)*0.125)
	}

	newRTO := u.srtt + 4*u.rttvar

	minRTO := u.minRTO
	if minRTO <= 0 {
		minRTO = 100 * time.Millisecond
	}
	maxRTO := u.maxRTO
	if maxRTO <= 0 {
		maxRTO = 3 * time.Second
	}

	if newRTO < minRTO {
		newRTO = minRTO
	} else if newRTO > maxRTO {
		newRTO = maxRTO
	}
	u.rto = newRTO
	utpLog("RTT sample=%v srtt=%v rttvar=%v rto=%v", sample, u.srtt, u.rttvar, u.rto)
}

func (u *utpConn) updateRTT(sample time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.updateRTTLocked(sample)
}

func (u *utpConn) processAck(ackNr uint16) {
	utpLog("ACK received ack=%d", ackNr)
	u.mu.Lock()
	defer u.mu.Unlock()

	now := time.Now()
	var ackedBytes uint32
	var normalAck bool

	for seq, seg := range u.unacked {
		// Serial number comparison with 16-bit wraparound
		if int16(ackNr-seq) >= 0 {
			if seg.packet != nil {
				ackedBytes += uint32(len(seg.packet.Payload))
			}
			// Karn's algorithm: Only measure RTT from segments that were NOT retransmitted
			if seg.attempts == 1 && !seg.sentAt.IsZero() {
				sample := now.Sub(seg.sentAt)
				u.updateRTTLocked(sample)
				normalAck = true
			}
			delete(u.unacked, seq)
		}
	}

	// Congestion control: increase cwnd upon valid ACKs
	if normalAck && ackedBytes > 0 {
		if u.cwnd == 0 {
			u.cwnd = 3000
		}
		if u.ssthresh == 0 {
			u.ssthresh = 64 * 1024
		}
		if u.maxCwnd == 0 {
			u.maxCwnd = 1024 * 1024
		}

		if u.cwnd < u.ssthresh {
			// Slow start: additive by bytes acked
			u.cwnd += ackedBytes
		} else {
			// Congestion avoidance: additive by ~MSS per RTT
			increase := (ackedBytes * 1400) / u.cwnd
			if increase == 0 {
				increase = 1
			}
			u.cwnd += increase
		}
		if u.cwnd > u.maxCwnd {
			u.cwnd = u.maxCwnd
		}
	}

	if u.windowCond != nil {
		u.windowCond.Broadcast()
	}
}

func (u *utpConn) checkRetransmit() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.isClosed {
		return u.closeErr
	}

	now := time.Now()
	rto := u.rto
	if rto <= 0 {
		rto = 500 * time.Millisecond
	}
	maxRetries := u.maxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	for seq, seg := range u.unacked {
		if now.Sub(seg.sentAt) > rto {
			if seg.attempts >= maxRetries {
				utpLog("connection failed seq=%d max retries exceeded", seq)
				u.closeErr = fmt.Errorf("uTP packet %d timed out after %d attempts", seq, seg.attempts)
				u.isClosed = true
				if u.windowCond != nil {
					u.windowCond.Broadcast()
				}
				return u.closeErr
			}

			// Packet loss detected: scale back congestion window
			minC := u.minCwnd
			if minC == 0 {
				minC = 3000
			}
			half := u.cwnd / 2
			if half < minC {
				half = minC
			}
			u.ssthresh = half
			u.cwnd = minC
			LogDashboardEvent("↓", "CWND reduced to %d KB upon loss detection", u.cwnd/1024)

			seg.attempts++
			seg.sentAt = now
			utpLog("retransmit seq=%d attempt=%d rto=%v", seq, seg.attempts, rto)
			LogDashboardEvent("↻", "Retransmitted seq=%d (attempt %d, RTO=%v)", seq, seg.attempts, rto)
			if u.conn != nil && u.remoteAddr != nil {
				u.conn.WriteToUDP(seg.packet.marshal(), u.remoteAddr)
			}
		}
	}
	return nil
}

func (u *utpConn) retransmissionLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-u.done:
			return
		case <-ticker.C:
			if err := u.checkRetransmit(); err != nil {
				u.Close()
				return
			}
		}
	}
}

func (u *utpConn) handleDataPacket(packet *utpPacket) ([]byte, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	seqDiff := int16(packet.SequenceNumber - u.expectedSeq)
	advWindow := u.advertisedWindowLocked()

	// Duplicate or already processed packet
	if seqDiff < 0 {
		utpLog("DATA duplicate seq=%d (expecting %d)", packet.SequenceNumber, u.expectedSeq)
		ack := &utpPacket{
			Type:           utpTypeST_STATE,
			Version:        utpVersion,
			ConnectionID:   u.connectionID,
			Timestamp:      uint32(time.Now().UnixMicro()),
			WindowSize:     advWindow,
			SequenceNumber: u.sendSeq,
			AckNumber:      u.ackNumber,
		}
		if u.conn != nil && u.remoteAddr != nil {
			u.conn.WriteToUDP(ack.marshal(), u.remoteAddr)
		}
		return nil, nil
	}

	// Out-of-order packet: buffer it
	if seqDiff > 0 {
		// Check packet count limit
		if len(u.recvBuffer) >= maxBufferedPackets {
			utpLog("dropped out-of-order packet seq=%d: max packet limit %d reached", packet.SequenceNumber, maxBufferedPackets)
			return nil, nil
		}

		// Calculate total buffered bytes
		var bufferedBytes int
		for _, b := range u.recvBuffer {
			bufferedBytes += len(b)
		}
		if bufferedBytes+len(packet.Payload) > maxRecvBufferBytes {
			utpLog("dropped out-of-order packet seq=%d: max buffer bytes %d exceeded", packet.SequenceNumber, maxRecvBufferBytes)
			return nil, nil
		}

		u.recvBuffer[packet.SequenceNumber] = append([]byte(nil), packet.Payload...)
		utpLog("DATA buffered seq=%d expecting=%d", packet.SequenceNumber, u.expectedSeq)
		ack := &utpPacket{
			Type:           utpTypeST_STATE,
			Version:        utpVersion,
			ConnectionID:   u.connectionID,
			Timestamp:      uint32(time.Now().UnixMicro()),
			WindowSize:     u.advertisedWindowLocked(),
			SequenceNumber: u.sendSeq,
			AckNumber:      u.ackNumber,
		}
		if u.conn != nil && u.remoteAddr != nil {
			u.conn.WriteToUDP(ack.marshal(), u.remoteAddr)
		}
		return nil, nil
	}

	// In-order packet: deliver it and any consecutive buffered packets
	var delivered []byte
	delivered = append(delivered, packet.Payload...)
	u.ackNumber = u.expectedSeq
	u.recvSeq = u.expectedSeq
	u.expectedSeq++

	for {
		if nextPayload, ok := u.recvBuffer[u.expectedSeq]; ok {
			delivered = append(delivered, nextPayload...)
			delete(u.recvBuffer, u.expectedSeq)
			u.ackNumber = u.expectedSeq
			u.recvSeq = u.expectedSeq
			u.expectedSeq++
		} else {
			break
		}
	}

	utpLog("DATA delivered seq=%d len=%d expectedNext=%d", packet.SequenceNumber, len(delivered), u.expectedSeq)
	ack := &utpPacket{
		Type:           utpTypeST_STATE,
		Version:        utpVersion,
		ConnectionID:   u.connectionID,
		Timestamp:      uint32(time.Now().UnixMicro()),
		WindowSize:     u.advertisedWindowLocked(),
		SequenceNumber: u.sendSeq,
		AckNumber:      u.ackNumber,
	}
	if u.conn != nil && u.remoteAddr != nil {
		u.conn.WriteToUDP(ack.marshal(), u.remoteAddr)
	}

	return delivered, nil
}

func dialUTP(p peer) (peer_conn, error) {
	remoteAddr, err := net.ResolveUDPAddr("udp", p.String())
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}

	initSeq := uint16(rand.Intn(65536))
	u := &utpConn{
		conn:          conn,
		remoteAddr:    remoteAddr,
		connectionID:  uint16(rand.Intn(65536)),
		sendSeq:       initSeq,
		unacked:       make(map[uint16]*utpSegment),
		recvBuffer:    make(map[uint16][]byte),
		rto:           500 * time.Millisecond,
		minRTO:        100 * time.Millisecond,
		maxRTO:        3 * time.Second,
		maxRetries:    5,
		recvWindowMax: 64 * 1024,
		peerWindow:    64 * 1024,
		cwnd:          3000,
		minCwnd:       3000,
		maxCwnd:       1024 * 1024,
		ssthresh:      64 * 1024,
		done:          make(chan struct{}),
	}
	u.windowCond = sync.NewCond(&u.mu)

	syn := &utpPacket{
		Type:           utpTypeST_SYN,
		Version:        utpVersion,
		ConnectionID:   u.connectionID,
		Timestamp:      uint32(time.Now().UnixMicro()),
		WindowSize:     u.advertisedWindowLocked(),
		SequenceNumber: u.sendSeq,
		AckNumber:      0,
	}

	if _, err := conn.WriteToUDP(syn.marshal(), remoteAddr); err != nil {
		conn.Close()
		return nil, err
	}
	utpLog("SYN sent connID=%d seq=%d", u.connectionID, syn.SequenceNumber)

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		conn.Close()
		return nil, err
	}

	buf := make([]byte, 1500)

	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("uTP handshake failed: %w", err)
	}

	if addr.String() != remoteAddr.String() {
		conn.Close()
		return nil, fmt.Errorf("uTP response from unexpected peer")
	}

	response, err := unmarshalUTPPacket(buf[:n])
	if err != nil {
		conn.Close()
		return nil, err
	}

	if response.Type != utpTypeST_STATE {
		conn.Close()
		return nil, fmt.Errorf(
			"expected uTP STATE, got type %d",
			response.Type,
		)
	}

	u.ackNumber = response.SequenceNumber
	u.recvSeq = response.SequenceNumber
	u.expectedSeq = response.SequenceNumber + 1
	u.sendSeq++ // SYN consumes one sequence number
	if response.WindowSize > 0 {
		u.peerWindow = response.WindowSize
	}
	utpLog("STATE received seq=%d ack=%d peerWindow=%d", response.SequenceNumber, response.AckNumber, response.WindowSize)
	LogDashboardEvent("✓", "uTP Handshake established with %s (connID=%d)", p.String(), response.ConnectionID)

	conn.SetReadDeadline(time.Time{})

	go u.retransmissionLoop()

	return u, nil
}

func (u *utpConn) Read(p []byte) (int, error) {
	u.mu.Lock()
	if len(u.readBuf) > 0 {
		n := copy(p, u.readBuf)
		u.readBuf = u.readBuf[n:]
		u.mu.Unlock()
		return n, nil
	}
	if u.recvEOF {
		u.mu.Unlock()
		return 0, io.EOF
	}
	if u.isClosed {
		err := u.closeErr
		u.mu.Unlock()
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("uTP connection closed")
	}
	u.mu.Unlock()

	buf := make([]byte, 1500)

	for {
		n, addr, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			return 0, err
		}

		if addr.String() != u.remoteAddr.String() {
			continue
		}

		packet, err := unmarshalUTPPacket(buf[:n])
		if err != nil {
			continue
		}

		if packet.ConnectionID != u.connectionID {
			continue
		}

		// Update remote peer advertised window
		u.mu.Lock()
		u.peerWindow = packet.WindowSize
		if u.windowCond != nil {
			u.windowCond.Broadcast()
		}
		u.mu.Unlock()

		// Acknowledge outstanding sent packets using the incoming AckNumber
		u.processAck(packet.AckNumber)

		switch packet.Type {
		case utpTypeST_STATE:
			continue

		case utpTypeST_DATA:
			delivered, err := u.handleDataPacket(packet)
			if err != nil {
				return 0, err
			}
			if len(delivered) == 0 {
				// Packet was duplicate or out-of-order buffered
				continue
			}

			u.mu.Lock()
			copied := copy(p, delivered)
			if copied < len(delivered) {
				u.readBuf = append(u.readBuf, delivered[copied:]...)
			}
			u.mu.Unlock()

			return copied, nil

		case utpTypeST_FIN:
			utpLog("FIN received, EOF")
			u.mu.Lock()
			// Send STATE ACK for FIN
			ack := &utpPacket{
				Type:           utpTypeST_STATE,
				Version:        utpVersion,
				ConnectionID:   u.connectionID,
				Timestamp:      uint32(time.Now().UnixMicro()),
				WindowSize:     u.advertisedWindowLocked(),
				SequenceNumber: u.sendSeq,
				AckNumber:      packet.SequenceNumber,
			}
			if u.conn != nil && u.remoteAddr != nil {
				u.conn.WriteToUDP(ack.marshal(), u.remoteAddr)
			}
			u.recvEOF = true
			if len(u.readBuf) > 0 {
				n := copy(p, u.readBuf)
				u.readBuf = u.readBuf[n:]
				u.mu.Unlock()
				return n, nil
			}
			u.mu.Unlock()
			return 0, io.EOF

		case utpTypeST_RESET:
			utpLog("RESET received from peer")
			u.mu.Lock()
			u.isClosed = true
			u.closeErr = fmt.Errorf("uTP connection reset by peer")
			if u.windowCond != nil {
				u.windowCond.Broadcast()
			}
			conn := u.conn
			u.mu.Unlock()
			if conn != nil {
				conn.Close()
			}
			return 0, fmt.Errorf("uTP connection reset by peer")
		}
	}
}

func (u *utpConn) Write(p []byte) (int, error) {
	u.mu.Lock()

	for {
		if u.isClosed {
			err := u.closeErr
			u.mu.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("uTP connection closed")
		}

		effectiveWin := u.effectiveWindowLocked()
		inFlight := u.bytesInFlightLocked()
		// Flow control & Congestion control throttling
		if inFlight > 0 && inFlight+uint32(len(p)) > effectiveWin {
			if u.windowCond != nil {
				u.windowCond.Wait()
				continue
			}
		}
		break
	}

	seq := u.sendSeq
	u.sendSeq++
	ack := u.ackNumber
	connID := u.connectionID
	remoteAddr := u.remoteAddr
	conn := u.conn
	advWindow := u.advertisedWindowLocked()

	packet := &utpPacket{
		Type:           utpTypeST_DATA,
		Version:        utpVersion,
		ConnectionID:   connID,
		Timestamp:      uint32(time.Now().UnixMicro()),
		WindowSize:     advWindow,
		SequenceNumber: seq,
		AckNumber:      ack,
		Payload:        append([]byte(nil), p...),
	}

	u.unacked[seq] = &utpSegment{
		packet:   packet,
		sentAt:   time.Now(),
		attempts: 1,
	}
	u.mu.Unlock()

	utpLog("DATA sent seq=%d len=%d", seq, len(p))

	if conn != nil && remoteAddr != nil {
		_, err := conn.WriteToUDP(packet.marshal(), remoteAddr)
		if err != nil {
			u.mu.Lock()
			delete(u.unacked, seq)
			if u.windowCond != nil {
				u.windowCond.Broadcast()
			}
			u.mu.Unlock()
			return 0, err
		}
	}

	return len(p), nil
}

func (u *utpConn) sendReset() {
	u.mu.Lock()
	seq := u.sendSeq
	ack := u.ackNumber
	connID := u.connectionID
	remoteAddr := u.remoteAddr
	conn := u.conn
	u.mu.Unlock()

	if conn != nil && remoteAddr != nil {
		reset := &utpPacket{
			Type:           utpTypeST_RESET,
			Version:        utpVersion,
			ConnectionID:   connID,
			Timestamp:      uint32(time.Now().UnixMicro()),
			SequenceNumber: seq,
			AckNumber:      ack,
		}
		conn.WriteToUDP(reset.marshal(), remoteAddr)
	}
}

func (u *utpConn) Close() error {
	u.mu.Lock()
	if u.isClosed {
		u.mu.Unlock()
		return nil
	}

	u.closeOnce.Do(func() {
		if u.done != nil {
			close(u.done)
		}
	})
	u.isClosed = true
	if u.closeErr == nil {
		u.closeErr = fmt.Errorf("uTP connection closed")
	}
	if u.windowCond != nil {
		u.windowCond.Broadcast()
	}

	seq := u.sendSeq
	u.sendSeq++
	ack := u.ackNumber
	connID := u.connectionID
	remoteAddr := u.remoteAddr
	conn := u.conn
	advWindow := u.advertisedWindowLocked()
	u.mu.Unlock()

	utpLog("connection closing connID=%d", connID)

	if conn != nil && remoteAddr != nil {
		fin := &utpPacket{
			Type:           utpTypeST_FIN,
			Version:        utpVersion,
			ConnectionID:   connID,
			Timestamp:      uint32(time.Now().UnixMicro()),
			WindowSize:     advWindow,
			SequenceNumber: seq,
			AckNumber:      ack,
		}
		conn.WriteToUDP(fin.marshal(), remoteAddr)
		return conn.Close()
	}

	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (u *utpConn) SetDeadline(t time.Time) error {
	return u.conn.SetDeadline(t)
}

// DashboardTransport provides a clean snapshot of connection and congestion metrics.
type DashboardTransport struct {
	Mode          string
	Connected     bool
	RemoteAddr    string
	RTT           time.Duration
	RTO           time.Duration
	CWND          uint32
	PeerWindow    uint32
	BytesInFlight uint32
	Retries       int
}

type UTPStats = DashboardTransport

func (u *utpConn) Stats() DashboardTransport {
	u.mu.Lock()
	defer u.mu.Unlock()

	var retries int
	for _, s := range u.unacked {
		if s.attempts > 1 {
			retries += s.attempts - 1
		}
	}

	addr := ""
	if u.remoteAddr != nil {
		addr = u.remoteAddr.String()
	}

	return UTPStats{
		Mode:          "μTP / UDP",
		Connected:     !u.isClosed,
		RemoteAddr:    addr,
		RTT:           u.srtt,
		RTO:           u.rto,
		CWND:          u.cwnd,
		PeerWindow:    u.peerWindow,
		BytesInFlight: u.bytesInFlightLocked(),
		Retries:       retries,
	}
}
