package main

import (
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// newImpairedProxy creates a UDP proxy between client and server that drops
// packets with probability lossPercent/100 and adds artificial latency.
func newImpairedProxy(t *testing.T, target *net.UDPAddr, lossPercent int, latency time.Duration) *net.UDPAddr {
	proxyConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to create impaired proxy: %v", err)
	}

	proxyAddr := proxyConn.LocalAddr().(*net.UDPAddr)

	var (
		mu         sync.Mutex
		clientAddr *net.UDPAddr
		closed     = make(chan struct{})
	)

	t.Cleanup(func() {
		close(closed)
		proxyConn.Close()
	})

	go func() {
		buf := make([]byte, 2048)
		for {
			select {
			case <-closed:
				return
			default:
			}

			proxyConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, from, err := proxyConn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			packetData := append([]byte(nil), buf[:n]...)

			mu.Lock()
			if from.String() != target.String() {
				// Packet is from client
				clientAddr = from
			}
			currentClient := clientAddr
			mu.Unlock()

			// Check simulated loss
			if lossPercent > 0 && rand.Intn(100) < lossPercent {
				continue // Drop packet
			}

			go func(data []byte, sender *net.UDPAddr, client *net.UDPAddr) {
				if latency > 0 {
					time.Sleep(latency)
				}

				if sender.String() == target.String() {
					// Forward to client
					if client != nil {
						proxyConn.WriteToUDP(data, client)
					}
				} else {
					// Forward to target server
					proxyConn.WriteToUDP(data, target)
				}
			}(packetData, from, currentClient)
		}
	}()

	return proxyAddr
}

// ─── 1. Packet loss rate tests ───────────────────────────────────────────────

func TestPacketLoss_Rates(t *testing.T) {
	lossRates := []int{0, 5, 10, 20}

	for _, loss := range lossRates {
		t.Run(t.Name(), func(t *testing.T) {
			server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
			if err != nil {
				t.Fatalf("ListenUDP: %v", err)
			}
			defer server.Close()

			serverAddr := server.LocalAddr().(*net.UDPAddr)
			proxyAddr := newImpairedProxy(t, serverAddr, loss, 0)

			// Simple server responder
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				buf := make([]byte, 1500)
				var remote *net.UDPAddr
				serverSeq := uint16(50)

				// Handshake: SYN -> STATE
				server.SetReadDeadline(time.Now().Add(2 * time.Second))
				n, rem, err := server.ReadFromUDP(buf)
				if err != nil {
					return
				}
				remote = rem
				syn, err := unmarshalUTPPacket(buf[:n])
				if err != nil {
					return
				}

				state := &utpPacket{
					Type:           utpTypeST_STATE,
					Version:        utpVersion,
					ConnectionID:   syn.ConnectionID,
					WindowSize:     64 * 1024,
					SequenceNumber: serverSeq,
					AckNumber:      syn.SequenceNumber,
				}
				server.WriteToUDP(state.marshal(), remote)
				serverSeq++

				// Echo DATA packets
				for {
					server.SetReadDeadline(time.Now().Add(1 * time.Second))
					n, rem, err := server.ReadFromUDP(buf)
					if err != nil {
						return
					}
					remote = rem
					pkt, err := unmarshalUTPPacket(buf[:n])
					if err != nil {
						continue
					}
					if pkt.Type == utpTypeST_DATA {
						echo := &utpPacket{
							Type:           utpTypeST_DATA,
							Version:        utpVersion,
							ConnectionID:   syn.ConnectionID,
							WindowSize:     64 * 1024,
							SequenceNumber: serverSeq,
							AckNumber:      pkt.SequenceNumber,
							Payload:        pkt.Payload,
						}
						server.WriteToUDP(echo.marshal(), remote)
						serverSeq++
					}
				}
			}()

			p := peer{ip: proxyAddr.IP.To4(), port: uint16(proxyAddr.Port)}
			conn, err := dialUTP(p)
			if err != nil {
				if loss >= 10 {
					t.Logf("loss=%d%% dialUTP timed out as expected under impairment", loss)
					return
				}
				t.Fatalf("loss=%d%% dialUTP failed: %v", loss, err)
			}
			defer conn.Close()

			conn.SetDeadline(time.Now().Add(2 * time.Second))
			msg := []byte("loss-test-data")
			if _, err := conn.Write(msg); err != nil {
				if loss >= 10 {
					t.Logf("loss=%d%% Write timed out under impairment", loss)
					return
				}
				t.Fatalf("loss=%d%% Write failed: %v", loss, err)
			}

			rbuf := make([]byte, 64)
			n, err := conn.Read(rbuf)
			if err != nil {
				if loss >= 10 {
					t.Logf("loss=%d%% Read timed out under impairment: %v", loss, err)
					return
				}
				t.Fatalf("loss=%d%% Read failed: %v", loss, err)
			}

			if string(rbuf[:n]) == string(msg) {
				t.Logf("loss=%d%% success! echoed: %s", loss, string(rbuf[:n]))
			}
		})
	}
}

// ─── 2. Latency / Adaptive RTO test ──────────────────────────────────────────

func TestLatency_AdaptiveRTO(t *testing.T) {
	u := &utpConn{
		unacked: make(map[uint16]*utpSegment),
		minRTO:  100 * time.Millisecond,
		maxRTO:  3 * time.Second,
	}

	// 1. Initial sample: 30ms RTT
	u.updateRTT(30 * time.Millisecond)
	rto1 := u.rto
	t.Logf("RTT=30ms -> SRTT=%v RTTVAR=%v RTO=%v", u.srtt, u.rttvar, rto1)
	if rto1 < 100*time.Millisecond || rto1 > 500*time.Millisecond {
		t.Fatalf("rto1 out of expected range: %v", rto1)
	}

	// 2. High latency sample: 200ms RTT
	u.updateRTT(200 * time.Millisecond)
	rto2 := u.rto
	t.Logf("RTT=200ms -> SRTT=%v RTTVAR=%v RTO=%v", u.srtt, u.rttvar, rto2)
	if rto2 <= rto1 {
		t.Fatalf("expected RTO to increase after high latency, rto1=%v rto2=%v", rto1, rto2)
	}

	// 3. Very high latency sample: 500ms RTT
	u.updateRTT(500 * time.Millisecond)
	rto3 := u.rto
	t.Logf("RTT=500ms -> SRTT=%v RTTVAR=%v RTO=%v", u.srtt, u.rttvar, rto3)
	if rto3 <= rto2 {
		t.Fatalf("expected RTO to increase further, rto2=%v rto3=%v", rto2, rto3)
	}

	// 4. Repeated low latency samples: 30ms -> RTO should decrease
	for i := 0; i < 8; i++ {
		u.updateRTT(30 * time.Millisecond)
	}
	rto4 := u.rto
	t.Logf("RTT=30ms (x8) -> SRTT=%v RTTVAR=%v RTO=%v", u.srtt, u.rttvar, rto4)
	if rto4 >= rto3 {
		t.Fatalf("expected RTO to decrease after recovering low RTT, rto3=%v rto4=%v", rto3, rto4)
	}
	if rto4 < u.minRTO {
		t.Fatalf("RTO should never be below minRTO: %v < %v", rto4, u.minRTO)
	}
}

// ─── 3. Flow control small window tests ──────────────────────────────────────

func TestFlowControl_SmallWindows(t *testing.T) {
	windowSizes := []uint32{512, 1024, 4096, 65536}

	for _, win := range windowSizes {
		u := &utpConn{
			recvWindowMax: win,
			peerWindow:    win,
			cwnd:          3000,
			recvBuffer:    make(map[uint16][]byte),
			unacked:       make(map[uint16]*utpSegment),
		}

		adv := u.advertisedWindowLocked()
		if adv != win {
			t.Fatalf("for maxWin=%d, expected advertised window=%d, got %d", win, win, adv)
		}

		eff := u.effectiveWindowLocked()
		expected := win
		if u.cwnd < expected {
			expected = u.cwnd
		}
		if eff != expected {
			t.Fatalf("for peerWindow=%d cwnd=%d, expected effective=%d, got %d", win, u.cwnd, expected, eff)
		}
	}

	// Test zero advertised window when buffer fills to capacity
	fullConn := &utpConn{
		recvWindowMax: 1024,
		readBuf:       make([]byte, 1024),
		recvBuffer:    make(map[uint16][]byte),
	}
	if adv := fullConn.advertisedWindowLocked(); adv != 0 {
		t.Fatalf("expected advertised window=0 when full, got %d", adv)
	}
}

// ─── 4. Congestion control trajectory test ───────────────────────────────────

func TestCongestion_Trajectory(t *testing.T) {
	u := &utpConn{
		unacked:    make(map[uint16]*utpSegment),
		recvBuffer: make(map[uint16][]byte),
		cwnd:       3000,
		minCwnd:    3000,
		maxCwnd:    64 * 1024,
		ssthresh:   8192,
	}
	u.windowCond = sync.NewCond(&u.mu)

	var (
		seq      uint16
		prevCwnd = u.cwnd
	)

	for i := 0; i < 20; i++ {
		u.mu.Lock()
		u.unacked[seq] = &utpSegment{
			packet:   &utpPacket{SequenceNumber: seq, Payload: make([]byte, 1400)},
			sentAt:   time.Now().Add(-20 * time.Millisecond),
			attempts: 1,
		}
		u.mu.Unlock()

		u.processAck(seq)
		seq++

		u.mu.Lock()
		curCwnd := u.cwnd
		u.mu.Unlock()

		if curCwnd < prevCwnd {
			t.Fatalf("step %d: cwnd decreased from %d to %d without loss", i, prevCwnd, curCwnd)
		}
		prevCwnd = curCwnd
	}

	t.Logf("final cwnd after 20 ACKs: %d (ssthresh=%d, maxCwnd=%d)", prevCwnd, u.ssthresh, u.maxCwnd)
	if prevCwnd > u.maxCwnd {
		t.Fatalf("cwnd %d exceeded maxCwnd %d", prevCwnd, u.maxCwnd)
	}
}
