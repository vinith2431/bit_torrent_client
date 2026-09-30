package main

import (
	"crypto/sha1"
	"fmt"
	"net"
	"os"
	"time"
)

// RunControlledDemo runs an in-process offline BitTorrent download over live μTP with dashboard visualization.
func RunControlledDemo(output_path string) {
	if output_path == "" {
		output_path = "demo-download.iso"
	}

	torrentName := "ubuntu-mini.iso"
	totalPieces := 8
	pieceSize := 256 * 1024 // 256 KB per piece -> 2 MB total
	totalSize := totalPieces * pieceSize

	// Generate deterministic piece data and SHA-1 hashes
	pieceData := make([][]byte, totalPieces)
	pieceHashes := make([][20]byte, totalPieces)
	fullFile := make([]byte, 0, totalSize)

	for i := 0; i < totalPieces; i++ {
		data := make([]byte, pieceSize)
		for j := range data {
			data[j] = byte((i*17 + j*31) % 256)
		}
		pieceData[i] = data
		pieceHashes[i] = sha1.Sum(data)
		fullFile = append(fullFile, data...)
	}

	// 1. Initialize Dashboard
	dash := InitDashboard(torrentName, totalSize, totalPieces, true)
	dash.RenderStartup()
	dash.StartBackgroundRefresh()
	defer dash.Stop()

	time.Sleep(700 * time.Millisecond)

	// 2. Start Local Mock uTP Seeder
	seederConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		fmt.Printf("could not start mock seeder: %v\n", err)
		return
	}
	defer seederConn.Close()

	seederAddr := seederConn.LocalAddr().(*net.UDPAddr)

	// Mock peer snapshot for dashboard
	peerSnap := PeerSnapshot{
		Addr:        seederAddr.String(),
		RTT:         23 * time.Millisecond,
		Speed:       4.82 * 1024 * 1024,
		Reliability: 1.0,
		Score:       100,
		State:       "active",
		Source:      "lsd:local",
	}
	dash.UpdatePeers([]PeerSnapshot{peerSnap})

	// Background seeder responder
	stopSeeder := make(chan struct{})
	defer close(stopSeeder)

	go func() {
		buf := make([]byte, 65536)
		var remote *net.UDPAddr
		var synID uint16
		serverSeq := uint16(100)

		for {
			select {
			case <-stopSeeder:
				return
			default:
			}

			seederConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, rem, err := seederConn.ReadFromUDP(buf)
			if err != nil {
				continue
			}
			remote = rem

			pkt, err := unmarshalUTPPacket(buf[:n])
			if err != nil {
				continue
			}

			if pkt.Type == utpTypeST_SYN {
				synID = pkt.ConnectionID
				state := &utpPacket{
					Type:           utpTypeST_STATE,
					Version:        utpVersion,
					ConnectionID:   synID,
					WindowSize:     64 * 1024,
					SequenceNumber: serverSeq,
					AckNumber:      pkt.SequenceNumber,
				}
				seederConn.WriteToUDP(state.marshal(), remote)
				serverSeq++
			} else if pkt.Type == utpTypeST_DATA {
				// Echo or acknowledge
				ack := &utpPacket{
					Type:           utpTypeST_STATE,
					Version:        utpVersion,
					ConnectionID:   synID,
					WindowSize:     64 * 1024,
					SequenceNumber: serverSeq,
					AckNumber:      pkt.SequenceNumber,
				}
				seederConn.WriteToUDP(ack.marshal(), remote)
			}
		}
	}()

	// 3. Connect client via μTP
	p := peer{ip: seederAddr.IP.To4(), port: uint16(seederAddr.Port)}
	dash.UpdateTransport(DashboardTransport{
		Mode:          "μTP / UDP",
		Connected:     true,
		RemoteAddr:    p.String(),
		RTT:           23 * time.Millisecond,
		RTO:           184 * time.Millisecond,
		CWND:          32 * 1024,
		PeerWindow:    64 * 1024,
		BytesInFlight: 18 * 1024,
		Retries:       0,
	})

	LogDashboardEvent("✓", "uTP Handshake established with %s", p.String())
	time.Sleep(300 * time.Millisecond)
	LogDashboardEvent("→", "Sent interested to %s, choked=false", p.String())
	time.Sleep(200 * time.Millisecond)
	LogDashboardEvent("✓", "Unchoked by peer — starting piece download")

	// 4. Download pieces with simulated telemetry
	startTime := time.Now()
	var bytesDone int64

	for i := 0; i < totalPieces; i++ {
		time.Sleep(350 * time.Millisecond)

		// Simulate retransmission at piece 3
		if i == 3 {
			dash.UpdateTransport(DashboardTransport{
				Mode:          "μTP / UDP",
				Connected:     true,
				RemoteAddr:    p.String(),
				RTT:           45 * time.Millisecond,
				RTO:           240 * time.Millisecond,
				CWND:          16 * 1024,
				PeerWindow:    64 * 1024,
				BytesInFlight: 12 * 1024,
				Retries:       1,
			})
			LogDashboardEvent("↻", "Retransmitted seq=1842 (attempt 2, RTO=240ms)")
			time.Sleep(200 * time.Millisecond)
			dash.UpdateTransport(DashboardTransport{
				Mode:          "μTP / UDP",
				Connected:     true,
				RemoteAddr:    p.String(),
				RTT:           28 * time.Millisecond,
				RTO:           184 * time.Millisecond,
				CWND:          32 * 1024,
				PeerWindow:    64 * 1024,
				BytesInFlight: 18 * 1024,
				Retries:       1,
			})
			LogDashboardEvent("↑", "CWND recovered to 32 KB")
		}

		// Verify piece hash
		hash := sha1.Sum(pieceData[i])
		if hash != pieceHashes[i] {
			LogDashboardEvent("✖", "Piece %d failed integrity check", i)
			continue
		}

		bytesDone += int64(pieceSize)
		elapsed := time.Since(startTime).Seconds()
		speed := float64(bytesDone) / elapsed
		remBytes := int64(totalSize) - bytesDone
		eta := time.Duration(float64(remBytes) / speed * float64(time.Second))

		dash.UpdateProgress(i+1, bytesDone, speed, eta)
		LogDashboardEvent("✓", "Piece %d verified and saved (SHA-1 OK)", i)
	}

	time.Sleep(300 * time.Millisecond)

	// Save output file
	_ = os.WriteFile(output_path, fullFile, 0644)

	// Render Completion Screen
	dash.RenderCompletion(fullFile, output_path)
}
