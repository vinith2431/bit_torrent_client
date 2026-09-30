package main

import (
	"bytes"
	"io"
	"net"
	"testing"
)

// BenchmarkThroughput_TCP measures raw TCP write/read loopback throughput with 1400-byte blocks.
func BenchmarkThroughput_TCP(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("TCP listen failed: %v", err)
	}
	defer l.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(conn, conn)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		b.Fatalf("TCP dial failed: %v", err)
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte("T"), 1400)
	rbuf := make([]byte, 1400)

	b.SetBytes(1400)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatalf("TCP Write failed: %v", err)
		}
		if _, err := io.ReadFull(conn, rbuf); err != nil {
			b.Fatalf("TCP Read failed: %v", err)
		}
	}
}

// BenchmarkThroughput_UTP measures uTP write/read loopback throughput with 1400-byte blocks.
func BenchmarkThroughput_UTP(b *testing.B) {
	udpListener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatalf("ListenUDP: %v", err)
	}
	defer udpListener.Close()

	addr := udpListener.LocalAddr().(*net.UDPAddr)

	stopServer := make(chan struct{})
	defer close(stopServer)

	go func() {
		buf := make([]byte, 2048)
		var remote *net.UDPAddr
		var synConnID uint16
		serverSeq := uint16(100)

		// Handshake
		n, rem, err := udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		remote = rem
		syn, err := unmarshalUTPPacket(buf[:n])
		if err != nil {
			return
		}
		synConnID = syn.ConnectionID

		state := &utpPacket{
			Type:           utpTypeST_STATE,
			Version:        utpVersion,
			ConnectionID:   synConnID,
			WindowSize:     64 * 1024,
			SequenceNumber: serverSeq,
			AckNumber:      syn.SequenceNumber,
		}
		udpListener.WriteToUDP(state.marshal(), remote)
		serverSeq++

		// Loop echoing DATA packets back
		for {
			select {
			case <-stopServer:
				return
			default:
			}

			n, rem, err := udpListener.ReadFromUDP(buf)
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
					ConnectionID:   synConnID,
					WindowSize:     64 * 1024,
					SequenceNumber: serverSeq,
					AckNumber:      pkt.SequenceNumber,
					Payload:        pkt.Payload,
				}
				udpListener.WriteToUDP(echo.marshal(), remote)
				serverSeq++
			}
		}
	}()

	p := peer{ip: addr.IP.To4(), port: uint16(addr.Port)}
	conn, err := dialUTP(p)
	if err != nil {
		b.Fatalf("dialUTP failed: %v", err)
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte("U"), 1400)
	rbuf := make([]byte, 1400)

	b.SetBytes(1400)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatalf("uTP Write failed: %v", err)
		}
		if _, err := io.ReadFull(conn, rbuf); err != nil {
			b.Fatalf("uTP Read failed: %v", err)
		}
	}
}
