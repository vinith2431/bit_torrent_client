package main

import (
	"net"
	"testing"
	"time"
)

func TestTransportModes(t *testing.T) {
	if transport_tcp == transport_utp {
		t.Fatal("TCP and uTP transport modes must be different")
	}
}

func TestDialWithFallback_FallbackToTCP(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on TCP: %v", err)
	}
	defer l.Close()

	tcpAddr := l.Addr().(*net.TCPAddr)
	p := peer{
		ip:   tcpAddr.IP.To4(),
		port: uint16(tcpAddr.Port),
	}

	go func() {
		c, err := l.Accept()
		if err == nil {
			c.Close()
		}
	}()

	conn, mode, err := dial_with_fallback(p)
	if err != nil {
		t.Fatalf("dial_with_fallback failed: %v", err)
	}
	defer conn.Close()

	if mode != transport_tcp {
		t.Fatalf("expected mode transport_tcp, got %v", mode)
	}
}

func TestDialWithFallback_ReturnsUTPWhenAvailable(t *testing.T) {
	udpListener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to listen UDP: %v", err)
	}
	defer udpListener.Close()

	addr := udpListener.LocalAddr().(*net.UDPAddr)

	go func() {
		buf := make([]byte, 1500)
		n, remote, err := udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}

		pkt, err := unmarshalUTPPacket(buf[:n])
		if err != nil || pkt.Type != utpTypeST_SYN {
			return
		}

		resp := &utpPacket{
			Type:           utpTypeST_STATE,
			Version:        utpVersion,
			ConnectionID:   pkt.ConnectionID,
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

	conn, mode, err := dial_with_fallback(p)
	if err != nil {
		t.Fatalf("dial_with_fallback to uTP server failed: %v", err)
	}
	defer conn.Close()

	if mode != transport_utp {
		t.Fatalf("expected transport_utp, got %v", mode)
	}
}

func TestDialWithFallback_BothUnavailable(t *testing.T) {
	// Pick an unused port by briefly binding and closing
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	port := uint16(l.Addr().(*net.TCPAddr).Port)
	l.Close()

	p := peer{
		ip:   net.IPv4(127, 0, 0, 1).To4(),
		port: port,
	}

	start := time.Now()
	_, _, err = dial_with_fallback(p)
	if err == nil {
		t.Fatal("expected error when both uTP and TCP unavailable, got nil")
	}
	t.Logf("both unavailable failed correctly in %v with: %v", time.Since(start), err)
}
