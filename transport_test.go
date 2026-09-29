package main

import (
	"net"
	"testing"
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
