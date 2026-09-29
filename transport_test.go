package main

import (
	"testing"
)

func TestTransportModes(t *testing.T) {
	if transport_tcp == transport_utp {
		t.Fatal("TCP and uTP transport modes must be different")
	}
}

func TestUTPNotImplemented(t *testing.T) {
	p := peer{
		ip:   []byte{127, 0, 0, 1},
		port: 1,
	}

	conn, err := dial_peer(p, transport_utp)

	if err == nil {
		if conn != nil {
			conn.Close()
		}
		t.Fatal("expected uTP to report not implemented")
	}
}
