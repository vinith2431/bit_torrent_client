package main

import (
	"net"
	"time"
)

type transport_mode int

const (
	transport_tcp transport_mode = iota
	transport_utp
)

func (m transport_mode) String() string {
	switch m {
	case transport_utp:
		return "μTP / UDP"
	case transport_tcp:
		return "TCP"
	default:
		return "Unknown"
	}
}

func dial_peer(p peer, mode transport_mode) (peer_conn, error) {
	switch mode {
	case transport_tcp:
		return net.DialTimeout("tcp", p.String(), 10*time.Second)
	case transport_utp:
		return dialUTP(p)
	default:
		return nil, net.UnknownNetworkError("unsupported transport")
	}
}

func dial_with_fallback(p peer) (peer_conn, transport_mode, error) {
	conn, err := dial_peer(p, transport_utp)
	if err == nil {
		return conn, transport_utp, nil
	}

	conn, err = dial_peer(p, transport_tcp)
	if err == nil {
		return conn, transport_tcp, nil
	}

	return nil, transport_tcp, err
}
