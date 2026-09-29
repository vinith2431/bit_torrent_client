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

func dial_peer(p peer, mode transport_mode) (peer_conn, error) {
	switch mode {
	case transport_tcp:
		return net.DialTimeout("tcp", p.String(), 10*time.Second)
	case transport_utp:
		return nil, net.UnknownNetworkError("uTP transport not implemented")
	default:
		return nil, net.UnknownNetworkError("unsupported transport")
	}
}
