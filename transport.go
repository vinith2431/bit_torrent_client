package main

import (
	"net"
	"time"
)

func dial_tcp(p peer) (peer_conn, error) {
	return net.DialTimeout("tcp", p.String(), 10*time.Second)
}

func dial_peer(p peer) (peer_conn, error) {
	return dial_tcp(p)
}
