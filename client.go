package main

import (
	"fmt"
	"net"
	"time"
)

type client struct {
	conn      net.Conn
	peer      peer
	info_hash [20]byte
	peer_id   [20]byte
	bitfield  bitfield
	choked    bool
}

func new_client(p peer, info_hash [20]byte, peer_id [20]byte, num_pieces int) (*client, error) {
	conn, err := net.DialTimeout("tcp", p.String(), 10*time.Second)
	if err != nil {
		return nil, err
	}

	if err := do_handshake(conn, info_hash, peer_id); err != nil {
		conn.Close()
		return nil, err
	}

	bf, choked, err := recv_bitfield(conn, num_pieces)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &client{
		conn:      conn,
		peer:      p,
		info_hash: info_hash,
		peer_id:   peer_id,
		bitfield:  bf,
		choked:    choked,
	}, nil
}

func do_handshake(conn net.Conn, info_hash [20]byte, peer_id [20]byte) error {
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	defer conn.SetDeadline(time.Time{})

	h := new_handshake(info_hash, peer_id)
	if _, err := conn.Write(h.serialize()); err != nil {
		return err
	}

	received, err := read_handshake(conn)
	if err != nil {
		return err
	}

	if received.info_hash != info_hash {
		return fmt.Errorf("expected infohash %x, got %x", info_hash, received.info_hash)
	}

	return nil
}

func recv_bitfield(conn net.Conn, num_pieces int) (bitfield, bool, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	bf := make(bitfield, (num_pieces+7)/8)
	choked := true
	for {
		msg, err := read_message(conn)
		if err != nil {
			return bf, choked, nil
		}
		if msg == nil {
			continue
		}
		switch msg.id {
		case msg_bitfield:
			return bitfield(msg.payload), choked, nil
		case msg_have:
			if i, err := parse_have(msg); err == nil {
				bf.set_piece(i)
			}
		case msg_unchoke:
			choked = false
		case msg_choke:
			choked = true
		}
	}
}

// send_request sends a request message asking for a block.
func (c *client) send_request(index, begin, length int) error {
	msg := format_request(index, begin, length)
	_, err := c.conn.Write(msg.serialize())
	return err
}

// send_interested tells the peer we want pieces from them.
func (c *client) send_interested() error {
	msg := &message{id: msg_interested}
	_, err := c.conn.Write(msg.serialize())
	return err
}

// send_not_interested tells the peer we don't need anything from them.
func (c *client) send_not_interested() error {
	msg := &message{id: msg_not_interested}
	_, err := c.conn.Write(msg.serialize())
	return err
}

// send_unchoke tells the peer they can request pieces from us.
func (c *client) send_unchoke() error {
	msg := &message{id: msg_unchoke}
	_, err := c.conn.Write(msg.serialize())
	return err
}

// send_have tells the peer we finished downloading a piece.
func (c *client) send_have(index int) error {
	msg := format_have(index)
	_, err := c.conn.Write(msg.serialize())
	return err
}

// read reads one message from the peer and updates client state.
func (c *client) read() (*message, error) {
	msg, err := read_message(c.conn)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
