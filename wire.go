package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type bitfield []byte

func (bf bitfield) has_piece(index int) bool {
	byte_index := index / 8
	offset := index % 8
	if byte_index < 0 || byte_index >= len(bf) {
		return false
	}
	return bf[byte_index]>>uint(7-offset)&1 != 0
}

func (bf bitfield) set_piece(index int) {
	byte_index := index / 8
	if byte_index < 0 || byte_index >= len(bf) {
		return
	}
	offset := index % 8
	bf[byte_index] |= 1 << uint(7-offset)
}

type handshake struct {
	pstr      string // always "BitTorrent Protocol"
	info_hash [20]byte
	peer_id   [20]byte
}

func new_handshake(info_hash [20]byte, peer_id [20]byte) *handshake {
	return &handshake{
		pstr:      "BitTorrent protocol",
		info_hash: info_hash,
		peer_id:   peer_id,
	}
}

func (h *handshake) serialize() []byte {
	buf := make([]byte, len(h.pstr)+49)
	// 49 = 1 (length prefix) + 8 (reserved) + 20 (infohash) + 20 (peer id)
	buf[0] = byte(len(h.pstr))

	curr := 1
	curr += copy(buf[curr:], h.pstr)
	curr += copy(buf[curr:], make([]byte, 8))
	curr += copy(buf[curr:], h.info_hash[:])
	curr += copy(buf[curr:], h.peer_id[:])

	return buf
}

func read_handshake(conn peer_conn) (*handshake, error) {
	len_buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, len_buf); err != nil {
		return nil, err
	}

	pstr_len := int(len_buf[0])
	if pstr_len == 0 {
		return nil, fmt.Errorf("pstr length is 0")
	}

	buf := make([]byte, pstr_len+48)
	// 48 = 8 (reserved) + 20 (infohash) + 20 (peer id)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}

	var info_hash, peer_id [20]byte
	copy(info_hash[:], buf[pstr_len+8:pstr_len+28])
	copy(peer_id[:], buf[pstr_len+28:])

	return &handshake{
		pstr:      string(buf[0:pstr_len]),
		info_hash: info_hash,
		peer_id:   peer_id,
	}, nil
}

type message_id uint8

const (
	msg_choke          message_id = 0
	msg_unchoke        message_id = 1
	msg_interested     message_id = 2
	msg_not_interested message_id = 3
	msg_have           message_id = 4
	msg_bitfield       message_id = 5
	msg_request        message_id = 6
	msg_piece          message_id = 7
	msg_cancel         message_id = 8
)

type message struct {
	id      message_id
	payload []byte
}

func (m *message) serialize() []byte {
	if m == nil {
		return make([]byte, 4)
	}
	length := uint32(len(m.payload) + 1)
	buf := make([]byte, length+4)
	buf[4] = byte(m.id)
	binary.BigEndian.PutUint32(buf[0:4], length)
	copy(buf[5:], m.payload)
	return buf
}

func read_message(conn peer_conn) (*message, error) {
	len_buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, len_buf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(len_buf)

	if length == 0 {
		return nil, nil
	}

	msg_buf := make([]byte, length)
	if _, err := io.ReadFull(conn, msg_buf); err != nil {
		return nil, err
	}

	return &message{
		id:      message_id(msg_buf[0]),
		payload: msg_buf[1:],
	}, nil
}

func format_request(index, begin, length int) *message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &message{
		id:      msg_request,
		payload: payload,
	}
}

func format_have(index int) *message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	return &message{
		id:      msg_have,
		payload: payload,
	}
}

func parse_piece(index int, buf []byte, msg *message) (int, error) {
	if msg.id != msg_piece {
		return 0, fmt.Errorf("expected piece (id %d), got id %d", msg_piece, msg.id)
	}
	if len(msg.payload) < 8 {
		return 0, fmt.Errorf("payload too short: %d < 8", len(msg.payload))
	}

	parsed_index := int(binary.BigEndian.Uint32(msg.payload[0:4]))
	if parsed_index != index {
		return 0, fmt.Errorf("expected piece %d, got %d", index, parsed_index)
	}

	begin := int(binary.BigEndian.Uint32(msg.payload[4:8]))
	if begin >= len(buf) {
		return 0, fmt.Errorf("begin offset %d out of range", begin)
	}

	data := msg.payload[8:]
	if begin+len(data) > len(buf) {
		return 0, fmt.Errorf("data too long: begin %d + len %d > buf %d", begin, len(data), len(buf))
	}
	copy(buf[begin:], data)
	return len(data), nil
}

func parse_have(msg *message) (int, error) {
	if msg.id != msg_have {
		return 0, fmt.Errorf("expected have (id %d), got id %d", msg_have, msg.id)
	}
	if len(msg.payload) != 4 {
		return 0, fmt.Errorf("expected payload length 4, got %d", len(msg.payload))
	}
	return int(binary.BigEndian.Uint32(msg.payload)), nil
}
