package main

import (
	"io"
	"time"
)

type peer_conn interface {
	io.Reader
	io.Writer
	io.Closer
	SetDeadline(time.Time) error
}
