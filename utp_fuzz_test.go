package main

import (
	"testing"
)

// FuzzUnmarshalUTPPacket fuzz-tests the packet unmarshaler against arbitrary byte sequences.
// The invariant is that unmarshalUTPPacket MUST NEVER panic regardless of input.
func FuzzUnmarshalUTPPacket(f *testing.F) {
	// Seed corpus with various valid and boundary packets
	f.Add(make([]byte, 20))
	f.Add([]byte{})
	f.Add([]byte{0x01}) // 1 byte

	// Valid DATA packet header
	validData := make([]byte, 20)
	validData[0] = (utpTypeST_DATA << 4) | utpVersion
	f.Add(validData)

	// Valid SYN packet header with payload
	validSyn := make([]byte, 25)
	validSyn[0] = (utpTypeST_SYN << 4) | utpVersion
	f.Add(validSyn)

	// Valid STATE packet
	validState := make([]byte, 20)
	validState[0] = (utpTypeST_STATE << 4) | utpVersion
	f.Add(validState)

	// Invalid version / invalid type seeds
	invalidType := make([]byte, 20)
	invalidType[0] = (0x0F << 4) | utpVersion
	f.Add(invalidType)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Target parser: must not panic on any arbitrary byte input
		_, _ = unmarshalUTPPacket(data)
	})
}
