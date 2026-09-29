package main

import (
	"bytes"
	"crypto/sha1"
	"os"
	"testing"
)

func TestResumeStateAndVerification(t *testing.T) {
	oldName := ""
	t.Cleanup(func() {
		if oldName != "" {
			os.Remove(oldName + ".part")
			os.Remove(oldName + ".resume")
		}
	})

	data := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	const pieceLength = 16

	torrent := torrent_file{
		piece_length: pieceLength,
		length:       len(data),
		name:         "resume-test.bin",
	}

	copy(torrent.info_hash[:], "resume-test-info!!")

	for start := 0; start < len(data); start += pieceLength {
		end := start + pieceLength
		if end > len(data) {
			end = len(data)
		}

		torrent.piece_hashes = append(
			torrent.piece_hashes,
			sha1.Sum(data[start:end]),
		)
	}

	oldName = torrent.name

	partPath, resumePath := resume_paths(&torrent)

	partFile, err := os.OpenFile(
		partPath,
		os.O_CREATE|os.O_RDWR|os.O_TRUNC,
		0644,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer partFile.Close()

	if err := partFile.Truncate(int64(torrent.length)); err != nil {
		t.Fatal(err)
	}

	// Pretend that the first two pieces were already downloaded.
	complete := make([]bool, len(torrent.piece_hashes))

	for index := 0; index < 2 && index < len(complete); index++ {
		start := index * pieceLength
		end := start + pieceLength

		if end > len(data) {
			end = len(data)
		}

		if _, err := partFile.WriteAt(data[start:end], int64(start)); err != nil {
			t.Fatal(err)
		}

		complete[index] = true
	}

	if err := save_resume_state(&torrent, complete); err != nil {
		t.Fatal(err)
	}

	// Verify that the resume file can be loaded.
	loaded, err := load_resume_state(&torrent)
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded) != len(complete) {
		t.Fatalf(
			"loaded %d pieces, expected %d",
			len(loaded),
			len(complete),
		)
	}

	for i := range complete {
		if loaded[i] != complete[i] {
			t.Fatalf(
				"piece %d: loaded=%v expected=%v",
				i,
				loaded[i],
				complete[i],
			)
		}
	}

	// Verify that valid saved pieces remain marked complete.
	verified, err := verify_saved_pieces(
		&torrent,
		partFile,
		loaded,
	)
	if err != nil {
		t.Fatal(err)
	}

	if verified != 2 {
		t.Fatalf(
			"verified %d pieces, expected 2",
			verified,
		)
	}

	// Corrupt the first completed piece.
	corrupt := bytes.Repeat([]byte{0xff}, pieceLength)

	if _, err := partFile.WriteAt(corrupt, 0); err != nil {
		t.Fatal(err)
	}

	verified, err = verify_saved_pieces(
		&torrent,
		partFile,
		loaded,
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded[0] {
		t.Fatal("corrupted piece 0 was still marked complete")
	}

	if verified != 1 {
		t.Fatalf(
			"verified %d pieces after corruption, expected 1",
			verified,
		)
	}

	_ = resumePath
}
