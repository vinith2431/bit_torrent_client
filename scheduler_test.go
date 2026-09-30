package main

import "testing"

func make_bitfield(num_pieces int, pieces ...int) bitfield {
	bf := make(bitfield, (num_pieces+7)/8)

	for _, piece := range pieces {
		bf.set_piece(piece)
	}

	return bf
}

func TestSchedulerSelectsRarestPiece(t *testing.T) {
	const numPieces = 6

	scheduler := new_piece_scheduler(numPieces)

	// Piece availability:
	//
	// Piece 0 -> 3 peers
	// Piece 1 -> 2 peers
	// Piece 2 -> 3 peers
	// Piece 3 -> 1 peer
	// Piece 4 -> 2 peers
	// Piece 5 -> 3 peers

	scheduler.add_peer(
		make_bitfield(numPieces, 0, 1, 2, 3),
	)

	scheduler.add_peer(
		make_bitfield(numPieces, 0, 1, 2, 4),
	)

	scheduler.add_peer(
		make_bitfield(numPieces, 0, 2, 5),
	)

	peer := make_bitfield(numPieces, 0, 1, 2, 3, 4, 5)

	got := scheduler.next(peer)

	if got != 3 {
		t.Fatalf(
			"expected rarest piece 3, got %d",
			got,
		)
	}
}

func TestSchedulerDoesNotDuplicateActivePiece(t *testing.T) {
	const numPieces = 4

	scheduler := new_piece_scheduler(numPieces)

	scheduler.add_peer(
		make_bitfield(numPieces, 0, 1, 2, 3),
	)

	peer := make_bitfield(numPieces, 0, 1, 2, 3)

	first := scheduler.next(peer)

	if first == -1 {
		t.Fatal("first scheduler selection returned -1")
	}

	second := scheduler.next(peer)

	if second == -1 {
		t.Fatal("second scheduler selection returned -1")
	}

	if first == second {
		t.Fatalf(
			"same piece was assigned twice: piece %d",
			first,
		)
	}
}

func TestSchedulerReleaseMakesPieceAvailableAgain(t *testing.T) {
	const numPieces = 3

	scheduler := new_piece_scheduler(numPieces)

	scheduler.add_peer(
		make_bitfield(numPieces, 0, 1, 2),
	)

	peer := make_bitfield(numPieces, 0, 1, 2)

	first := scheduler.next(peer)

	if first == -1 {
		t.Fatal("scheduler returned -1")
	}

	scheduler.release(first)

	second := scheduler.next(peer)

	if second != first {
		t.Fatalf(
			"released piece %d was not made available again; got %d",
			first,
			second,
		)
	}
}

func TestSchedulerCompletedPieceIsNeverSelected(t *testing.T) {
	const numPieces = 3

	scheduler := new_piece_scheduler(numPieces)

	scheduler.add_peer(
		make_bitfield(numPieces, 0, 1, 2),
	)

	scheduler.set_complete(0)

	peer := make_bitfield(numPieces, 0, 1, 2)

	got := scheduler.next(peer)

	if got == 0 {
		t.Fatal("completed piece 0 was selected")
	}

	if got == -1 {
		t.Fatal("scheduler failed to select another available piece")
	}
}

func TestSchedulerUpdatesAvailabilityWhenPeerLeaves(t *testing.T) {
	const numPieces = 3

	scheduler := new_piece_scheduler(numPieces)

	peerA := make_bitfield(numPieces, 0, 1)
	peerB := make_bitfield(numPieces, 0)

	scheduler.add_peer(peerA)
	scheduler.add_peer(peerB)

	// Piece 1 is available from only Peer A.
	// Piece 0 is available from both peers.
	//
	// Therefore Piece 1 should be selected.
	peer := make_bitfield(numPieces, 0, 1)

	got := scheduler.next(peer)

	if got != 1 {
		t.Fatalf(
			"expected piece 1 as rarest piece, got %d",
			got,
		)
	}

	// Release it before testing the next condition.
	scheduler.release(1)

	// Peer A leaves.
	scheduler.remove_peer(peerA)

	// Now piece 0 should have availability 1,
	// and piece 1 should have availability 0.
	//
	// Since this peer still has both pieces, only piece 0
	// should be considered available.
	got = scheduler.next(peer)

	if got != 0 {
		t.Fatalf(
			"expected piece 0 after peer removal, got %d",
			got,
		)
	}
}

func TestSchedulerUpdatesAvailabilityOnHave(t *testing.T) {
	numPieces := 4
	scheduler := new_piece_scheduler(numPieces)

	// Two peers initially have pieces 0 and 1.
	peerA := make_bitfield(numPieces, 0, 1)
	peerB := make_bitfield(numPieces, 0, 1)

	scheduler.add_peer(peerA)
	scheduler.add_peer(peerB)

	// Piece 2 is initially unavailable.
	if scheduler.availability[2] != 0 {
		t.Fatalf(
			"expected piece 2 availability 0, got %d",
			scheduler.availability[2],
		)
	}

	// Peer A announces piece 2 through a HAVE message.
	peerA.set_piece(2)
	scheduler.add_piece_to_peer(2)

	// Piece 2 should now have availability 1.
	if scheduler.availability[2] != 1 {
		t.Fatalf(
			"expected piece 2 availability 1 after HAVE, got %d",
			scheduler.availability[2],
		)
	}

	// Peer A can now provide piece 2.
	got := scheduler.next(peerA)

	// Piece 2 is now the rarest available piece:
	// piece 0 = 2 peers
	// piece 1 = 2 peers
	// piece 2 = 1 peer
	if got != 2 {
		t.Fatalf(
			"expected rarest piece 2 after HAVE, got %d",
			got,
		)
	}
}

func TestSchedulerAllowsLimitedEndgameDuplicate(t *testing.T) {
	numPieces := 3
	scheduler := new_piece_scheduler(numPieces)

	peer := make_bitfield(numPieces, 0, 1, 2)
	scheduler.add_peer(peer)

	// Leave only piece 2 incomplete.
	scheduler.set_complete(0)
	scheduler.set_complete(1)

	first := scheduler.next(peer)

	if first != 2 {
		t.Fatalf("expected piece 2 as first end-game claim, got %d", first)
	}

	second := scheduler.next(peer)

	if second != 2 {
		t.Fatalf("expected piece 2 as second end-game claim, got %d", second)
	}

	third := scheduler.next(peer)

	if third != -1 {
		t.Fatalf("expected no third claim for piece 2, got %d", third)
	}
}

func TestSchedulerEndgameRelease(t *testing.T) {
	numPieces := 3
	scheduler := new_piece_scheduler(numPieces)

	peer := make_bitfield(numPieces, 0, 1, 2)
	scheduler.add_peer(peer)

	// Leave only piece 2 incomplete.
	scheduler.set_complete(0)
	scheduler.set_complete(1)

	// Two workers claim the same final piece.
	first := scheduler.next(peer)
	second := scheduler.next(peer)

	if first != 2 || second != 2 {
		t.Fatalf("expected two end-game claims for piece 2, got %d and %d",
			first, second)
	}

	if scheduler.active[2] != 2 {
		t.Fatalf("expected 2 active claims, got %d", scheduler.active[2])
	}

	// One worker fails.
	scheduler.release(2)

	if scheduler.active[2] != 1 {
		t.Fatalf("expected 1 active claim after release, got %d",
			scheduler.active[2])
	}

	// The remaining worker is still downloading the piece.
	third := scheduler.next(peer)

	if third != 2 {
		t.Fatalf("expected piece 2 to be reclaimed after one worker failed, got %d",
			third)
	}

	if scheduler.active[2] != 2 {
		t.Fatalf("expected 2 active claims after reclaim, got %d",
			scheduler.active[2])
	}
}

func TestSchedulerFirstCompletedEndgameResultWins(t *testing.T) {
	numPieces := 3
	scheduler := new_piece_scheduler(numPieces)

	peer := make_bitfield(numPieces, 0, 1, 2)
	scheduler.add_peer(peer)

	// Only piece 2 remains.
	scheduler.set_complete(0)
	scheduler.set_complete(1)

	// Two workers claim the same final piece.
	first := scheduler.next(peer)
	second := scheduler.next(peer)

	if first != 2 || second != 2 {
		t.Fatalf("expected two end-game claims for piece 2, got %d and %d",
			first, second)
	}

	if scheduler.active[2] != 2 {
		t.Fatalf("expected 2 active claims, got %d",
			scheduler.active[2])
	}

	// First valid result completes the piece.
	scheduler.set_complete(2)

	if !scheduler.is_complete(2) {
		t.Fatal("expected piece 2 to be complete")
	}

	if scheduler.active[2] != 0 {
		t.Fatalf("expected active claims to reset to 0, got %d",
			scheduler.active[2])
	}

	// A late duplicate result must not make the piece downloadable again.
	duplicate := scheduler.next(peer)

	if duplicate != -1 {
		t.Fatalf("expected completed piece to reject duplicate claim, got %d",
			duplicate)
	}
}


