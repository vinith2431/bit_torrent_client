package main

import (
	"container/heap"
	"testing"
)

// sequential_piece_selection scans pieces from the beginning and
// returns the first piece that is available to the peer.
func sequential_piece_selection(availability []int, bf bitfield) int {
	for i := range availability {
		if availability[i] > 0 && bf.has_piece(i) {
			return i
		}
	}

	return -1
}

// Benchmark sequential piece selection.
func BenchmarkSequentialPieceSelection(b *testing.B) {
	const numPieces = 10000

	availability := make([]int, numPieces)
	for i := range availability {
		availability[i] = (i % 10) + 1
	}

	// Make the rare available piece near the end so the sequential
	// algorithm has to scan most of the piece list.
	bf := make(bitfield, (numPieces+7)/8)
	bf.set_piece(numPieces - 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sequential_piece_selection(availability, bf)
	}
}

// Benchmark rarest-piece selection using the scheduler's priority queue.
func BenchmarkRarestPieceSelection(b *testing.B) {
	const numPieces = 10000

	scheduler := new_piece_scheduler(numPieces)

	bf := make(bitfield, (numPieces+7)/8)

	for i := 0; i < numPieces; i++ {
		bf.set_piece(i)
		scheduler.availability[i] = (i % 10) + 1
		scheduler.queue.items[i].availability = scheduler.availability[i]
	}

	heap.Init(&scheduler.queue)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scheduler.mu.Lock()

		item := heap.Pop(&scheduler.queue).(piece_priority)
		index := item.index

		heap.Push(&scheduler.queue, item)

		scheduler.mu.Unlock()

		_ = index
	}
}
