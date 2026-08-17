// Package compact merges SSTables while respecting sequence numbers and tombstones.
package compact

import (
	"errors"

	"github.com/LYH2263/go-durable-engine/internal/sstable"
)

var (
	ErrNoInputs = errors.New("compact: no inputs")
	ErrInvalid  = errors.New("compact: invalid argument")
)

// Input describes an SSTable to merge.
type Input struct {
	Path  string
	Level int
}
type Result struct {
	OutputPath      string
	Meta            *sstable.Meta
	Removed         []string
	KeptTombstones  int
	DroppedObsolete int
}
type Options struct {
	// DropTombstones drops tombstones from the output (dangerous unless no lower data).
	DropTombstones bool
	OutputLevel    int
	BlockSize      int
}
type FileRef struct {
	Path  string
	Name  string
	Level int
	Size  int64
}
type mergeItem struct {
	idx     int
	key     []byte
	value   []byte
	seq     uint64
	deleted bool
}
type mergeHeap []*mergeItem
