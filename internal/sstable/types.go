// Package sstable implements block-based SSTables with restart points and bloom filters.
package sstable

import (
	"errors"
	"os"

	"github.com/LYH2263/go-durable-engine/internal/bloomx"
)

var (
	ErrCorrupt  = errors.New("sstable: corrupt file")
	ErrClosed   = errors.New("sstable: closed")
	ErrNotFound = errors.New("sstable: not found")
	ErrInvalid  = errors.New("sstable: invalid argument")
	ErrBadMagic = errors.New("sstable: bad magic")
)

const (
	magicFooter      = uint32(0x53535442) // SSTB
	restartInterval  = 16
	defaultBlockSize = 4096
	flagDelete       = byte(1)
	flagValue        = byte(0)
)

// Entry is one logical KV written to an SSTable.
type Entry struct {
	Key     []byte
	Value   []byte
	Seq     uint64
	Deleted bool
}
type WriterOptions struct {
	BlockSize       int
	RestartInterval int
	BloomFP         float64
}
type Writer struct {
	f            *os.File
	path         string
	blockSize    int
	restartInt   int
	buf          []byte
	restarts     []uint32
	entryCount   int
	blockEntries int
	lastKey      []byte
	index        []indexRec
	bloom        *bloomx.Builder
	minKey       []byte
	maxKey       []byte
	maxSeq       uint64
	closed       bool
}
type indexRec struct {
	lastKey []byte
	offset  uint64
	length  uint64
}
type Meta struct {
	Path     string
	MinKey   []byte
	MaxKey   []byte
	MaxSeq   uint64
	Entries  uint64
	FileSize int64
}
type Reader struct {
	f        *os.File
	path     string
	index    []indexRec
	bloom    *bloomx.Filter
	maxSeq   uint64
	entries  uint64
	minKey   []byte
	maxKey   []byte
	fileSize int64
}
type blockIter struct {
	data     []byte
	restarts []uint32
	off      int
	key      []byte
	value    []byte
	seq      uint64
	deleted  bool
	valid    bool
}
type Iterator struct {
	r       *Reader
	bi      int
	blockIt *blockIter
	err     error
}
