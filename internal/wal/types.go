// Package wal implements a CRC-framed write-ahead log with truncated-tail recovery.
package wal

import (
	"errors"
	"os"
	"sync"
)

var (
	ErrClosed     = errors.New("wal: closed")
	ErrCorrupt    = errors.New("wal: corrupt record")
	ErrBadType    = errors.New("wal: bad record type")
	ErrShortWrite = errors.New("wal: short write")
)

// InjectWriteErr, when non-nil, is consumed by the next writeFrame call.
// Tests use it to simulate a WAL I/O failure without depending on chmod.
var InjectWriteErr error

// Record types.
const (
	TypePut    byte = 1
	TypeDelete byte = 2
	TypeBatch  byte = 3
)

const (
	headerSize = 4 + 1 + 4 // length(u32 LE) + type + crc32 LE over (type||payload)
	fileName   = "wal.log"
)

// Record is one decoded WAL entry.
type Record struct {
	Type    byte
	Key     []byte
	Value   []byte
	Seq     uint64
	Deleted bool
}
type Options struct {
	// SyncEveryN syncs after every N appends; 0 means every append when SyncOnWrite is true.
	SyncEveryN int
	// SyncOnWrite forces Sync after each Append/AppendBatch when SyncEveryN==0.
	SyncOnWrite bool
	// FileName overrides the default wal.log name.
	FileName string
}
type BatchEntry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}
type Log struct {
	mu      sync.Mutex
	dir     string
	path    string
	f       *os.File
	opts    Options
	appends int
	closed  bool
	size    int64
	nextSeq uint64
}
