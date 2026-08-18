// Package lsmkv is a durable log-structured merge-tree key-value library.
package lsmkv

import (
	"errors"
	"sync"

	"github.com/LYH2263/go-durable-engine/internal/filelock"
	"github.com/LYH2263/go-durable-engine/internal/manifest"
	"github.com/LYH2263/go-durable-engine/internal/memtable"
	"github.com/LYH2263/go-durable-engine/internal/sstable"
	"github.com/LYH2263/go-durable-engine/internal/wal"
)

var (
	ErrClosed   = errors.New("lsmkv: closed")
	ErrNotFound = errors.New("lsmkv: not found")
	ErrInvalid  = errors.New("lsmkv: invalid argument")
	ErrReadOnly = errors.New("lsmkv: read only")
)

// SyncPolicy controls when the WAL is fsynced.
type SyncPolicy int

const (
	// SyncFull fsyncs the WAL after every write.
	SyncFull SyncPolicy = iota
	// SyncBatch fsyncs every N writes (see Options.SyncEveryN).
	SyncBatch
	// SyncNone never fsyncs automatically (caller must Sync).
	SyncNone
)

// Options configures database open behavior.
type Options struct {
	// MemtableBytes is the soft flush threshold (default 4MiB).
	MemtableBytes int64
	// SyncPolicy selects WAL durability.
	SyncPolicy SyncPolicy
	// SyncEveryN is used when SyncPolicy == SyncBatch (default 32).
	SyncEveryN int
	// CompactThreshold is the minimum number of L0 SSTs to trigger Compact (default 2).
	CompactThreshold int
	// ReadOnly opens without taking a write lock mutation path (still locks dir).
	ReadOnly bool
}
type DB struct {
	mu     sync.RWMutex
	dir    string
	opts   Options
	lock   *filelock.Lock
	wal    *wal.Log
	mem    *memtable.MemTable
	manif  *manifest.Manifest
	tables []*sstable.Reader
	closed bool
}
type Stats struct {
	MemtableBytes int64
	MemtableKeys  int
	SSTFiles      int
	WALSize       int64
}
type WriteBatch struct {
	entries []wal.BatchEntry
}
type Iterator struct {
	db      *DB
	items   []iterItem
	idx     int
	release func()
	// pinned holds live SST readers; Compact may close them while the iterator is open.
	pinned []*sstable.Reader
}
type iterItem struct {
	key     []byte
	value   []byte
	deleted bool
	seq     uint64
}
