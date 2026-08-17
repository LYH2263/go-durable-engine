package lsmkv

import (
	"os"
	"path/filepath"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/manifest"
	"github.com/LYH2263/go-durable-engine/internal/sstable"
	"github.com/LYH2263/go-durable-engine/internal/wal"
)

func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalid
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	seq, err := db.wal.AppendPut(key, value)
	if err != nil {
		return err
	}
	db.mem.Put(key, value, seq)
	if db.mem.ShouldFlush() {
		return db.flushLocked()
	}
	return nil
}
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrInvalid
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	seq, err := db.wal.AppendDelete(key)
	if err != nil {
		return err
	}
	db.mem.Delete(key, seq)
	if db.mem.ShouldFlush() {
		return db.flushLocked()
	}
	return nil
}
func NewWriteBatch() *WriteBatch { return &WriteBatch{} }
func (b *WriteBatch) Put(key, value []byte) {
	b.entries = append(b.entries, wal.BatchEntry{
		Key: byteutil.Clone(key), Value: byteutil.Clone(value),
	})
}
func (b *WriteBatch) Delete(key []byte) {
	b.entries = append(b.entries, wal.BatchEntry{
		Key: byteutil.Clone(key), Deleted: true,
	})
}
func (b *WriteBatch) Len() int { return len(b.entries) }
func (b *WriteBatch) Clear()   { b.entries = b.entries[:0] }
func (db *DB) Write(batch *WriteBatch) error {
	if batch == nil || len(batch.entries) == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	_, err := db.wal.AppendBatch(batch.entries)
	if err != nil {
		return err
	}
	// Re-read seqs by applying in order with Get from wal nextSeq — AppendBatch
	// already allocated seqs internally; replay into mem by scanning entries with
	// synthetic increasing seqs matching WAL. We reconstruct by asking wal NextSeq
	// after append: last seq = NextSeq()-1, first = last-len+1.
	last := db.wal.NextSeq() - 1
	first := last - uint64(len(batch.entries)) + 1
	for i, e := range batch.entries {
		seq := first + uint64(i)
		if e.Deleted {
			db.mem.Delete(e.Key, seq)
		} else {
			db.mem.Put(e.Key, e.Value, seq)
		}
	}
	if db.mem.ShouldFlush() {
		return db.flushLocked()
	}
	return nil
}
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.wal.Sync()
}
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	return db.flushLocked()
}
func (db *DB) flushLocked() error {
	snap := db.mem.Snapshot()
	if len(snap) == 0 {
		return nil
	}
	name, err := db.manif.AllocFileName()
	if err != nil {
		return err
	}
	path := filepath.Join(db.dir, name)
	entries := make([]sstable.Entry, 0, len(snap))
	var maxSeq uint64
	var minKey, maxKey []byte
	for _, e := range snap {
		entries = append(entries, sstable.Entry{
			Key: e.Key, Value: e.Value, Seq: e.Seq, Deleted: e.Deleted,
		})
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
		if minKey == nil || byteutil.Compare(e.Key, minKey) < 0 {
			minKey = e.Key
		}
		if maxKey == nil || byteutil.Compare(e.Key, maxKey) > 0 {
			maxKey = e.Key
		}
	}
	meta, err := sstable.WriteFile(path, entries, nil)
	if err != nil {
		return err
	}
	fm := manifest.FileMeta{
		Name: name, Level: 0,
		MinKey: byteutil.Clone(minKey), MaxKey: byteutil.Clone(maxKey),
		MaxSeq: maxSeq, Entries: uint64(len(entries)), FileSize: meta.FileSize,
	}
	// BUG06: skip manifest AddFile companion
	if false {
		if err := db.manif.AddFile(fm, maxSeq); err != nil {
			_ = os.Remove(path)
			return err
		}
	}
	r, err := sstable.Open(path)
	if err != nil {
		return err
	}
	db.tables = append(db.tables, r)
	db.mem.Clear()
	if err := db.wal.Reset(); err != nil {
		return err
	}
	db.wal.SetNextSeq(maxSeq)
	return nil
}
