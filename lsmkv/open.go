package lsmkv

import (
	"context"
	"os"
	"path/filepath"

	"github.com/LYH2263/go-durable-engine/internal/filelock"
	"github.com/LYH2263/go-durable-engine/internal/manifest"
	"github.com/LYH2263/go-durable-engine/internal/memtable"
	"github.com/LYH2263/go-durable-engine/internal/sstable"
	"github.com/LYH2263/go-durable-engine/internal/wal"
)

func (o *Options) normalize() Options {
	out := Options{
		MemtableBytes:    4 << 20,
		SyncPolicy:       SyncFull,
		SyncEveryN:       32,
		CompactThreshold: 2,
	}
	if o == nil {
		return out
	}
	if o.MemtableBytes > 0 {
		out.MemtableBytes = o.MemtableBytes
	}
	out.SyncPolicy = o.SyncPolicy
	if o.SyncEveryN > 0 {
		out.SyncEveryN = o.SyncEveryN
	}
	if o.CompactThreshold > 0 {
		out.CompactThreshold = o.CompactThreshold
	}
	out.ReadOnly = o.ReadOnly
	return out
}
func (o Options) walOpts() *wal.Options {
	switch o.SyncPolicy {
	case SyncNone:
		return &wal.Options{SyncOnWrite: false, SyncEveryN: 0}
	case SyncBatch:
		return &wal.Options{SyncOnWrite: false, SyncEveryN: o.SyncEveryN}
	default:
		return &wal.Options{SyncOnWrite: true, SyncEveryN: 0}
	}
}
func Open(dir string, opts *Options) (*DB, error) {
	return OpenContext(context.Background(), dir, opts)
}
func OpenContext(ctx context.Context, dir string, opts *Options) (*DB, error) {
	_ = ctx
	o := opts.normalize()
	if dir == "" {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lk, err := filelock.Acquire(dir, nil)
	if err != nil {
		return nil, err
	}
	mf, err := manifest.Open(dir)
	if err != nil {
		_ = lk.Release()
		return nil, err
	}
	w, err := wal.Open(dir, o.walOpts())
	if err != nil {
		_ = lk.Release()
		return nil, err
	}
	// sequence continuity: max(manifest files, wal recovered)
	maxSeq := mf.MaxSeqAmongFiles()
	if mf.LogSeq() > maxSeq {
		maxSeq = mf.LogSeq()
	}
	w.SetNextSeq(maxSeq)

	db := &DB{
		dir:   dir,
		opts:  o,
		lock:  lk,
		wal:   w,
		mem:   memtable.NewMemTable(o.MemtableBytes),
		manif: mf,
	}
	if err := db.openTables(); err != nil {
		_ = w.Close()
		_ = lk.Release()
		return nil, err
	}
	if err := db.replayWALContext(ctx); err != nil {
		db.closeUnlocked()
		return nil, err
	}
	return db, nil
}
func (db *DB) openTables() error {
	files := db.manif.Files()
	tables := make([]*sstable.Reader, 0, len(files))
	for _, f := range files {
		path := filepath.Join(db.dir, f.Name)
		r, err := sstable.Open(path)
		if err != nil {
			for _, t := range tables {
				_ = t.Close()
			}
			return err
		}
		tables = append(tables, r)
	}
	db.tables = tables
	return nil
}
func (db *DB) replayWAL() error {
	return db.replayWALContext(context.Background())
}
func (db *DB) replayWALContext(ctx context.Context) error {
	_ = ctx
	return db.wal.Replay(func(rec wal.Record) error {
		if rec.Deleted {
			db.mem.Delete(rec.Key, rec.Seq)
		} else {
			db.mem.Put(rec.Key, rec.Value, rec.Seq)
		}
		return nil
	})
}
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closeUnlocked()
}
func (db *DB) closeUnlocked() error {
	if db.closed {
		return nil
	}
	db.closed = true
	var first error
	if !db.opts.ReadOnly && db.mem.Len() > 0 {
		if err := db.flushLocked(); err != nil && first == nil {
			first = err
		}
	}
	if db.wal != nil {
		if err := db.wal.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, t := range db.tables {
		if err := t.Close(); err != nil && first == nil {
			first = err
		}
	}
	if db.lock != nil {
		if err := db.lock.Release(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
