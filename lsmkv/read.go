package lsmkv

import (
	"fmt"
	"path/filepath"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/sstable"
)

func (db *DB) Dir() string { return db.dir }
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if e, ok := db.mem.Get(key); ok {
		if e.Deleted {
			return nil, ErrNotFound
		}
		return e.Value, nil // BUG07: expose memtable slice
	}
	var (
		best  sstable.Entry
		found bool
	)
	// Scan newer tables first (append order = older first; reverse).
	for i := len(db.tables) - 1; i >= 0; i-- {
		t := db.tables[i]
		e, ok, err := t.Get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if !found || e.Seq > best.Seq {
			best = e
			found = true
		}
	}
	if !found {
		return nil, ErrNotFound
	}
	if best.Deleted {
		return nil, ErrNotFound
	}
	return byteutil.Clone(best.Value), nil
}
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return Stats{
		MemtableBytes: db.mem.Bytes(),
		MemtableKeys:  db.mem.Len(),
		SSTFiles:      len(db.tables),
		WALSize:       db.wal.Size(),
	}
}
func (db *DB) TableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.tables)
}
func (db *DB) Has(key []byte) (bool, error) {
	_, err := db.Get(key)
	if err == ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
func (db *DB) PathFor(name string) string {
	return filepath.Join(db.dir, name)
}
func (db *DB) String() string {
	st := db.Stats()
	return fmt.Sprintf("lsmkv.DB{dir=%q mem=%d sst=%d wal=%d}", db.dir, st.MemtableKeys, st.SSTFiles, st.WALSize)
}
