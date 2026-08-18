package lsmkv

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/compact"
	"github.com/LYH2263/go-durable-engine/internal/manifest"
	"github.com/LYH2263/go-durable-engine/internal/sstable"
)

func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	if len(db.tables) < db.opts.CompactThreshold {
		return nil
	}
	refs := make([]compact.FileRef, 0, len(db.tables))
	files := db.manif.Files()
	nameByPath := map[string]manifest.FileMeta{}
	for _, f := range files {
		nameByPath[filepath.Join(db.dir, f.Name)] = f
	}
	for _, t := range db.tables {
		fm := nameByPath[t.Path()]
		refs = append(refs, compact.FileRef{
			Path: t.Path(), Name: fm.Name, Level: fm.Level, Size: t.FileSize(),
		})
	}
	for _, t := range db.tables {
		_ = t.Close()
	}
	alloc := func() (string, error) { return db.manif.AllocFileName() }
	res, err := compact.CompactDir(db.dir, refs, alloc, &compact.Options{DropTombstones: false})
	if err != nil {
		if errors.Is(err, compact.ErrNoInputs) {
			return nil
		}
		return err
	}
	// Close and remove old tables that were merged
	removeSet := map[string]struct{}{}
	for _, p := range res.Removed {
		removeSet[p] = struct{}{}
	}
	kept := make([]*sstable.Reader, 0, len(db.tables))
	var removeNames []string
	for _, t := range db.tables {
		if _, ok := removeSet[t.Path()]; ok {
			removeNames = append(removeNames, filepath.Base(t.Path()))
			_ = t.Close()
			continue
		}
		kept = append(kept, t)
	}
	outReader, err := sstable.Open(res.OutputPath)
	if err != nil {
		return err
	}
	kept = append(kept, outReader)
	add := []manifest.FileMeta{{
		Name: filepath.Base(res.OutputPath), Level: 1,
		MinKey: byteutil.Clone(res.Meta.MinKey), MaxKey: byteutil.Clone(res.Meta.MaxKey),
		MaxSeq: res.Meta.MaxSeq, Entries: res.Meta.Entries, FileSize: res.Meta.FileSize,
	}}
	if err := db.manif.Replace(removeNames, add, res.Meta.MaxSeq); err != nil {
		return err
	}
	for p := range removeSet {
		_ = os.Remove(p)
	}
	db.tables = kept
	return nil
}
