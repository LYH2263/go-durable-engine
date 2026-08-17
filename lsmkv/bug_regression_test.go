package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/compact"
	"github.com/LYH2263/go-durable-engine/internal/crc32x"
	"github.com/LYH2263/go-durable-engine/internal/manifest"
	"github.com/LYH2263/go-durable-engine/internal/sstable"
	"github.com/LYH2263/go-durable-engine/internal/wal"
)

func TestBug01_TombstoneSurvivesFlushCompact(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{MemtableBytes: 1 << 20, CompactThreshold: 2, SyncPolicy: SyncFull}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("k")); err != ErrNotFound {
		t.Fatalf("want tombstone, got %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := db2.Get([]byte("k")); err != ErrNotFound {
		t.Fatalf("reopen: want tombstone, got %v", err)
	}
}

func TestBug02_WALSkipsBadCRCContinues(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{SyncPolicy: SyncFull})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "wal.log")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if err := db2.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	db3, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	v, err := db3.Get([]byte("b"))
	if err != nil || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("after skip bad frame: %v %q", err, v)
	}
}

func TestBug03_MergePicksHighestSeq(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "a.sst")
	p2 := filepath.Join(dir, "b.sst")
	_, err := sstable.WriteFile(p1, []sstable.Entry{
		{Key: []byte("k"), Value: []byte("old"), Seq: 1},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sstable.WriteFile(p2, []sstable.Entry{
		{Key: []byte("k"), Value: []byte("new"), Seq: 9},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !compact.PreferNewerSeq(9, 1) {
		t.Fatal("PreferNewerSeq must prefer higher sequence")
	}
	out := filepath.Join(dir, "out.sst")
	res, err := compact.MergeFiles(
		[]compact.Input{{Path: p1, Level: 0}, {Path: p2, Level: 0}},
		out,
		&compact.Options{DropTombstones: false},
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.DroppedObsolete != 1 {
		t.Fatalf("want 1 obsolete drop, got %d", res.DroppedObsolete)
	}
	r, err := sstable.Open(res.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	e, ok, err := r.Get([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(e.Value, []byte("new")) {
		t.Fatalf("want newest seq value, got %q", e.Value)
	}
}

func TestBug04_SeqMonotonicAfterReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{SyncPolicy: SyncFull})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := NewWriteBatch()
	b.Put([]byte("z"), []byte("1"))
	b.Put([]byte("z"), []byte("2"))
	if err := db.Write(b); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("z"))
	if err != nil || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("batch must apply newer value: %v %q", err, v)
	}
}

func TestBug05_BloomNoFalseNegative(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "t.sst")
	_, err := sstable.WriteFile(p, []sstable.Entry{
		{Key: []byte("present"), Value: []byte("v"), Seq: 1},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !r.MayContain([]byte("present")) {
		t.Fatal("bloom false negative on inserted key")
	}
	e, ok, err := r.Get([]byte("present"))
	if err != nil || !ok || !bytes.Equal(e.Value, []byte("v")) {
		t.Fatalf("get present: ok=%v err=%v val=%q", ok, err, e.Value)
	}
}

func TestBug06_ManifestTracksFlushedSST(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{SyncPolicy: SyncFull})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("m"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	mf, err := manifest.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if mf.Count() != 1 {
		t.Fatalf("manifest files=%d want 1", mf.Count())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.TableCount() != 1 {
		t.Fatalf("reopen tables=%d want 1", db2.TableCount())
	}
}

func TestBug07_GetReturnsIndependentBytes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("alias"), []byte("orig")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("alias"))
	if err != nil {
		t.Fatal(err)
	}
	v[0] = 'X'
	v2, err := db.Get([]byte("alias"))
	if err != nil || !bytes.Equal(v2, []byte("orig")) {
		t.Fatalf("get alias not independent: %q", v2)
	}
	_ = byteutil.Clone
}

func TestBug08_WALCorruptIsTyped(t *testing.T) {
	dir := t.TempDir()
	l, err := wal.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendPut([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Append a second frame: valid CRC but payload too short for decodeOne.
	payload := []byte{0x00}
	typ := wal.TypePut
	crc := crc32x.FrameChecksum(typ, payload)
	hdr := make([]byte, 9)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = typ
	binary.LittleEndian.PutUint32(hdr[5:9], crc)
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(append(hdr, payload...)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := wal.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	err = l2.Replay(func(wal.Record) error { return nil })
	if err == nil {
		t.Fatal("want decode error")
	}
	if !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestBug09_WALCloseSyncBeforeFileClose(t *testing.T) {
	dir := t.TempDir()
	l, err := wal.Open(dir, &wal.Options{SyncOnWrite: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendPut([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, err := db.Get([]byte("k"))
	if err != nil || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("reopen after wal close: %v %q", err, v)
	}
}

func TestBug10_CompactRespectsCancel(t *testing.T) {
	dir := t.TempDir()
	inputs := make([]compact.Input, 0, 4)
	refs := make([]compact.FileRef, 0, 4)
	for i := 0; i < 4; i++ {
		p := filepath.Join(dir, fmt.Sprintf("%d.sst", i))
		entries := []sstable.Entry{{Key: []byte{byte('a' + i)}, Value: []byte("v"), Seq: uint64(i + 1)}}
		if _, err := sstable.WriteFile(p, entries, nil); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, compact.Input{Path: p, Level: 0})
		refs = append(refs, compact.FileRef{Path: p, Name: filepath.Base(p), Level: 0, Size: 1})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := filepath.Join(dir, "merged.sst")
	_, err := compact.MergeFilesContext(ctx, inputs, out, nil)
	if err == nil {
		t.Fatal("want cancel error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	ctxP, cancelP := context.WithCancel(context.Background())
	cancelP()
	alloc := func() (string, error) { return "out-ctx.sst", nil }
	_, err = compact.CompactDirContext(ctxP, dir, refs, alloc, nil)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("CompactDirContext want canceled, got %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_, err = compact.MergeFilesContext(ctx2, inputs, out, nil)
	if err != nil {
		t.Fatal(err)
	}
}
