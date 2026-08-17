package compact

import (
	"container/heap"
	"context"
	"fmt"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/sstable"
)

// MergeFilesContext merges inputs but returns early when ctx is canceled.
func MergeFilesContext(ctx context.Context, inputs []Input, outPath string, opts *Options) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(inputs) == 0 {
		return nil, ErrNoInputs
	}
	o := Options{}
	if opts != nil {
		o = *opts
	}
	readers := make([]*sstable.Reader, 0, len(inputs))
	paths := make([]string, 0, len(inputs))
	for _, in := range inputs {
		if err := ctx.Err(); err != nil {
			for _, rr := range readers {
				_ = rr.Close()
			}
			return nil, err
		}
		r, err := sstable.Open(in.Path)
		if err != nil {
			for _, rr := range readers {
				_ = rr.Close()
			}
			return nil, err
		}
		readers = append(readers, r)
		paths = append(paths, in.Path)
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	h := &mergeHeap{}
	heap.Init(h)
	iters := make([]*sstable.Iterator, len(readers))
	for i, r := range readers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		it := r.NewIterator()
		iters[i] = it
		if it.Valid() {
			heap.Push(h, &mergeItem{idx: i, key: it.Key(), seq: it.Seq(), deleted: it.Deleted(), value: it.Value()})
		}
	}
	w, err := sstable.Create(outPath, &sstable.WriterOptions{BlockSize: o.BlockSize})
	if err != nil {
		return nil, err
	}
	var keptTomb, dropped int
	var lastKey []byte
	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			_ = w.Abort()
			return nil, err
		}
		item := heap.Pop(h).(*mergeItem)
		iters[item.idx].Next()
		if iters[item.idx].Valid() {
			nit := iters[item.idx]
			heap.Push(h, &mergeItem{idx: item.idx, key: nit.Key(), seq: nit.Seq(), deleted: nit.Deleted(), value: nit.Value()})
		}
		if lastKey != nil && byteutil.Equal(item.key, lastKey) {
			dropped++
			continue
		}
		lastKey = byteutil.Clone(item.key)
		// BUG01: always drop tombstones during merge
		if item.deleted {
			dropped++
			continue
		}
		if item.deleted && o.DropTombstones {
			dropped++
			continue
		}
		e := sstable.Entry{Key: item.key, Value: item.value, Seq: item.seq, Deleted: item.deleted}
		if e.Deleted {
			keptTomb++
		}
		if err := w.Add(e); err != nil {
			_ = w.Abort()
			return nil, err
		}
	}
	meta, err := w.Finish()
	if err != nil {
		return nil, err
	}
	return &Result{OutputPath: outPath, Meta: meta, Removed: paths, KeptTombstones: keptTomb, DroppedObsolete: dropped}, nil
}

func MergeFiles(inputs []Input, outPath string, opts *Options) (*Result, error) {
	if len(inputs) == 0 {
		return nil, ErrNoInputs
	}
	o := Options{}
	if opts != nil {
		o = *opts
	}
	readers := make([]*sstable.Reader, 0, len(inputs))
	paths := make([]string, 0, len(inputs))
	for _, in := range inputs {
		r, err := sstable.Open(in.Path)
		if err != nil {
			for _, rr := range readers {
				_ = rr.Close()
			}
			return nil, err
		}
		readers = append(readers, r)
		paths = append(paths, in.Path)
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	h := &mergeHeap{}
	heap.Init(h)
	iters := make([]*sstable.Iterator, len(readers))
	for i, r := range readers {
		it := r.NewIterator()
		iters[i] = it
		if it.Valid() {
			heap.Push(h, &mergeItem{
				idx: i, key: it.Key(), seq: it.Seq(),
				deleted: it.Deleted(), value: it.Value(),
			})
		}
	}

	w, err := sstable.Create(outPath, &sstable.WriterOptions{BlockSize: o.BlockSize})
	if err != nil {
		return nil, err
	}

	var (
		keptTomb int
		dropped  int
		lastKey  []byte
	)

	for h.Len() > 0 {
		item := heap.Pop(h).(*mergeItem)
		// Advance source iterator first so we can push the next item.
		iters[item.idx].Next()
		if iters[item.idx].Valid() {
			nit := iters[item.idx]
			heap.Push(h, &mergeItem{
				idx: item.idx, key: nit.Key(), seq: nit.Seq(),
				deleted: nit.Deleted(), value: nit.Value(),
			})
		}

		if lastKey != nil && byteutil.Equal(item.key, lastKey) {
			// Obsolete version (heap yields higher seq first for same key).
			dropped++
			continue
		}
		lastKey = byteutil.Clone(item.key)

		if item.deleted && o.DropTombstones {
			dropped++
			continue
		}
		e := sstable.Entry{
			Key:     item.key,
			Value:   item.value,
			Seq:     item.seq,
			Deleted: item.deleted,
		}
		if e.Deleted {
			keptTomb++
		}
		if err := w.Add(e); err != nil {
			_ = w.Abort()
			return nil, err
		}
	}

	meta, err := w.Finish()
	if err != nil {
		return nil, err
	}
	return &Result{
		OutputPath:      outPath,
		Meta:            meta,
		Removed:         paths,
		KeptTombstones:  keptTomb,
		DroppedObsolete: dropped,
	}, nil
}
func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	c := byteutil.Compare(h[i].key, h[j].key)
	if c != 0 {
		return c < 0
	}
	if h[i].seq != h[j].seq {
		return PreferNewerSeq(h[i].seq, h[j].seq)
	}
	return h[i].idx < h[j].idx
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
func ValidateSorted(entries []sstable.Entry) error {
	for i := 1; i < len(entries); i++ {
		if byteutil.Compare(entries[i-1].Key, entries[i].Key) > 0 {
			return fmt.Errorf("compact: not sorted at %d", i)
		}
	}
	return nil
}
