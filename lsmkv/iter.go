package lsmkv

import (
	"bytes"
	"sort"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
)

func (db *DB) NewIterator() *Iterator {
	db.mu.RLock()
	items := db.collectMerged()
	db.mu.RUnlock()
	return &Iterator{db: db, items: items, idx: -1}
}
func (db *DB) collectMerged() []iterItem {
	type cand struct {
		key     []byte
		value   []byte
		seq     uint64
		deleted bool
	}
	m := map[string]cand{}
	update := func(k, v []byte, seq uint64, del bool) {
		sk := string(k)
		if cur, ok := m[sk]; ok && cur.seq >= seq {
			return
		}
		m[sk] = cand{key: byteutil.Clone(k), value: byteutil.Clone(v), seq: seq, deleted: del}
	}
	for _, e := range db.mem.Snapshot() {
		update(e.Key, e.Value, e.Seq, e.Deleted)
	}
	for _, t := range db.tables {
		it := t.NewIterator()
		for it.Valid() {
			update(it.Key(), it.Value(), it.Seq(), it.Deleted())
			it.Next()
		}
	}
	keys := make([]string, 0, len(m))
	for k, c := range m {
		if c.deleted {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]iterItem, 0, len(keys))
	for _, k := range keys {
		c := m[k]
		out = append(out, iterItem{key: c.key, value: c.value, seq: c.seq, deleted: false})
	}
	return out
}
func (it *Iterator) SeekToFirst() {
	if len(it.items) == 0 {
		it.idx = -1
		return
	}
	it.idx = 0
}
func (it *Iterator) Seek(target []byte) {
	it.idx = sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].key, target) >= 0
	})
	if it.idx >= len(it.items) {
		it.idx = -1
	}
}
func (it *Iterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.items)
}
func (it *Iterator) Next() {
	if it.idx < 0 {
		return
	}
	it.idx++
	if it.idx >= len(it.items) {
		it.idx = -1
	}
}
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.items[it.idx].key)
}
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.items[it.idx].value)
}
func (it *Iterator) Close() {
	if it.release != nil {
		it.release()
		it.release = nil
	}
}
