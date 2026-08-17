// Package memtable implements an in-memory skiplist memtable for LSM writes.
package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
)

const (
	maxLevel     = 16
	pNumerator   = 1
	pDenominator = 4
)

// Entry is a key/value with sequence and tombstone flag.
type Entry struct {
	Key     []byte
	Value   []byte
	Seq     uint64
	Deleted bool
}
type node struct {
	entry Entry
	next  []*node
}
type SkipList struct {
	mu    sync.RWMutex
	head  *node
	level int
	size  int
	bytes int64
	rnd   *rand.Rand
	rndMu sync.Mutex
}

func New() *SkipList {
	h := &node{next: make([]*node, maxLevel)}
	return &SkipList{
		head:  h,
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
func (s *SkipList) randomLevel() int {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	lvl := 1
	for lvl < maxLevel && s.rnd.Intn(pDenominator) < pNumerator {
		lvl++
	}
	return lvl
}
func compareKey(a, b []byte) int {
	return bytes.Compare(a, b)
}
func (s *SkipList) Put(key, value []byte, seq uint64) {
	s.set(key, value, seq, false)
}
func (s *SkipList) Delete(key []byte, seq uint64) {
	s.set(key, nil, seq, true)
}
func (s *SkipList) set(key, value []byte, seq uint64, deleted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*node, maxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareKey(x.next[i].entry.Key, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}
	x = x.next[0]
	if x != nil && compareKey(x.entry.Key, key) == 0 {
		// replace if newer seq
		if seq >= x.entry.Seq {
			oldBytes := estimateBytes(x.entry)
			x.entry.Value = byteutil.Clone(value)
			x.entry.Seq = seq
			x.entry.Deleted = deleted
			s.bytes += estimateBytes(x.entry) - oldBytes
		}
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	n := &node{
		entry: Entry{
			Key:     byteutil.Clone(key),
			Value:   byteutil.Clone(value),
			Seq:     seq,
			Deleted: deleted,
		},
		next: make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	s.size++
	s.bytes += estimateBytes(n.entry)
}
func estimateBytes(e Entry) int64 {
	return int64(len(e.Key) + len(e.Value) + 16)
}
func (s *SkipList) Get(key []byte) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareKey(x.next[i].entry.Key, key) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && compareKey(x.entry.Key, key) == 0 {
		e := x.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		return e, true
	}
	return Entry{}, false
}
func (s *SkipList) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}
func (s *SkipList) ApproxBytes() int64 {
	return atomic.LoadInt64(&s.bytes)
}
func (s *SkipList) Bytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytes
}
func (s *SkipList) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.head = &node{next: make([]*node, maxLevel)}
	s.level = 1
	s.size = 0
	s.bytes = 0
}

type Iterator struct {
	s   *SkipList
	cur *node
}

func (s *SkipList) NewIterator() *Iterator {
	s.mu.RLock()
	// hold RLock for the lifetime of iteration via Snapshot approach:
	// copy entries for safety instead of holding lock.
	s.mu.RUnlock()
	return &Iterator{s: s, cur: s.head}
}
func (it *Iterator) SeekToFirst() {
	it.s.mu.RLock()
	defer it.s.mu.RUnlock()
	it.cur = it.s.head.next[0]
}
func (it *Iterator) Seek(target []byte) {
	it.s.mu.RLock()
	defer it.s.mu.RUnlock()
	x := it.s.head
	for i := it.s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareKey(x.next[i].entry.Key, target) < 0 {
			x = x.next[i]
		}
	}
	it.cur = x.next[0]
}
func (it *Iterator) Valid() bool {
	return it.cur != nil
}
func (it *Iterator) Next() {
	if it.cur == nil {
		return
	}
	it.s.mu.RLock()
	defer it.s.mu.RUnlock()
	it.cur = it.cur.next[0]
}
func (it *Iterator) Key() []byte {
	if it.cur == nil {
		return nil
	}
	return byteutil.Clone(it.cur.entry.Key)
}
func (it *Iterator) Value() []byte {
	if it.cur == nil {
		return nil
	}
	return byteutil.Clone(it.cur.entry.Value)
}
func (it *Iterator) Seq() uint64 {
	if it.cur == nil {
		return 0
	}
	return it.cur.entry.Seq
}
func (it *Iterator) Deleted() bool {
	if it.cur == nil {
		return false
	}
	return it.cur.entry.Deleted
}
func (it *Iterator) Entry() Entry {
	if it.cur == nil {
		return Entry{}
	}
	e := it.cur.entry
	e.Key = byteutil.Clone(e.Key)
	e.Value = byteutil.Clone(e.Value)
	return e
}
func (s *SkipList) Snapshot() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Entry, 0, s.size)
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		e := n.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		out = append(out, e)
	}
	return out
}
func (s *SkipList) Ascend(fn func(Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		e := n.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		if !fn(e) {
			return
		}
	}
}
func (s *SkipList) AscendRange(lo, hi []byte, fn func(Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x := s.head
	if lo != nil {
		for i := s.level - 1; i >= 0; i-- {
			for x.next[i] != nil && compareKey(x.next[i].entry.Key, lo) < 0 {
				x = x.next[i]
			}
		}
	}
	for n := x.next[0]; n != nil; n = n.next[0] {
		if hi != nil && compareKey(n.entry.Key, hi) >= 0 {
			return
		}
		e := n.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		if !fn(e) {
			return
		}
	}
}
func (s *SkipList) MaxSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var max uint64
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		if n.entry.Seq > max {
			max = n.entry.Seq
		}
	}
	return max
}
