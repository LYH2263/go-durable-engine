package sstable

import (
	"sort"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
)

func (r *Reader) NewIterator() *Iterator {
	it := &Iterator{r: r, bi: -1}
	it.Next()
	return it
}
func (it *Iterator) Valid() bool {
	return it.err == nil && it.blockIt != nil && it.blockIt.Valid()
}
func (it *Iterator) Next() {
	if it.err != nil {
		return
	}
	if it.blockIt != nil {
		it.blockIt.Next()
		if it.blockIt.Valid() {
			return
		}
	}
	it.bi++
	for it.bi < len(it.r.index) {
		block, err := it.r.readBlock(it.r.index[it.bi])
		if err != nil {
			it.err = err
			return
		}
		bit, err := newBlockIter(block)
		if err != nil {
			it.err = err
			return
		}
		it.blockIt = bit
		if bit.Valid() {
			return
		}
		it.bi++
	}
	it.blockIt = nil
}
func (it *Iterator) Seek(target []byte) {
	it.err = nil
	if len(it.r.index) == 0 {
		it.blockIt = nil
		return
	}
	idx := sort.Search(len(it.r.index), func(i int) bool {
		return byteutil.Compare(it.r.index[i].lastKey, target) >= 0
	})
	if idx >= len(it.r.index) {
		it.bi = len(it.r.index)
		it.blockIt = nil
		return
	}
	it.bi = idx
	block, err := it.r.readBlock(it.r.index[idx])
	if err != nil {
		it.err = err
		return
	}
	bit, err := newBlockIter(block)
	if err != nil {
		it.err = err
		return
	}
	bit.Seek(target)
	it.blockIt = bit
	if !bit.Valid() {
		it.Next()
	}
}
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.blockIt.key)
}
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.blockIt.value)
}
func (it *Iterator) Seq() uint64 {
	if !it.Valid() {
		return 0
	}
	return it.blockIt.seq
}
func (it *Iterator) Deleted() bool {
	if !it.Valid() {
		return false
	}
	return it.blockIt.deleted
}
func (it *Iterator) Err() error { return it.err }
func (it *Iterator) Entry() Entry {
	if !it.Valid() {
		return Entry{}
	}
	return Entry{
		Key:     it.Key(),
		Value:   it.Value(),
		Seq:     it.Seq(),
		Deleted: it.Deleted(),
	}
}
