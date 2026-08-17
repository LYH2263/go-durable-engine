package sstable

import (
	"encoding/binary"
	"os"
	"sort"

	"github.com/LYH2263/go-durable-engine/internal/bloomx"
	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/crc32x"
	"github.com/LYH2263/go-durable-engine/internal/varintx"
)

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() < 56 {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	var footer [56]byte
	if _, err := f.ReadAt(footer[:], st.Size()-56); err != nil {
		_ = f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint32(footer[0:4]) != magicFooter ||
		binary.LittleEndian.Uint32(footer[52:56]) != magicFooter {
		_ = f.Close()
		return nil, ErrBadMagic
	}
	indexOff := int64(binary.LittleEndian.Uint64(footer[4:12]))
	indexLen := int64(binary.LittleEndian.Uint64(footer[12:20]))
	bloomOff := int64(binary.LittleEndian.Uint64(footer[20:28]))
	bloomLen := int64(binary.LittleEndian.Uint64(footer[28:36]))
	maxSeq := binary.LittleEndian.Uint64(footer[36:44])
	entries := binary.LittleEndian.Uint64(footer[44:52])

	bloomData := make([]byte, bloomLen)
	if bloomLen > 0 {
		if _, err := f.ReadAt(bloomData, bloomOff); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	bf, err := bloomx.UnmarshalBinary(bloomData)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	indexData := make([]byte, indexLen)
	if _, err := f.ReadAt(indexData, indexOff); err != nil {
		_ = f.Close()
		return nil, err
	}
	if len(indexData) < 4 {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	body := indexData[:len(indexData)-4]
	want := binary.LittleEndian.Uint32(indexData[len(indexData)-4:])
	if crc32x.ChecksumCastagnoli(body) != want {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	n, off, err := varintx.DecodeUint64(body)
	if err != nil {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	index := make([]indexRec, 0, n)
	for i := uint64(0); i < n; i++ {
		klen, noff, err := varintx.DecodeUint64(body[off:])
		if err != nil {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		off += noff
		if off+int(klen) > len(body) {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		key := byteutil.Clone(body[off : off+int(klen)])
		off += int(klen)
		boff, noff, err := varintx.DecodeUint64(body[off:])
		if err != nil {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		off += noff
		blen, noff, err := varintx.DecodeUint64(body[off:])
		if err != nil {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		off += noff
		index = append(index, indexRec{lastKey: key, offset: boff, length: blen})
	}
	r := &Reader{
		f:        f,
		path:     path,
		index:    index,
		bloom:    bf,
		maxSeq:   maxSeq,
		entries:  entries,
		fileSize: st.Size(),
	}
	if len(index) > 0 {
		r.maxKey = byteutil.Clone(index[len(index)-1].lastKey)
		// min key approximated by first key of first block — load lazily on demand
	}
	return r, nil
}
func (r *Reader) Path() string    { return r.path }
func (r *Reader) MaxSeq() uint64  { return r.maxSeq }
func (r *Reader) Entries() uint64 { return r.entries }
func (r *Reader) FileSize() int64 { return r.fileSize }
func (r *Reader) MayContain(key []byte) bool {
	return r.bloom.MayContain(key)
}
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}
func (r *Reader) readBlock(rec indexRec) ([]byte, error) {
	buf := make([]byte, rec.length)
	if _, err := r.f.ReadAt(buf, int64(rec.offset)); err != nil {
		return nil, err
	}
	if len(buf) < 8 {
		return nil, ErrCorrupt
	}
	body := buf[:len(buf)-4]
	want := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if crc32x.ChecksumCastagnoli(body) != want {
		return nil, ErrCorrupt
	}
	return body, nil
}
func parseRestarts(block []byte) (data []byte, restarts []uint32, err error) {
	if len(block) < 4 {
		return nil, nil, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(block[len(block)-4:])
	need := int(n)*4 + 4
	if len(block) < need {
		return nil, nil, ErrCorrupt
	}
	restartOff := len(block) - need
	data = block[:restartOff]
	restarts = make([]uint32, n)
	for i := 0; i < int(n); i++ {
		restarts[i] = binary.LittleEndian.Uint32(block[restartOff+i*4 : restartOff+i*4+4])
	}
	return data, restarts, nil
}
func newBlockIter(block []byte) (*blockIter, error) {
	data, restarts, err := parseRestarts(block)
	if err != nil {
		return nil, err
	}
	it := &blockIter{data: data, restarts: restarts}
	it.Next()
	return it, nil
}
func (it *blockIter) Next() {
	if it.off >= len(it.data) {
		it.valid = false
		return
	}
	shared, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	nonShared, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	vlen, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	seq, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	if it.off >= len(it.data) {
		it.valid = false
		return
	}
	flag := it.data[it.off]
	it.off++
	if it.off+int(nonShared)+int(vlen) > len(it.data) {
		it.valid = false
		return
	}
	key := make([]byte, int(shared)+int(nonShared))
	copy(key, it.key[:shared])
	copy(key[shared:], it.data[it.off:it.off+int(nonShared)])
	it.off += int(nonShared)
	val := byteutil.Clone(it.data[it.off : it.off+int(vlen)])
	it.off += int(vlen)
	it.key = key
	it.value = val
	it.seq = seq
	it.deleted = flag == flagDelete
	it.valid = true
}
func (it *blockIter) Valid() bool { return it.valid }
func (it *blockIter) Seek(target []byte) {
	// binary search restarts
	n := len(it.restarts)
	if n == 0 {
		it.off = 0
		it.key = nil
		it.Next()
		for it.valid && byteutil.Compare(it.key, target) < 0 {
			it.Next()
		}
		return
	}
	lo, hi := 0, n-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		it.off = int(it.restarts[mid])
		it.key = nil
		it.Next()
		if !it.valid {
			hi = mid - 1
			continue
		}
		if byteutil.Compare(it.key, target) <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	it.off = int(it.restarts[lo])
	it.key = nil
	it.Next()
	for it.valid && byteutil.Compare(it.key, target) < 0 {
		it.Next()
	}
}
func (r *Reader) Get(key []byte) (Entry, bool, error) {
	if !r.MayContain(key) {
		return Entry{}, false, nil
	}
	if len(r.index) == 0 {
		return Entry{}, false, nil
	}
	// find first block whose lastKey >= key
	idx := sort.Search(len(r.index), func(i int) bool {
		return byteutil.Compare(r.index[i].lastKey, key) >= 0
	})
	if idx >= len(r.index) {
		return Entry{}, false, nil
	}
	block, err := r.readBlock(r.index[idx])
	if err != nil {
		return Entry{}, false, err
	}
	it, err := newBlockIter(block)
	if err != nil {
		return Entry{}, false, err
	}
	it.Seek(key)
	if it.Valid() && byteutil.Equal(it.key, key) {
		return Entry{Key: byteutil.Clone(it.key), Value: byteutil.Clone(it.value), Seq: it.seq, Deleted: it.deleted}, true, nil
	}
	return Entry{}, false, nil
}
