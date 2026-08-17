package crc32x

import (
	"encoding/binary"
)

type Digest struct {
	crc uint32
	tab *Table
}

func New(tab *Table) *Digest {
	if tab == nil {
		tab = IEEETable()
	}
	return &Digest{crc: 0, tab: tab}
}
func NewIEEE() *Digest       { return New(IEEETable()) }
func NewCastagnoli() *Digest { return New(CastagnoliTable()) }
func (d *Digest) Write(p []byte) (int, error) {
	d.crc = d.tab.Update(d.crc, p)
	return len(p), nil
}
func (d *Digest) Sum(b []byte) []byte {
	s := d.Sum32()
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], s)
	return append(b, tmp[:]...)
}
func (d *Digest) Reset()         { d.crc = 0 }
func (d *Digest) Size() int      { return 4 }
func (d *Digest) BlockSize() int { return 1 }
func (d *Digest) Sum32() uint32  { return d.crc }
func (d *Digest) Sum32LE() [4]byte {
	var out [4]byte
	binary.LittleEndian.PutUint32(out[:], d.crc)
	return out
}
func (d *Digest) Sum32BE() [4]byte {
	var out [4]byte
	binary.BigEndian.PutUint32(out[:], d.crc)
	return out
}

type Rolling struct {
	window []byte
	size   int
	pos    int
	filled bool
	tab    *Table
}

func NewRolling(n int, tab *Table) *Rolling {
	if n <= 0 {
		n = 1
	}
	if tab == nil {
		tab = CastagnoliTable()
	}
	return &Rolling{window: make([]byte, n), size: n, tab: tab}
}
func (r *Rolling) Push(c byte) uint32 {
	r.window[r.pos] = c
	r.pos = (r.pos + 1) % r.size
	if r.pos == 0 {
		r.filled = true
	}
	if !r.filled {
		return r.tab.Checksum(r.window[:r.pos])
	}
	// Reorder to logical order: oldest at pos.
	ordered := make([]byte, r.size)
	copy(ordered, r.window[r.pos:])
	copy(ordered[r.size-r.pos:], r.window[:r.pos])
	return r.tab.Checksum(ordered)
}
func (r *Rolling) Reset() {
	r.pos = 0
	r.filled = false
	for i := range r.window {
		r.window[i] = 0
	}
}
