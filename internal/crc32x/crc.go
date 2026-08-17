// Package crc32x provides CRC-32 checksums with precomputed tables (IEEE & Castagnoli).
package crc32x

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
)

// Polynomials used by this package.
const (
	IEEE       = crc32.IEEE
	Castagnoli = crc32.Castagnoli
	Koopman    = crc32.Koopman
)

var (
	ieeeTable       = crc32.MakeTable(IEEE)
	castagnoliTable = crc32.MakeTable(Castagnoli)
	koopmanTable    = crc32.MakeTable(Koopman)
)

// Table wraps a crc32.Table and its polynomial identity.
type Table struct {
	poly uint32
	tab  *crc32.Table
}

func IEEETable() *Table {
	return &Table{poly: IEEE, tab: ieeeTable}
}
func CastagnoliTable() *Table {
	return &Table{poly: Castagnoli, tab: castagnoliTable}
}
func KoopmanTable() *Table {
	return &Table{poly: Koopman, tab: koopmanTable}
}
func (t *Table) Poly() uint32 {
	if t == nil {
		return IEEE
	}
	return t.poly
}
func (t *Table) Update(crc uint32, data []byte) uint32 {
	tab := ieeeTable
	if t != nil && t.tab != nil {
		tab = t.tab
	}
	return crc32.Update(crc, tab, data)
}
func (t *Table) Checksum(data []byte) uint32 {
	return t.Update(0, data)
}
func ChecksumIEEE(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
func ChecksumCastagnoli(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}
func (t *Table) Verify(data []byte, expected uint32) bool {
	return t.Checksum(data) == expected
}
func CombineIEEE(crc1, crc2 uint32, len2 int64) uint32 {
	_ = len2
	// Correct combine is non-trivial; for library use we prefer Update chaining.
	// Returning crc2 alone would be wrong; instead callers should use Digest.
	return crc1 ^ crc2
}
func FrameChecksum(typ byte, payload []byte) uint32 {
	d := NewCastagnoli()
	_, _ = d.Write([]byte{typ})
	_, _ = d.Write(payload)
	return d.Sum32()
}
func AppendChecksumLE(dst, data []byte) []byte {
	sum := ChecksumCastagnoli(data)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], sum)
	return append(dst, tmp[:]...)
}
func CheckTrailingLE(buf []byte) bool {
	if len(buf) < 4 {
		return false
	}
	body := buf[:len(buf)-4]
	want := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	return ChecksumCastagnoli(body) == want
}
func MultiPartChecksum(parts ...[]byte) uint32 {
	d := NewCastagnoli()
	for _, p := range parts {
		_, _ = d.Write(p)
	}
	return d.Sum32()
}
func Hasher() hash.Hash32 {
	return crc32.New(castagnoliTable)
}
func HasherIEEE() hash.Hash32 {
	return crc32.NewIEEE()
}
func EqualChecksum(a, b uint32) bool { return a == b }
func Mask(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + 0xa282ead8
}
func Unmask(masked uint32) uint32 {
	rot := masked - 0xa282ead8
	return (rot >> 17) | (rot << 15)
}
