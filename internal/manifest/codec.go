package manifest

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/crc32x"
)

func decode(data []byte) (State, error) {
	if len(data) >= 4 && binary.LittleEndian.Uint32(data[:4]) == magicBinary {
		return decodeBinary(data)
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return State{}, ErrCorrupt
	}
	if st.Version == 0 {
		st.Version = versionJSON
	}
	return st, nil
}
func decodeBinary(data []byte) (State, error) {
	if len(data) < 4+4+8+8+4 {
		return State{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint32(data[0:4]) != magicBinary {
		return State{}, ErrCorrupt
	}
	ver := binary.LittleEndian.Uint32(data[4:8])
	next := binary.LittleEndian.Uint64(data[8:16])
	logSeq := binary.LittleEndian.Uint64(data[16:24])
	n := binary.LittleEndian.Uint32(data[24:28])
	off := 28
	files := make([]FileMeta, 0, n)
	for i := uint32(0); i < n; i++ {
		if off+2 > len(data) {
			return State{}, ErrCorrupt
		}
		nameLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+nameLen > len(data) {
			return State{}, ErrCorrupt
		}
		name := string(data[off : off+nameLen])
		off += nameLen
		if off+4+8+8+8 > len(data) {
			return State{}, ErrCorrupt
		}
		level := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		maxSeq := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		entries := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		fileSize := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		if off+2 > len(data) {
			return State{}, ErrCorrupt
		}
		mkLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+mkLen > len(data) {
			return State{}, ErrCorrupt
		}
		minKey := byteutil.Clone(data[off : off+mkLen])
		off += mkLen
		if off+2 > len(data) {
			return State{}, ErrCorrupt
		}
		xkLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+xkLen > len(data) {
			return State{}, ErrCorrupt
		}
		maxKey := byteutil.Clone(data[off : off+xkLen])
		off += xkLen
		files = append(files, FileMeta{
			Name: name, Level: level, MinKey: minKey, MaxKey: maxKey,
			MaxSeq: maxSeq, Entries: entries, FileSize: fileSize,
		})
	}
	if off+4 > len(data) {
		return State{}, ErrCorrupt
	}
	want := binary.LittleEndian.Uint32(data[off : off+4])
	if crc32x.ChecksumCastagnoli(data[:off]) != want {
		return State{}, ErrCorrupt
	}
	return State{
		Version:   int(ver),
		NextFile:  next,
		LogSeq:    logSeq,
		Files:     files,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}
func EncodeBinary(st State) []byte {
	var b []byte
	hdr := make([]byte, 28)
	binary.LittleEndian.PutUint32(hdr[0:4], magicBinary)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(st.Version))
	binary.LittleEndian.PutUint64(hdr[8:16], st.NextFile)
	binary.LittleEndian.PutUint64(hdr[16:24], st.LogSeq)
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(st.Files)))
	b = append(b, hdr...)
	for _, f := range st.Files {
		name := []byte(f.Name)
		var tmp [2]byte
		binary.LittleEndian.PutUint16(tmp[:], uint16(len(name)))
		b = append(b, tmp[:]...)
		b = append(b, name...)
		var num [4 + 8 + 8 + 8]byte
		binary.LittleEndian.PutUint32(num[0:4], uint32(f.Level))
		binary.LittleEndian.PutUint64(num[4:12], f.MaxSeq)
		binary.LittleEndian.PutUint64(num[12:20], f.Entries)
		binary.LittleEndian.PutUint64(num[20:28], uint64(f.FileSize))
		b = append(b, num[:]...)
		binary.LittleEndian.PutUint16(tmp[:], uint16(len(f.MinKey)))
		b = append(b, tmp[:]...)
		b = append(b, f.MinKey...)
		binary.LittleEndian.PutUint16(tmp[:], uint16(len(f.MaxKey)))
		b = append(b, tmp[:]...)
		b = append(b, f.MaxKey...)
	}
	sum := crc32x.ChecksumCastagnoli(b)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], sum)
	b = append(b, crc[:]...)
	return b
}
func cloneState(st State) State {
	out := st
	out.Files = make([]FileMeta, len(st.Files))
	for i, f := range st.Files {
		out.Files[i] = FileMeta{
			Name:     f.Name,
			Level:    f.Level,
			MinKey:   byteutil.Clone(f.MinKey),
			MaxKey:   byteutil.Clone(f.MaxKey),
			MaxSeq:   f.MaxSeq,
			Entries:  f.Entries,
			FileSize: f.FileSize,
		}
	}
	return out
}
