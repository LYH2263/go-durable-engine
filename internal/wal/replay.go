package wal

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/LYH2263/go-durable-engine/internal/crc32x"
	"github.com/LYH2263/go-durable-engine/internal/varintx"
)

func (l *Log) recoverTail() error {
	if l.size == 0 {
		return nil
	}
	validEnd, maxSeq, err := scanValidPrefix(l.f, l.size)
	if err != nil {
		return err
	}
	l.nextSeq = maxSeq
	if validEnd < l.size {
		// Bug02: a torn/short tail (partial header, truncated payload, or a
		// CRC mismatch from trailing garbage) must be truncated to the last
		// valid frame so Open continues. Aborting here would discard every
		// record appended after the corruption on the next reopen.
		if TornTailAction() != "truncate" {
			return ErrCorrupt
		}
		if err := l.f.Truncate(validEnd); err != nil {
			return err
		}
		if _, err := l.f.Seek(validEnd, io.SeekStart); err != nil {
			return err
		}
		l.size = validEnd
	} else {
		if _, err := l.f.Seek(0, io.SeekEnd); err != nil {
			return err
		}
	}
	return nil
}
func scanValidPrefix(f *os.File, size int64) (validEnd int64, maxSeq uint64, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	var off int64
	buf := make([]byte, headerSize)
	for off+headerSize <= size {
		if _, err := f.ReadAt(buf, off); err != nil {
			return off, maxSeq, nil // treat as torn
		}
		plen := binary.LittleEndian.Uint32(buf[0:4])
		typ := buf[4]
		wantCRC := binary.LittleEndian.Uint32(buf[5:9])
		if typ != TypePut && typ != TypeDelete && typ != TypeBatch {
			return off, maxSeq, nil
		}
		frameEnd := off + headerSize + int64(plen)
		if frameEnd > size {
			return off, maxSeq, nil // truncated payload
		}
		payload := make([]byte, plen)
		if plen > 0 {
			if _, err := f.ReadAt(payload, off+headerSize); err != nil {
				return off, maxSeq, nil
			}
		}
		got := crc32x.FrameChecksum(typ, payload)
		if got != wantCRC {
			return off, maxSeq, nil
		}
		// parse seq from payload for nextSeq tracking
		seq, ok := peekSeq(typ, payload)
		if ok && seq > maxSeq {
			maxSeq = seq
		}
		off = frameEnd
	}
	if off < size {
		// trailing partial header
		return off, maxSeq, nil
	}
	return off, maxSeq, nil
}
func peekSeq(typ byte, payload []byte) (uint64, bool) {
	switch typ {
	case TypePut, TypeDelete:
		seq, _, err := varintx.DecodeUint64(payload)
		if err != nil {
			return 0, false
		}
		return seq, true
	case TypeBatch:
		n, off, err := varintx.DecodeUint64(payload)
		if err != nil {
			return 0, false
		}
		var max uint64
		for i := uint64(0); i < n; i++ {
			seq, noff, err := varintx.DecodeUint64(payload[off:])
			if err != nil {
				return 0, false
			}
			off += noff
			if seq > max {
				max = seq
			}
			// skip type
			if off >= len(payload) {
				return 0, false
			}
			off++
			// skip key
			klen, noff, err := varintx.DecodeUint64(payload[off:])
			if err != nil {
				return 0, false
			}
			off += noff + int(klen)
			// skip value
			vlen, noff, err := varintx.DecodeUint64(payload[off:])
			if err != nil {
				return 0, false
			}
			off += noff + int(vlen)
		}
		return max, true
	default:
		return 0, false
	}
}
func (l *Log) Replay(fn func(Record) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	size := l.size
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var off int64
	hdr := make([]byte, headerSize)
	for off+headerSize <= size {
		if _, err := l.f.ReadAt(hdr, off); err != nil {
			break
		}
		plen := binary.LittleEndian.Uint32(hdr[0:4])
		typ := hdr[4]
		want := binary.LittleEndian.Uint32(hdr[5:9])
		frameEnd := off + headerSize + int64(plen)
		if frameEnd > size {
			break
		}
		payload := make([]byte, plen)
		if plen > 0 {
			if _, err := l.f.ReadAt(payload, off+headerSize); err != nil {
				break
			}
		}
		if crc32x.FrameChecksum(typ, payload) != want {
			break
		}
		recs, err := decodeFrame(typ, payload)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if err := fn(r); err != nil {
				return err
			}
		}
		off = frameEnd
	}
	// restore file offset to end for appends
	_, err := l.f.Seek(l.size, io.SeekStart)
	return err
}
func decodeFrame(typ byte, payload []byte) ([]Record, error) {
	switch typ {
	case TypePut, TypeDelete:
		r, err := decodeOne(typ, payload)
		if err != nil {
			return nil, err
		}
		return []Record{r}, nil
	case TypeBatch:
		return decodeBatch(payload)
	default:
		return nil, ErrBadType
	}
}
func decodeOne(typ byte, payload []byte) (Record, error) {
	seq, off, err := varintx.DecodeUint64(payload)
	if err != nil {
		return Record{}, ErrCorrupt
	}
	klen, noff, err := varintx.DecodeUint64(payload[off:])
	if err != nil {
		return Record{}, ErrCorrupt
	}
	off += noff
	if off+int(klen) > len(payload) {
		return Record{}, ErrCorrupt
	}
	key := append([]byte(nil), payload[off:off+int(klen)]...)
	off += int(klen)
	vlen, noff, err := varintx.DecodeUint64(payload[off:])
	if err != nil {
		return Record{}, ErrCorrupt
	}
	off += noff
	if off+int(vlen) > len(payload) {
		return Record{}, ErrCorrupt
	}
	val := append([]byte(nil), payload[off:off+int(vlen)]...)
	return Record{
		Type:    typ,
		Key:     key,
		Value:   val,
		Seq:     seq,
		Deleted: typ == TypeDelete,
	}, nil
}
func decodeBatch(payload []byte) ([]Record, error) {
	n, off, err := varintx.DecodeUint64(payload)
	if err != nil {
		return nil, ErrCorrupt
	}
	out := make([]Record, 0, n)
	for i := uint64(0); i < n; i++ {
		seq, noff, err := varintx.DecodeUint64(payload[off:])
		if err != nil {
			return nil, ErrCorrupt
		}
		off += noff
		if off >= len(payload) {
			return nil, ErrCorrupt
		}
		typ := payload[off]
		off++
		klen, noff, err := varintx.DecodeUint64(payload[off:])
		if err != nil {
			return nil, ErrCorrupt
		}
		off += noff
		if off+int(klen) > len(payload) {
			return nil, ErrCorrupt
		}
		key := append([]byte(nil), payload[off:off+int(klen)]...)
		off += int(klen)
		vlen, noff, err := varintx.DecodeUint64(payload[off:])
		if err != nil {
			return nil, ErrCorrupt
		}
		off += noff
		if off+int(vlen) > len(payload) {
			return nil, ErrCorrupt
		}
		val := append([]byte(nil), payload[off:off+int(vlen)]...)
		off += int(vlen)
		out = append(out, Record{
			Type:    typ,
			Key:     key,
			Value:   val,
			Seq:     seq,
			Deleted: typ == TypeDelete,
		})
	}
	return out, nil
}
