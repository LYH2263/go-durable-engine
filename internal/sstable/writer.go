package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/LYH2263/go-durable-engine/internal/bloomx"
	"github.com/LYH2263/go-durable-engine/internal/byteutil"
	"github.com/LYH2263/go-durable-engine/internal/crc32x"
	"github.com/LYH2263/go-durable-engine/internal/varintx"
)

func (o *WriterOptions) normalize() WriterOptions {
	out := WriterOptions{BlockSize: defaultBlockSize, RestartInterval: restartInterval, BloomFP: 0.01}
	if o != nil {
		if o.BlockSize > 0 {
			out.BlockSize = o.BlockSize
		}
		if o.RestartInterval > 0 {
			out.RestartInterval = o.RestartInterval
		}
		if o.BloomFP > 0 {
			out.BloomFP = o.BloomFP
		}
	}
	return out
}
func Create(path string, opts *WriterOptions) (*Writer, error) {
	o := opts.normalize()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		f:          f,
		path:       path,
		blockSize:  o.BlockSize,
		restartInt: o.RestartInterval,
		bloom:      bloomx.NewBuilder(o.BloomFP),
		buf:        make([]byte, 0, o.BlockSize),
	}, nil
}
func (w *Writer) Add(e Entry) error {
	if w.closed {
		return ErrClosed
	}
	if e.Key == nil {
		return ErrInvalid
	}
	// NOTE: tombstones must be persisted. Do not skip Deleted entries.
	if w.lastKey != nil && byteutil.Compare(e.Key, w.lastKey) < 0 {
		return fmt.Errorf("sstable: keys out of order: %q < %q", e.Key, w.lastKey)
	}
	if w.minKey == nil {
		w.minKey = byteutil.Clone(e.Key)
	}
	w.maxKey = byteutil.Clone(e.Key)
	if e.Seq > w.maxSeq {
		w.maxSeq = e.Seq
	}
	w.bloom.Add(e.Key)

	shared := 0
	if w.blockEntries%w.restartInt != 0 && w.lastKey != nil {
		shared = byteutil.SharedPrefixLen(w.lastKey, e.Key)
	} else {
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	}
	nonShared := len(e.Key) - shared
	var enc []byte
	enc = varintx.EncodeUint64(enc, uint64(shared))
	enc = varintx.EncodeUint64(enc, uint64(nonShared))
	enc = varintx.EncodeUint64(enc, uint64(len(e.Value)))
	enc = varintx.EncodeUint64(enc, e.Seq)
	flag := flagValue
	if e.Deleted {
		flag = flagDelete
	}
	enc = append(enc, flag)
	enc = append(enc, e.Key[shared:]...)
	enc = append(enc, e.Value...)

	w.buf = append(w.buf, enc...)
	w.lastKey = byteutil.Clone(e.Key)
	w.entryCount++
	w.blockEntries++

	if len(w.buf) >= w.blockSize {
		return w.flushBlock()
	}
	return nil
}
func (w *Writer) flushBlock() error {
	if len(w.buf) == 0 {
		return nil
	}
	// trailer: restart array + count + crc
	block := append([]byte(nil), w.buf...)
	for _, off := range w.restarts {
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], off)
		block = append(block, tmp[:]...)
	}
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(w.restarts)))
	block = append(block, tmp[:]...)
	crc := crc32x.ChecksumCastagnoli(block)
	binary.LittleEndian.PutUint32(tmp[:], crc)
	block = append(block, tmp[:]...)

	off, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	n, err := w.f.Write(block)
	if err != nil {
		return err
	}
	if n != len(block) {
		return io.ErrShortWrite
	}
	w.index = append(w.index, indexRec{
		lastKey: byteutil.Clone(w.lastKey),
		offset:  uint64(off),
		length:  uint64(len(block)),
	})
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	w.blockEntries = 0
	return nil
}
func (w *Writer) Finish() (*Meta, error) {
	if w.closed {
		return nil, ErrClosed
	}
	if err := w.flushBlock(); err != nil {
		return nil, err
	}
	bloomOff, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	bf := w.bloom.Build()
	bdata, err := bf.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if _, err := w.f.Write(bdata); err != nil {
		return nil, err
	}
	bloomLen := int64(len(bdata))

	indexOff, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	var indexBuf []byte
	indexBuf = varintx.EncodeUint64(indexBuf, uint64(len(w.index)))
	for _, rec := range w.index {
		indexBuf = varintx.EncodeUint64(indexBuf, uint64(len(rec.lastKey)))
		indexBuf = append(indexBuf, rec.lastKey...)
		indexBuf = varintx.EncodeUint64(indexBuf, rec.offset)
		indexBuf = varintx.EncodeUint64(indexBuf, rec.length)
	}
	indexCRC := crc32x.ChecksumCastagnoli(indexBuf)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], indexCRC)
	indexBuf = append(indexBuf, crcBuf[:]...)
	if _, err := w.f.Write(indexBuf); err != nil {
		return nil, err
	}
	indexLen := int64(len(indexBuf))

	// footer: magic(4) | indexOff(8) | indexLen(8) | bloomOff(8) | bloomLen(8) | maxSeq(8) | entries(8) | magic(4)
	const footerN = 56
	var footer [footerN]byte
	binary.LittleEndian.PutUint32(footer[0:4], magicFooter)
	binary.LittleEndian.PutUint64(footer[4:12], uint64(indexOff))
	binary.LittleEndian.PutUint64(footer[12:20], uint64(indexLen))
	binary.LittleEndian.PutUint64(footer[20:28], uint64(bloomOff))
	binary.LittleEndian.PutUint64(footer[28:36], uint64(bloomLen))
	binary.LittleEndian.PutUint64(footer[36:44], w.maxSeq)
	binary.LittleEndian.PutUint64(footer[44:52], uint64(w.entryCount))
	binary.LittleEndian.PutUint32(footer[52:56], magicFooter)
	if _, err := w.f.Write(footer[:]); err != nil {
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	st, err := w.f.Stat()
	if err != nil {
		return nil, err
	}
	if err := w.f.Close(); err != nil {
		return nil, err
	}
	w.closed = true
	return &Meta{
		Path:     w.path,
		MinKey:   w.minKey,
		MaxKey:   w.maxKey,
		MaxSeq:   w.maxSeq,
		Entries:  uint64(w.entryCount),
		FileSize: st.Size(),
	}, nil
}
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.f.Close()
	return os.Remove(w.path)
}
func WriteFile(path string, entries []Entry, opts *WriterOptions) (*Meta, error) {
	w, err := Create(path, opts)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			_ = w.Abort()
			return nil, err
		}
	}
	return w.Finish()
}
