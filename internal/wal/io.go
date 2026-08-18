package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/LYH2263/go-durable-engine/internal/crc32x"
	"github.com/LYH2263/go-durable-engine/internal/varintx"
)

func (o *Options) normalize() Options {
	out := Options{SyncOnWrite: true, FileName: fileName}
	if o != nil {
		out = *o
		if out.FileName == "" {
			out.FileName = fileName
		}
	}
	return out
}
func Open(dir string, opts *Options) (*Log, error) {
	o := opts.normalize()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, o.FileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	l := &Log{
		dir:  dir,
		path: path,
		f:    f,
		opts: o,
		size: st.Size(),
	}
	if err := l.recoverTail(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}
func (l *Log) Path() string { return l.path }
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}
func (l *Log) NextSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextSeq + 1
}
func (l *Log) allocSeq() uint64 {
	l.nextSeq++
	return l.nextSeq
}
func (l *Log) AppendPut(key, value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	seq := l.allocSeq()
	payload := encodeKV(seq, key, value)
	if err := l.writeFrame(TypePut, payload); err != nil {
		return 0, err
	}
	return seq, l.maybeSyncLocked()
}
func (l *Log) AppendDelete(key []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	seq := l.allocSeq()
	payload := encodeKV(seq, key, nil)
	if err := l.writeFrame(TypeDelete, payload); err != nil {
		return 0, err
	}
	return seq, l.maybeSyncLocked()
}
func (l *Log) AppendBatch(entries []BatchEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if len(entries) == 0 {
		return l.nextSeq, nil
	}
	var payload []byte
	payload = varintx.EncodeUint64(payload, uint64(len(entries)))
	var last uint64
	for _, e := range entries {
		seq := l.allocSeq()
		last = seq
		payload = varintx.EncodeUint64(payload, seq)
		if e.Deleted {
			payload = append(payload, TypeDelete)
		} else {
			payload = append(payload, TypePut)
		}
		payload = varintx.EncodeUint64(payload, uint64(len(e.Key)))
		payload = append(payload, e.Key...)
		payload = varintx.EncodeUint64(payload, uint64(len(e.Value)))
		payload = append(payload, e.Value...)
	}
	if err := l.writeFrame(TypeBatch, payload); err != nil {
		return 0, err
	}
	return last, l.maybeSyncLocked()
}
func encodeKV(seq uint64, key, value []byte) []byte {
	var b []byte
	b = varintx.EncodeUint64(b, seq)
	b = varintx.EncodeUint64(b, uint64(len(key)))
	b = append(b, key...)
	b = varintx.EncodeUint64(b, uint64(len(value)))
	b = append(b, value...)
	return b
}
func (l *Log) writeFrame(typ byte, payload []byte) error {
	if err := InjectWriteErr; err != nil {
		InjectWriteErr = nil
		return err
	}
	if len(payload) > 1<<28 {
		return fmt.Errorf("wal: payload too large: %d", len(payload))
	}
	crc := crc32x.FrameChecksum(typ, payload)
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = typ
	binary.LittleEndian.PutUint32(hdr[5:9], crc)
	n, err := l.f.Write(hdr)
	if err != nil {
		return err
	}
	if n != len(hdr) {
		return ErrShortWrite
	}
	if len(payload) > 0 {
		n, err = l.f.Write(payload)
		if err != nil {
			return err
		}
		if n != len(payload) {
			return ErrShortWrite
		}
	}
	l.size += int64(headerSize + len(payload))
	l.appends++
	return nil
}
func (l *Log) maybeSyncLocked() error {
	if l.opts.SyncEveryN > 0 {
		if l.appends%l.opts.SyncEveryN == 0 {
			return l.f.Sync()
		}
		return nil
	}
	if l.opts.SyncOnWrite {
		return l.f.Sync()
	}
	return nil
}
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.f.Sync()
}
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var first error
	if err := l.f.Sync(); err != nil {
		first = err
	}
	if err := l.f.Close(); err != nil && first == nil {
		first = err
	}
	return first
}
func (l *Log) Reset() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.f.Truncate(0); err != nil {
		return err
	}
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	l.size = 0
	l.appends = 0
	// keep nextSeq monotonic across resets
	return l.f.Sync()
}
func (l *Log) SetNextSeq(seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq > l.nextSeq {
		l.nextSeq = seq
	}
}
func CRCTable() *crc32.Table {
	return crc32.MakeTable(crc32.Castagnoli)
}
