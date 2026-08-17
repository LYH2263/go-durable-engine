package memtable

type MemTable struct {
	sl       *SkipList
	maxBytes int64
}

func NewMemTable(maxBytes int64) *MemTable {
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}
	return &MemTable{sl: New(), maxBytes: maxBytes}
}
func (m *MemTable) Underlying() *SkipList             { return m.sl }
func (m *MemTable) Put(key, value []byte, seq uint64) { m.sl.Put(key, value, seq) }
func (m *MemTable) Delete(key []byte, seq uint64)     { m.sl.Delete(key, seq) }
func (m *MemTable) Get(key []byte) (Entry, bool)      { return m.sl.Get(key) }
func (m *MemTable) ShouldFlush() bool {
	return m.sl.Bytes() >= m.maxBytes
}
func (m *MemTable) Bytes() int64           { return m.sl.Bytes() }
func (m *MemTable) Len() int               { return m.sl.Len() }
func (m *MemTable) Snapshot() []Entry      { return m.sl.Snapshot() }
func (m *MemTable) Clear()                 { m.sl.Clear() }
func (m *MemTable) MaxSeq() uint64         { return m.sl.MaxSeq() }
func (m *MemTable) NewIterator() *Iterator { return m.sl.NewIterator() }
