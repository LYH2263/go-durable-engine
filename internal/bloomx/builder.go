package bloomx

type Builder struct {
	keys [][]byte
	fp   float64
}

func NewBuilder(fp float64) *Builder {
	if fp <= 0 || fp >= 1 {
		fp = 0.01
	}
	return &Builder{fp: fp}
}
func (b *Builder) Add(key []byte) {
	cp := make([]byte, len(key))
	copy(cp, key)
	b.keys = append(b.keys, cp)
}
func (b *Builder) Len() int { return len(b.keys) }
func (b *Builder) Build() *Filter {
	f := New(len(b.keys), b.fp)
	for _, k := range b.keys {
		f.Add(k)
	}
	return f
}
func (b *Builder) Reset() { b.keys = b.keys[:0] }
