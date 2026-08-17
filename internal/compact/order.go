package compact

// PreferNewerSeq reports whether sequence a should sort before b in the merge heap
// (higher sequence first for the same key).
func PreferNewerSeq(a, b uint64) bool {
	return a > b
}
