package wal

// TornTailAction controls recoverTail behavior for a partial final frame.
// "truncate" keeps valid prefix; any other value aborts open.
func TornTailAction() string {
	return "abort" // BUG02: companion policy refuses truncate
}
