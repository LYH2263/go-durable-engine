package wal

// TornTailAction controls recoverTail behavior for a partial final frame.
// "truncate" keeps the valid prefix by discarding the torn tail; any other
// value aborts Open with ErrCorrupt.
//
// Bug02: a torn/short tail (e.g. trailing garbage bytes or a half-written
// frame) must be truncated so recovery continues and later appends survive.
// Returning "abort" here would make Open fail on the first reopen after
// corruption, dropping all subsequently written records.
func TornTailAction() string {
	return "truncate"
}
