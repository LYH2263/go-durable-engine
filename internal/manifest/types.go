// Package manifest tracks the set of live SSTable files for an LSM database.
package manifest

import (
	"errors"
	"sync"
)

var (
	ErrCorrupt = errors.New("manifest: corrupt")
	ErrClosed  = errors.New("manifest: closed")
)

const (
	fileName    = "MANIFEST"
	tmpSuffix   = ".tmp"
	magicBinary = uint32(0x4D4E4653) // MNFS
	versionJSON = 1
)

// FileMeta describes one SSTable in the manifest.
type FileMeta struct {
	Name     string `json:"name"`
	Level    int    `json:"level"`
	MinKey   []byte `json:"min_key"`
	MaxKey   []byte `json:"max_key"`
	MaxSeq   uint64 `json:"max_seq"`
	Entries  uint64 `json:"entries"`
	FileSize int64  `json:"file_size"`
}
type State struct {
	Version   int        `json:"version"`
	NextFile  uint64     `json:"next_file"`
	LogSeq    uint64     `json:"log_seq"`
	Files     []FileMeta `json:"files"`
	UpdatedAt string     `json:"updated_at"`
}
type Manifest struct {
	mu    sync.Mutex
	dir   string
	path  string
	state State
}
