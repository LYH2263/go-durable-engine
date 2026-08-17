package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func Open(dir string) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fileName)
	m := &Manifest{dir: dir, path: path}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			m.state = State{Version: versionJSON, NextFile: 1, Files: nil, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			if err := m.persistLocked(); err != nil {
				return nil, err
			}
			return m, nil
		}
		return nil, err
	}
	st, err := decode(data)
	if err != nil {
		return nil, err
	}
	m.state = st
	return m, nil
}
func (m *Manifest) persistLocked() error {
	m.state.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	m.state.Version = versionJSON
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + tmpSuffix
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	// best-effort sync directory
	if d, err := os.Open(m.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
func (m *Manifest) Current() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.state)
}
func (m *Manifest) Files() []FileMeta {
	return m.Current().Files
}
func (m *Manifest) LogSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state.LogSeq
}
func (m *Manifest) AllocFileName() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := fmt.Sprintf("%06d.sst", m.state.NextFile)
	m.state.NextFile++
	if err := m.persistLocked(); err != nil {
		return "", err
	}
	return name, nil
}
func (m *Manifest) AddFile(meta FileMeta, logSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Files = append(m.state.Files, meta)
	if logSeq > m.state.LogSeq {
		m.state.LogSeq = logSeq
	}
	return m.persistLocked()
}
func (m *Manifest) Replace(remove []string, add []FileMeta, logSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rm := map[string]struct{}{}
	for _, n := range remove {
		rm[n] = struct{}{}
	}
	kept := make([]FileMeta, 0, len(m.state.Files))
	for _, f := range m.state.Files {
		if _, ok := rm[f.Name]; !ok {
			kept = append(kept, f)
		}
	}
	kept = append(kept, add...)
	m.state.Files = kept
	if logSeq > m.state.LogSeq {
		m.state.LogSeq = logSeq
	}
	return m.persistLocked()
}
func (m *Manifest) SetLogSeq(seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if seq > m.state.LogSeq {
		m.state.LogSeq = seq
	}
	return m.persistLocked()
}
func (m *Manifest) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.state.Files)
}
func (m *Manifest) MaxSeqAmongFiles() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var max uint64
	for _, f := range m.state.Files {
		if f.MaxSeq > max {
			max = f.MaxSeq
		}
	}
	return max
}
func (m *Manifest) Path() string { return m.path }
func (m *Manifest) Dir() string  { return m.dir }
