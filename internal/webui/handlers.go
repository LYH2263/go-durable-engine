package webui

import (
	"encoding/json"
	"net/http"

	"github.com/LYH2263/go-durable-engine/lsmkv"
)

// Stats mirrors a subset of DB stats for the status API.
type Stats struct {
	MemtableKeys int   `json:"memtable_keys"`
	SSTFiles     int   `json:"sst_files"`
	WALSize      int64 `json:"wal_size"`
}

// NewMux registers HTML and JSON status routes for db.
func NewMux(db *lsmkv.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(IndexHTML())
	})
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		st := db.Stats()
		_ = json.NewEncoder(w).Encode(Stats{
			MemtableKeys: st.MemtableKeys,
			SSTFiles:     st.SSTFiles,
			WALSize:      st.WALSize,
		})
	})
	return mux
}

// ListenAndServe starts a minimal status server (for demos/tests).
func ListenAndServe(addr string, db *lsmkv.DB) error {
	return http.ListenAndServe(addr, NewMux(db))
}
