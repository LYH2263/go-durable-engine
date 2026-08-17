package webui_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LYH2263/go-durable-engine/internal/webui"
	"github.com/LYH2263/go-durable-engine/lsmkv"
)

func TestStatusPage(t *testing.T) {
	db, err := lsmkv.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(webui.NewMux(db))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	resp2, err := http.Get(srv.URL + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("api status=%d", resp2.StatusCode)
	}
}
