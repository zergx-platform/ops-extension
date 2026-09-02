package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zergx-platform/ops-extension/internal/jjlab"
)

// --- flat tarball fixture (jjlab archive has no top-level dir) --------

func buildFlatArchive(files map[string]string) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		hdr := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}
		_ = tw.WriteHeader(hdr)
		_, _ = tw.Write([]byte(content))
	}
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

func readArchive(data []byte) []string {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var out []string
	for {
		hdr, err := tr.Next()
		if err != nil {
			return out
		}
		out = append(out, hdr.Name)
	}
}

// --- jj bookmark resolution ---------------------------------------------

func newFakeJJ(t *testing.T, bookmarks string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/repos/verify/exists/branches", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(bookmarks))
	})
	mux.HandleFunc("/api/v1/repos/verify/nope/branches", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"branches":[]}`))
	})
	mux.HandleFunc("/api/v1/repos/verify/ws/", func(w http.ResponseWriter, r *http.Request) {
		// jjlab tarball is flat: /api/v1/repos/verify/ws/archive/tarball/{rev}
		rev := strings.TrimPrefix(r.URL.Path, "/api/v1/repos/verify/ws/archive/tarball/")
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(buildFlatArchive(map[string]string{
			"file.txt": "content-" + rev,
		}))
	})
	return httptest.NewServer(mux)
}

func TestJJBookmarkHead(t *testing.T) {
	jj := newFakeJJ(t, `{"branches":[{"name":"main","sha":"abc123"}]}`)
	defer jj.Close()
	s := &server{jj: jj.URL, wsCache: map[string]wsCacheEntry{}}

	rev, err := s.jjBookmarkHead(context.Background(), "verify", "exists", "main")
	if err != nil || rev != "abc123" {
		t.Fatalf("rev=%q err=%v", rev, err)
	}
	if _, err := s.jjBookmarkHead(context.Background(), "verify", "nope", "main"); err == nil {
		t.Fatal("missing bookmark should error")
	}
}

func TestResolveWorkspaceSessionName(t *testing.T) {
	jj := newFakeJJ(t, `{"branches":[{"name":"main","sha":"abc123"}]}`)
	defer jj.Close()
	s := &server{jj: jj.URL, wsCache: map[string]wsCacheEntry{}}

	ws, sid, err := s.resolveWorkspace(context.Background(), map[string]interface{}{}, "verify:exists:main")
	if err != nil || ws.org != "verify" || ws.repo != "exists" || ws.bookmark != "main" || ws.rev != "abc123" || sid != "verify:exists:main" {
		t.Fatalf("ws=%+v sid=%q err=%v", ws, sid, err)
	}

	if _, _, err := s.resolveWorkspace(context.Background(), map[string]interface{}{}, "not-a-derived-name"); err == nil {
		t.Fatal("non-derived session name must error")
	}
}

func TestSyncStateMachine(t *testing.T) {
	var syncs int32
	var lastBody []byte
	var lastPath string
	fakeJJ := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/sync") {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&syncs, 1)
		lastPath = r.URL.Path
		lastBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"ok":true,"skipped":false,"files":1}`))
	}))
	defer fakeJJ.Close()

	s := &server{jjops: jjlab.New(fakeJJ.URL, "devtoken"), runtimeNamespace: "temp",
		wsCache: map[string]wsCacheEntry{}, synced: map[string]string{}}
	ws := workspace{org: "verify", repo: "ws", bookmark: "main", rev: "rev1"}

	// First ensure → syncs.
	if err := s.ensureSynced(context.Background(), "cid1", "verify:ws:main", ws); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&syncs); n != 1 {
		t.Fatalf("syncs=%d want 1", n)
	}
	// The sync request must name the org/repo/rev.
	var reqBody struct {
		Org  string `json:"org"`
		Repo string `json:"repo"`
		Rev  string `json:"rev"`
	}
	_ = json.Unmarshal(lastBody, &reqBody)
	if reqBody.Org != "verify" || reqBody.Repo != "ws" || reqBody.Rev != "rev1" {
		t.Fatalf("sync body = %+v", reqBody)
	}
	// The path must address the session's sandbox (labelKey of the session).
	wantPath := "/api/v1/ops/services/" + labelKey("verify:ws:main") + "/sync"
	if lastPath != wantPath {
		t.Fatalf("sync path = %q want %q", lastPath, wantPath)
	}

	// Second ensure with same rev → cached, no extra sync.
	if err := s.ensureSynced(context.Background(), "cid1", "verify:ws:main", ws); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&syncs); n != 1 {
		t.Fatalf("syncs=%d want 1 (cached)", n)
	}

	// New rev → syncs again.
	ws.rev = "rev2"
	if err := s.ensureSynced(context.Background(), "cid1", "verify:ws:main", ws); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&syncs); n != 2 {
		t.Fatalf("syncs=%d want 2", n)
	}

	// Worker restart (need_sync): markUnsynced forces a re-push.
	s.markUnsynced("cid1")
	if err := s.ensureSynced(context.Background(), "cid1", "verify:ws:main", ws); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&syncs); n != 3 {
		t.Fatalf("syncs=%d want 3", n)
	}

	// jjlab failure surfaces as an error (unreachable server).
	closed := jjlab.New("http://127.0.0.1:1", "")
	if _, err := closed.Sync(context.Background(), "x", jjlab.SyncRequest{
		Org: "o", Repo: "r", Rev: "v"}, false); err == nil {
		t.Fatal("unreachable jjlab must error")
	}
}

func TestSyncWorkerRejectsBadResponse(t *testing.T) {
	fakeJJ := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"ok":false,"error":"worker sync 500: boom"}`))
	}))
	defer fakeJJ.Close()
	s := &server{jjops: jjlab.New(fakeJJ.URL, "devtoken"), runtimeNamespace: "temp",
		synced: map[string]string{}}
	ws := workspace{org: "o", repo: "r", bookmark: "main", rev: "v"}
	if err := s.ensureSynced(context.Background(), "cid", "o:r:main", ws); err == nil {
		t.Fatal("jjlab error must propagate")
	}
	if s.syncedRev("cid") != "" {
		t.Fatal("rev must not be recorded on failure")
	}
}
