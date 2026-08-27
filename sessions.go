package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// sandboxCtx is the resolved per-call sandbox context: which workspace, which
// worker pod, and the repo revision the pod is synced to.
type sandboxCtx struct {
	session string // raw session name (or derived legacy label)
	cid     string // container key (k8s label value)
	ws      workspace
}

// workspace is the org/repo/bookmark triple plus the bookmark's target commit.
type workspace struct {
	org      string
	repo     string
	bookmark string
	rev      string // bookmark head commit id
}

type wsCacheEntry struct {
	ws      workspace
	expires time.Time
}

// resolveWorkspace maps a tool call to its workspace. Priority: the
// first-class `session_name` envelope field ("org:repo:bookmark", verified
// against jj-server) → legacy `_org`/`_repo`/`_branch` args. ops-extension
// talks to jj-server only — no repo-extension, no mapping table.
func (s *server) resolveWorkspace(ctx context.Context, args map[string]interface{}, sessionName string) (workspace, string, error) {
	if sessionName != "" {
		org, repo, bm, ok := parseSessionName(sessionName)
		if !ok {
			return workspace{}, "", fmt.Errorf(
				"session %q is not named org:repo:bookmark — cannot resolve its workspace (rename the session or pass _org/_repo/_branch explicitly)", sessionName)
		}
		ws, err := s.lookupWorkspace(ctx, sessionName, org, repo, bm)
		return ws, sessionName, err
	}
	org, repo, bm := strArg(args, "_org"), strArg(args, "_repo"), strArg(args, "_branch")
	if org == "" || repo == "" {
		return workspace{}, "", fmt.Errorf("missing session context (pass session_name, or _org/_repo)")
	}
	if bm == "" {
		bm = "main"
	}
	sid := org + ":" + repo + ":" + bm
	ws, err := s.lookupWorkspace(ctx, sid, org, repo, bm)
	return ws, sid, err
}

// lookupWorkspace resolves the bookmark head via jj-server, with a short
// cache to keep the hot path (every sandbox tool call) free of extra round
// trips while a call still notices bookmark moves quickly. Expired entries
// are swept on every miss so the map cannot grow unbounded across sessions.
func (s *server) lookupWorkspace(ctx context.Context, sid, org, repo, bm string) (workspace, error) {
	s.wsMu.Lock()
	if e, ok := s.wsCache[sid]; ok && time.Now().Before(e.expires) {
		ws := e.ws
		s.wsMu.Unlock()
		return ws, nil
	}
	s.sweepExpiredLocked()
	s.wsMu.Unlock()

	rev, err := s.jjBookmarkHead(ctx, org, repo, bm)
	if err != nil {
		return workspace{}, err
	}
	ws := workspace{org: org, repo: repo, bookmark: bm, rev: rev}
	s.wsMu.Lock()
	s.wsCache[sid] = wsCacheEntry{ws: ws, expires: time.Now().Add(30 * time.Second)}
	s.wsMu.Unlock()
	return ws, nil
}

// sweepExpiredLocked drops expired cache entries; caller holds wsMu.
func (s *server) sweepExpiredLocked() {
	now := time.Now()
	for k, e := range s.wsCache {
		if !now.Before(e.expires) {
			delete(s.wsCache, k)
		}
	}
}

// invalidateWorkspace drops the cached resolution (e.g. after sandbox-port
// moved the bookmark) so the next call observes the new head.
func (s *server) invalidateWorkspace(sid string) {
	s.wsMu.Lock()
	delete(s.wsCache, sid)
	s.wsMu.Unlock()
}

// jjBookmarkHead fetches a bookmark's target commit id from jj-server.
func (s *server) jjBookmarkHead(ctx context.Context, org, repo, bm string) (string, error) {
	u := fmt.Sprintf("%s/api/v1/repos/%s/%s/bookmarks", s.jj, urlPathEscape(org), urlPathEscape(repo))
	body, err := s.httpGetRaw(ctx, u)
	if err != nil {
		return "", fmt.Errorf("jj bookmarks %s/%s: %w", org, repo, err)
	}
	var out struct {
		Bookmarks []struct {
			Branch   string `json:"branch"`
			FullName string `json:"full_name"`
			Target   string `json:"target"`
		} `json:"bookmarks"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("jj bookmarks %s/%s: bad response: %w", org, repo, err)
	}
	for _, b := range out.Bookmarks {
		if b.Branch == bm || b.FullName == "refs/heads/"+bm {
			return b.Target, nil
		}
	}
	return "", fmt.Errorf("bookmark %q not found in %s/%s", bm, org, repo)
}

// ensureSandbox resolves the workspace, ensures the session's worker pod
// exists (get-or-create), and — when needSync — that the pod's workspace
// matches the bookmark head.
func (s *server) ensureSandbox(ctx context.Context, args map[string]interface{}, sessionName string, needSync bool) (sandboxCtx, error) {
	ws, sid, err := s.resolveWorkspace(ctx, args, sessionName)
	if err != nil {
		return sandboxCtx{}, err
	}
	info, err := s.k8s.EnsureContainer(ctx, sid, "")
	if err != nil {
		return sandboxCtx{}, fmt.Errorf("ensure container for %s: %w", sid, err)
	}
	s.publishSandboxVars(ctx, sid, info)
	sc := sandboxCtx{session: sid, cid: info.ContainerID, ws: ws}
	if needSync {
		if err := s.ensureSynced(ctx, sc.cid, ws); err != nil {
			return sandboxCtx{}, err
		}
	}
	return sc, nil
}

// ensureSynced pushes the repo tree at ws.rev into the worker unless the
// worker already holds that rev. Only the worker's sync/files endpoint is
// used (a pure overlay extract): files that exist only in the sandbox are
// never deleted, repo files are overwritten with the new rev's content.
func (s *server) ensureSynced(ctx context.Context, cid string, ws workspace) error {
	if s.syncedRev(cid) == ws.rev {
		return nil
	}
	wu, err := s.workerBaseURL(ctx, cid)
	if err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	if err := s.syncToURL(ctx, wu, ws); err != nil {
		return err
	}
	s.setSyncedRev(cid, ws.rev)
	return nil
}

// workerBaseURL resolves a container's worker HTTP base (injectable for tests).
func (s *server) workerBaseURL(ctx context.Context, cid string) (string, error) {
	if s.workerResolver != nil {
		return s.workerResolver(cid)
	}
	return s.resolveWorkerURL(ctx, cid)
}

// markUnsynced forgets the tracked rev (worker restart / need_sync) so the
// next ensureSynced pushes again.
func (s *server) markUnsynced(cid string) {
	s.syncMu.Lock()
	delete(s.synced, cid)
	s.syncMu.Unlock()
}

func (s *server) syncedRev(cid string) string {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	return s.synced[cid]
}

func (s *server) setSyncedRev(cid, rev string) {
	s.syncMu.Lock()
	s.synced[cid] = rev
	s.syncMu.Unlock()
}

// syncToURL streams the jj archive for ws.rev into the worker's sync/files
// endpoint, stripping the archive's top-level "{repo}-{rev}/" directory so the
// workspace root holds the repo files directly. Fully streaming (no temp
// files): jj response → repack pipe → worker request body.
func (s *server) syncToURL(ctx context.Context, workerBase string, ws workspace) error {
	archiveURL := fmt.Sprintf("%s/api/v1/repos/%s/%s/%s/archive",
		s.jj, urlPathEscape(ws.org), urlPathEscape(ws.repo), urlPathEscape(ws.rev))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, archiveURL, nil)
	if err != nil {
		return err
	}
	resp, err := longClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch archive: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch archive: %d", resp.StatusCode)
	}

	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(repackStripTop(resp.Body, pw))
	}()

	syncURL := strings.TrimSuffix(workerBase, "/") + "/api/v1/sync/files?rev=" + url.QueryEscape(ws.rev)
	sreq, err := http.NewRequestWithContext(ctx, http.MethodPost, syncURL, pr)
	if err != nil {
		return err
	}
	sreq.Header.Set("Content-Type", "application/gzip")
	sresp, err := longClient.Do(sreq)
	if err != nil {
		return fmt.Errorf("worker sync: %w", err)
	}
	defer sresp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(sresp.Body, 1<<20))
	if sresp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker sync: HTTP %d: %s", sresp.StatusCode, string(body))
	}
	var out struct {
		OK    bool   `json:"ok"`
		Files int    `json:"files"`
		Err   string `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil || !out.OK {
		return fmt.Errorf("worker sync: %s", orDefaultStr(string(body), err.Error()))
	}
	return nil
}

// repackStripTop streams a tar.gz, dropping the first path component of every
// entry (like tar --strip-components=1). The top-level entry itself is
// skipped entirely.
func repackStripTop(r io.Reader, w io.Writer) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	out := gzip.NewWriter(w)
	defer out.Close()
	tw := tar.NewWriter(out)
	defer tw.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := strings.TrimPrefix(hdr.Name, "./")
		parts := strings.SplitN(name, "/", 2)
		if len(parts) < 2 || parts[1] == "" {
			continue // the stripped top-level entry itself
		}
		hdr.Name = parts[1]
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := io.Copy(tw, tr); err != nil {
				return err
			}
		}
	}
}

func orDefaultStr(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
