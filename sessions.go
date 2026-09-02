package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/zergx-platform/ops-extension/internal/jjlab"
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

// bookmarkOrDefault returns the bookmark or "latest" when empty.
func (w workspace) bookmarkOrDefault() string {
	if w.bookmark == "" {
		return "latest"
	}
	return w.bookmark
}

type wsCacheEntry struct {
	ws      workspace
	expires time.Time
}

// resolveWorkspace maps a tool call to its workspace. Priority: the
// first-class `session_name` envelope field ("org:repo:bookmark", verified
// against jjlab) → legacy `_org`/`_repo`/`_branch` args. ops-extension
// talks to jjlab only — no repo-extension, no mapping table.
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

// lookupWorkspace resolves the bookmark head via jjlab, with a short
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

// jjBookmarkHead fetches a bookmark's target commit id from jjlab.
func (s *server) jjBookmarkHead(ctx context.Context, org, repo, bm string) (string, error) {
	u := fmt.Sprintf("%s/api/v1/repos/%s/%s/branches", s.jj, urlPathEscape(org), urlPathEscape(repo))
	body, err := s.httpGetRaw(ctx, u)
	if err != nil {
		return "", fmt.Errorf("jj branches %s/%s: %w", org, repo, err)
	}
	var out struct {
		Branches []struct {
			Name string `json:"name"`
			Sha  string `json:"sha"`
		} `json:"branches"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("jj branches %s/%s: bad response: %w", org, repo, err)
	}
	for _, b := range out.Branches {
		if b.Name == bm {
			return b.Sha, nil
		}
	}
	return "", fmt.Errorf("bookmark %q not found in %s/%s", bm, org, repo)
}

// ensureSandbox resolves the workspace, ensures the session's worker pod
// exists (get-or-create via jjlab), and — when needSync — that the pod's
// workspace matches the bookmark head (sync executed server-side by jjlab).
func (s *server) ensureSandbox(ctx context.Context, args map[string]interface{}, sessionName string, needSync bool) (sandboxCtx, error) {
	ws, sid, err := s.resolveWorkspace(ctx, args, sessionName)
	if err != nil {
		return sandboxCtx{}, err
	}
	info, err := s.ensureWorker(ctx, sid)
	if err != nil {
		return sandboxCtx{}, fmt.Errorf("ensure container for %s: %w", sid, err)
	}
	s.publishSandboxVars(ctx, sid, info)
	sc := sandboxCtx{session: sid, cid: info.ContainerID, ws: ws}
	if needSync {
		if err := s.ensureSynced(ctx, sc.cid, sc.session, ws); err != nil {
			return sandboxCtx{}, err
		}
	}
	return sc, nil
}

// sandboxWorkerPort is the port worker-go serves inside the sandbox pod.
// The worker's default is 8080, so the ensure request pins ZERGX_PORT to keep
// the process, the declared containerPort and the readiness probe in sync.
const sandboxWorkerPort = 48080

// ensureWorker get-or-creates the session's worker pod through jjlab (a bare
// `zergx-worker` service with a zergx/session annotation preserving the raw
// session name) and returns its container info.
func (s *server) ensureWorker(ctx context.Context, sid string) (ContainerInfo, error) {
	key := labelKey(sid)
	st, err := s.jjops.EnsureService(ctx, jjlab.ServiceRequest{
		Name:  key,
		Image: s.workerImage,
		Kind:  "bare",
		Ports: []jjlab.PortSpec{{Container: sandboxWorkerPort, Service: 80}},
		Env: map[string]string{
			"WORKER_PORT": "48080",
		},
		Annotations: map[string]string{
			"zergx/session": sid,
		},
		Namespace: s.runtimeNamespace,
	})
	if err != nil {
		return ContainerInfo{}, err
	}
	return ContainerInfo{
		ContainerID: key,
		PodName:     "sandbox-" + key[:8],
		Namespace:   s.runtimeNamespace,
		WorkerURL:   st.WorkerURL(sandboxWorkerPort),
		PodIP:       st.PodIP,
		Status:      statusFromReady(st),
		SessionName: sid,
	}, nil
}

// statusFromReady maps jjlab's readiness into the legacy status vocabulary.
func statusFromReady(st jjlab.ServiceStatus) string {
	if st.Ready {
		return "running"
	}
	if st.Phase != "" {
		return strings.ToLower(st.Phase)
	}
	return "pending"
}

// workerInfo fetches the current sandbox state from jjlab.
func (s *server) workerInfo(ctx context.Context, key string) (ContainerInfo, error) {
	st, err := s.jjops.Service(ctx, key, s.runtimeNamespace)
	if err != nil {
		return ContainerInfo{}, err
	}
	return ContainerInfo{
		ContainerID: key,
		PodName:     "sandbox-" + key[:8],
		Namespace:   s.runtimeNamespace,
		WorkerURL:   st.WorkerURL(sandboxWorkerPort),
		PodIP:       st.PodIP,
		Status:      statusFromReady(st),
	}, nil
}

// destroyWorker deletes the sandbox through jjlab. The ID may be the raw
// session name, the derived key, or a pod/short name.
func (s *server) destroyWorker(ctx context.Context, id string) error {
	return s.jjops.DeleteService(ctx, labelKey(id), s.runtimeNamespace)
}

// ensureSynced pushes the repo tree at ws.rev into the worker unless jjlab
// already holds that rev. The tarball fetch + overlay extract happen inside
// jjlab (which owns both the repo store and the pod); ops-extension just
// names the target.
func (s *server) ensureSynced(ctx context.Context, cid, session string, ws workspace) error {
	if s.syncedRev(cid) == ws.rev {
		return nil
	}
	if _, err := s.jjops.Sync(ctx, labelKey(session), jjlab.SyncRequest{
		Org:       ws.org,
		Repo:      ws.repo,
		Rev:       ws.rev,
		Namespace: s.runtimeNamespace,
	}, false); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	s.setSyncedRev(cid, ws.rev)
	return nil
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

// ── local naming helpers (moved from internal/k8s; sessions are addressed by
// deterministic keys, no stored state) ──

// labelKey sanitizes an arbitrary label (session names contain ':' which is
// illegal in k8s label values and pod names) into a deterministic k8s-safe
// key. Values that are already valid are used as-is (e.g. UUIDs).
func labelKey(label string) string {
	if validLabelValue(label) {
		return label
	}
	sum := sha256.Sum256([]byte(label))
	return hex.EncodeToString(sum[:])[:16]
}

// validLabelValue follows the k8s label value grammar: alphanumerics, '-',
// '_' and '.', at most 63 chars.
func validLabelValue(v string) bool {
	if len(v) == 0 || len(v) > 63 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

// ContainerInfo is the ops-extension view of a sandbox worker (now sourced
// from jjlab instead of client-go pod listings).
type ContainerInfo struct {
	ContainerID string
	PodName     string
	Namespace   string
	WorkerURL   string
	PodIP       string
	Status      string
	SessionName string
}
