package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"forgejo.develop.10.199.64.20.nip.io/zergx/ops-extension/internal/jsonwrite"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// writeRawJSON re-emits an already-encoded JSON body.
func writeRawJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

// ---- async install task (reuses the buildTask machinery: SSE + status) ----

// helmInstallBody carries a helm install/upgrade request into the async task.
type helmInstallBody struct {
	ReleaseName string                 `json:"release_name"`
	Chart       string                 `json:"chart"` // local dir path, or chart ref
	Version     string                 `json:"version,omitempty"`
	Values      map[string]interface{} `json:"values,omitempty"`
	// repo-mode: chart lives in a repo checkout fetched from jjlab.
	Org      string `json:"org,omitempty"`
	Repo     string `json:"repo,omitempty"`
	Bookmark string `json:"bookmark,omitempty"`
	// chart_path is relative to the fetched repo root (default ".").
	ChartPath string `json:"chart_path,omitempty"`
}

// startHelmInstall forwards the install request to jjlab's async task
// (which owns the helm binary, the k8s access and the chart checkout).
// repo-mode (org/repo/bookmark) is also resolved inside jjlab: the body's
// chart path is repo-relative there. Here we keep the legacy behavior of
// fetching the archive locally? No — jjlab fetches from its own repo store;
// we simply pass org/repo/bookmark/chart_path through.
func (s *server) startHelmInstall(b helmInstallBody) string {
	id, err := s.opsSubmitHelm(context.Background(), map[string]interface{}{
		"release_name": b.ReleaseName,
		"chart":        b.Chart,
		"version":      b.Version,
		"values":       b.Values,
		"org":          b.Org,
		"repo":         b.Repo,
		"bookmark":     b.Bookmark,
		"chart_path":   b.ChartPath,
	})
	if err != nil {
		// jjlab rejections surface as a failed task id-less error; fabricate
		// a finished task so the SSE/status flow reports it.
		id = s.failTask("helm", b.ReleaseName, err.Error())
	}
	return id
}

// failTask registers an already-failed build task (used when the upstream
// submission itself fails, so callers still get a pollable id).
func (s *server) failTask(kind, tag, msg string) string {
	id := uuid.NewString()
	now := time.Now()
	t := &buildTask{
		ID:         id,
		Kind:       kind,
		Tag:        tag,
		State:      "failed",
		StartedAt:  now,
		FinishedAt: &now,
		Error:      msg,
		subs:       map[chan buildLogLine]struct{}{},
	}
	s.builds.Store(id, t)
	s.evictBuilds()
	return id
}

// helmInstall is the async install/upgrade endpoint (jjlab executes).
func (s *server) helmInstall(w http.ResponseWriter, r *http.Request) {
	var b helmInstallBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if b.ReleaseName == "" || (b.Chart == "" && (b.Org == "" || b.Repo == "")) {
		writeErr(w, http.StatusBadRequest, "release_name + (chart or org/repo) required")
		return
	}
	id := s.startHelmInstall(b)
	jsonwrite.JSON(w, http.StatusAccepted, map[string]interface{}{"ok": true, "build_id": id})
}

// helmList returns all releases in the namespace (jjlab-sourced).
func (s *server) helmList(w http.ResponseWriter, r *http.Request) {
	v, err := s.httpGetJSON(r.Context(), s.jj+"/api/v1/ops/helm/releases?namespace="+url.QueryEscape(s.runtimeNamespace))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeRawJSON(w, v)
}

// helmStatus returns one release's status (jjlab-sourced).
func (s *server) helmStatus(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	v, err := s.httpGetJSON(r.Context(), s.jj+"/api/v1/ops/helm/releases/"+urlPathEscape(name)+"?namespace="+url.QueryEscape(s.runtimeNamespace))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeRawJSON(w, v)
}

// helmValues returns one release's computed values (jjlab-sourced).
func (s *server) helmValues(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	v, err := s.httpGetJSON(r.Context(), s.jj+"/api/v1/ops/helm/releases/"+urlPathEscape(name)+"/values?namespace="+url.QueryEscape(s.runtimeNamespace))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeRawJSON(w, v)
}

// helmUninstall removes a release (jjlab executes).
func (s *server) helmUninstall(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := s.httpDelete(r.Context(), s.jj+"/api/v1/ops/helm/releases/"+urlPathEscape(name)+"?namespace="+url.QueryEscape(s.runtimeNamespace)); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

// helmRollback reverts a release to a previous revision (0 = previous).
func (s *server) helmRollback(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var b struct {
		Revision int `json:"revision"`
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	if err := s.httpPostJSONErr(r.Context(), s.jj+"/api/v1/ops/helm/releases/"+urlPathEscape(name)+"/rollback?namespace="+url.QueryEscape(s.runtimeNamespace), map[string]interface{}{"revision": b.Revision}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}
