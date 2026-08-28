package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"forgejo.develop.10.199.64.20.nip.io/zergx/go-shared/jsonwrite"
	"os"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/release"
	"log/slog"
)

// helmManager wraps helm.sh/helm/v3 action.Configuration for release
// lifecycle (install/upgrade/uninstall/list/status/values/rollback). The
// namespace is fixed to the ops-extension namespace (single-node dev cluster
// without cross-namespace RBAC).
type helmManager struct {
	namespace string
	settings  *cli.EnvSettings
}

// newHelmManager builds an action.Configuration that resolves k8s access the
// same way the k8s.Manager does (explicit ZERGX_KUBECONFIG, then
// in-cluster, then ~/.kube/config).
func newHelmManager(namespace string) *helmManager {
	settings := cli.New()
	settings.SetNamespace(namespace)
	// Respect the same explicit-kubeconfig override as k8s.Manager for
	// verification instances.
	if kc := os.Getenv("ZERGX_KUBECONFIG"); kc != "" {
		settings.KubeConfig = kc
	}
	return &helmManager{namespace: namespace, settings: settings}
}

// cfg builds a fresh action.Configuration (per call, so each action gets a
// clean client).
func (h *helmManager) cfg() (*action.Configuration, error) {
	cfg := new(action.Configuration)
	err := cfg.Init(h.settings.RESTClientGetter(), h.namespace, "secret", func(format string, v ...interface{}) {
		slog.Debug(fmt.Sprintf("[helm] "+format, v...))
	})
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// installOrUpgrade locates a chart and installs it (or upgrades an existing
// release of the same name). `chartRef` is either a local directory path or a
// chart reference resolvable by ChartPathOptions (repo URL + name).
func (h *helmManager) installOrUpgrade(ctx context.Context, releaseName, chartRef, version string, values map[string]interface{}) (*release.Release, error) {
	cfg, err := h.cfg()
	if err != nil {
		return nil, err
	}

	cp := action.ChartPathOptions{}
	chartPath, err := cp.LocateChart(chartRef, h.settings)
	if err != nil {
		return nil, fmt.Errorf("locate chart: %w", err)
	}
	chrt, err := loader.Load(chartPath)
	if err != nil {
		return nil, fmt.Errorf("load chart: %w", err)
	}

	// Prefer upgrade when the release already exists.
	hist := action.NewHistory(cfg)
	hist.Max = 1
	if _, err := hist.Run(releaseName); err == nil {
		up := action.NewUpgrade(cfg)
		up.Namespace = h.namespace
		up.Wait = true
		up.Timeout = 10 * time.Minute
		if version != "" {
			up.Version = version
		}
		rel, err := up.RunWithContext(ctx, releaseName, chrt, values)
		if err != nil {
			return nil, fmt.Errorf("upgrade: %w", err)
		}
		return rel, nil
	}

	inst := action.NewInstall(cfg)
	inst.Namespace = h.namespace
	inst.ReleaseName = releaseName
	inst.CreateNamespace = false
	inst.Wait = true
	inst.Timeout = 10 * time.Minute
	if version != "" {
		inst.Version = version
	}
	rel, err := inst.RunWithContext(ctx, chrt, values)
	if err != nil {
		return nil, fmt.Errorf("install: %w", err)
	}
	return rel, nil
}

// uninstall removes a release.
func (h *helmManager) uninstall(releaseName string) error {
	cfg, err := h.cfg()
	if err != nil {
		return err
	}
	un := action.NewUninstall(cfg)
	un.Wait = true
	un.Timeout = 5 * time.Minute
	_, err = un.Run(releaseName)
	return err
}

// list returns all releases in the namespace.
func (h *helmManager) list() ([]*release.Release, error) {
	cfg, err := h.cfg()
	if err != nil {
		return nil, err
	}
	l := action.NewList(cfg)
	l.All = true
	return l.Run()
}

// status returns the release status.
func (h *helmManager) status(releaseName string) (*release.Release, error) {
	cfg, err := h.cfg()
	if err != nil {
		return nil, err
	}
	return action.NewStatus(cfg).Run(releaseName)
}

// values returns the computed values of a release.
func (h *helmManager) values(releaseName string) (map[string]interface{}, error) {
	cfg, err := h.cfg()
	if err != nil {
		return nil, err
	}
	return action.NewGetValues(cfg).Run(releaseName)
}

// rollback reverts a release to a previous revision.
func (h *helmManager) rollback(releaseName string, revision int) error {
	cfg, err := h.cfg()
	if err != nil {
		return err
	}
	r := action.NewRollback(cfg)
	r.Version = revision
	r.Wait = true
	r.Timeout = 5 * time.Minute
	return r.Run(releaseName)
}

// releaseSummary maps a release to the public JSON view.
func releaseSummary(r *release.Release) map[string]interface{} {
	status := ""
	description := ""
	if r.Info != nil {
		status = string(r.Info.Status)
		description = r.Info.Description
	}
	chartName := ""
	chartVersion := ""
	appVersion := ""
	if r.Chart != nil {
		chartName = r.Chart.Name()
		if r.Chart.Metadata != nil {
			chartVersion = r.Chart.Metadata.Version
			appVersion = r.Chart.Metadata.AppVersion
		}
	}
	return map[string]interface{}{
		"name":          r.Name,
		"namespace":     r.Namespace,
		"version":       r.Version,
		"status":        status,
		"description":   description,
		"chart":         chartName,
		"chart_version": chartVersion,
		"app_version":   appVersion,
	}
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

// startHelmInstall registers a helm install task and runs it in a background
// goroutine (async, like image builds).
func (s *server) startHelmInstall(b helmInstallBody) string {
	id := uuid.NewString()
	t := &buildTask{
		ID:        id,
		Kind:      "helm",
		Tag:       b.ReleaseName,
		State:     "running",
		StartedAt: time.Now(),
		subs:      map[chan buildLogLine]struct{}{},
	}
	s.builds.Store(id, t)
	s.evictBuilds()

	go s.runHelmInstall(t, b)
	return id
}

func (s *server) runHelmInstall(t *buildTask, b helmInstallBody) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	onStatus := func(line string) {
		for _, ln := range splitLines(line) {
			if ln != "" {
				t.append(buildLogLine{Stream: "helm", Line: ln})
			}
		}
	}

	chartRef := b.Chart
	// repo-mode: fetch the repo archive, then point at the chart dir inside it.
	if b.Org != "" && b.Repo != "" {
		tmpDir, err := s.fetchRepoArchive(ctx, b.Org, b.Repo, b.Bookmark)
		if err != nil {
			t.append(buildLogLine{Stream: "helm", Line: "ERROR: " + err.Error()})
			t.setResult("", err.Error())
			return
		}
		defer os.RemoveAll(tmpDir)
		chartPath := b.ChartPath
		if chartPath == "" {
			chartPath = "."
		}
		chartRef = filepath.Join(tmpDir, chartPath)
	}

	hm := newHelmManager(s.k8s.Namespace())
	rel, err := hm.installOrUpgrade(ctx, b.ReleaseName, chartRef, b.Version, b.Values)
	if err != nil {
		t.append(buildLogLine{Stream: "helm", Line: "ERROR: " + err.Error()})
		t.setResult("", err.Error())
		return
	}
	onStatus(fmt.Sprintf("release %q installed (version %d, status %s)", rel.Name, rel.Version, rel.Info.Status))
	t.setResult(rel.Name, "")
}

// ---- HTTP handlers ----

// helmInstall is the async install/upgrade endpoint.
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

// helmList returns all releases in the namespace.
func (s *server) helmList(w http.ResponseWriter, r *http.Request) {
	hm := newHelmManager(s.k8s.Namespace())
	rels, err := hm.list()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := []map[string]interface{}{}
	for _, rel := range rels {
		out = append(out, releaseSummary(rel))
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]interface{}{"releases": out})
}

// helmStatus returns one release's status.
func (s *server) helmStatus(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	hm := newHelmManager(s.k8s.Namespace())
	rel, err := hm.status(name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]interface{}{"release": releaseSummary(rel)})
}

// helmValues returns one release's computed values.
func (s *server) helmValues(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	hm := newHelmManager(s.k8s.Namespace())
	vals, err := hm.values(name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]interface{}{"values": vals})
}

// helmUninstall removes a release.
func (s *server) helmUninstall(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	hm := newHelmManager(s.k8s.Namespace())
	if err := hm.uninstall(name); err != nil {
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
	hm := newHelmManager(s.k8s.Namespace())
	if err := hm.rollback(name, b.Revision); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	jsonwrite.JSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}
