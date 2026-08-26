package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	abep "abep.dev/sdk"
	natsbus "abep.dev/sdk/nats"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"rucoder-agent/ops-extension/internal/buildkit"
	"rucoder-agent/ops-extension/internal/k8s"
	"rucoder-agent/ops-extension/internal/worker"
)

//go:embed manifest.yaml
var manifestYaml []byte

type server struct {
	k8s               *k8s.Manager
	ext               *abep.Extension
	buildkit          *buildkit.Client
	artifact          string // artifact registry base URL (packages + OCI + metadata)
	artifactImageHost string // TLS ingress host for image refs (FROM/push via buildkit)
	artifactToken     string // optional bearer/basic token for artifact write auth
	jj                string // jj-server URL (repo archive + contents + clone)

	wsMu    sync.Mutex              // guards wsCache
	wsCache map[string]wsCacheEntry // session -> workspace (short TTL)

	syncMu sync.Mutex        // guards synced
	synced map[string]string // container key -> synced rev

	// workerResolver overrides worker URL resolution (tests).
	workerResolver func(cid string) (string, error)

	builds sync.Map // build id -> *buildTask
}

func main() {
	ns := envOr("RUCODER_K8S_NAMESPACE", "temp")
	img := envOr("RUCODER_WORKER_IMAGE", "recoder-dev002.develop.10.199.64.20.nip.io/rucoder-worker:dev")
	natsURL := envOr("NATS_URL", "nats://nats.develop.svc.cluster.local:4222")
	port := envOr("RUCODER_PORT", "8080")
	portValue = port
	buildkitAddr := envOr("RUCODER_BUILDKIT_ADDR", "tcp://rucoder-buildkitd.temp.svc.cluster.local:1234")
	// jj-server replaces the old repo-manager (archive + contents + clone).
	// The cluster service is named rucoder-repo (jj-server is the binary).
	jj := envOr("RUCODER_JJ_SERVER_URL", envOr("RUCODER_REPO_MANAGER_URL", "http://rucoder-repo.temp.svc.cluster.local:80"))
	// Artifact registry replaces zot (OCI store) + rucoder-registry (metadata):
	// one base URL serves /v2 (OCI), /pkgs/<format> (protocol proxies) and
	// /pkgs/system (admin/metadata). This is the plain-HTTP in-cluster base
	// used for API calls and in-container CLI uploads.
	artifact := trimTrailingSlash(envOr("RUCODER_ARTIFACT_URL", "http://rucoder-artifact.temp.svc.cluster.local:80"))
	// Image references (buildkit FROM/push) must go through the TLS ingress
	// host configured as insecure in buildkitd's registry config — the svc
	// host is plain HTTP which buildkit cannot pull/push to.
	artifactImageHost := envOr("RUCODER_ARTIFACT_IMAGE_HOST", "rucoder-artifact.temp.10.199.64.20.nip.io")
	artifactToken := envOr("RUCODER_ARTIFACT_TOKEN", "")

	km, err := k8s.NewManager(k8s.Config{
		Namespace:     ns,
		WorkerImage:   img,
		CPURequest:    envOr("RUCODER_WORKER_CPU_REQUEST", ""),
		CPULimit:      envOr("RUCODER_WORKER_CPU_LIMIT", ""),
		MemoryRequest: envOr("RUCODER_WORKER_MEM_REQUEST", ""),
		MemoryLimit:   envOr("RUCODER_WORKER_MEM_LIMIT", ""),
	})
	if err != nil {
		slog.Error("k8s manager init failed", "svc", "ops-extension", "err", err)
		os.Exit(1)
	}

	s := &server{
		k8s:               km,
		buildkit:          buildkit.New(buildkitAddr),
		artifact:          artifact,
		artifactImageHost: artifactImageHost,
		artifactToken:     artifactToken,
		jj:                jj,
		wsCache:           map[string]wsCacheEntry{},
		synced:            map[string]string{},
	}

	// Verification instances must set RUCODER_DISABLE_NATS=1: the SDK uses
	// plain Subscribe (no queue groups), so a second replica on the same NATS
	// would receive duplicate tool.call messages alongside the live service.
	// To keep the tools testable, such instances expose them over HTTP at
	// POST /api/v1/tools/{name} with a JSON args body.
	toolBridge := false
	if os.Getenv("RUCODER_DISABLE_NATS") != "1" {
		nbus, err := natsbus.Connect(natsURL)
		if err != nil {
			slog.Error("nats connect failed", "svc", "ops-extension", "err", err)
			os.Exit(1)
		}
		manifest, err := abep.ParseManifest(manifestYaml)
		if err != nil {
			slog.Error("load manifest failed", "svc", "ops-extension", "err", err)
			os.Exit(1)
		}

		r := s.router(toolBridge)

		if err := abep.Serve(
			nbus,
			manifest.Config(
				s.handlers(),
				map[string]abep.VariableSpec{
					"sandbox-id":     {Resolve: s.resolveSandboxID},
					"sandbox-status": {Resolve: s.resolveSandboxStatus},
				},
				func(ctx context.Context, ev abep.LifecycleEvent) error {
					if ev.Kind == "deleted" {
						s.clearSandboxVars(ctx, ev.SessionName)
					}
					return nil
				},
			),
			abep.ServeOptions{
				Handler: r,
				Port:    port,
				Run: func(runCtx context.Context, ext *abep.Extension) {
					s.ext = ext
					slog.Info("listening", "svc", "ops-extension", "addr", ":"+port, "buildkit", buildkitAddr, "artifact", artifact, "jj", jj)
				},
			},
		); err != nil {
			slog.Error("serve failed", "svc", "ops-extension", "err", err)
			os.Exit(1)
		}
		return
	}

	r := s.router(true)
	addr := ":" + port
	slog.Info("listening", "svc", "ops-extension", "addr", addr, "buildkit", buildkitAddr, "artifact", artifact, "jj", jj)
	if err := http.ListenAndServe(addr, r); err != nil {
		slog.Error("http server failed", "svc", "ops-extension", "err", err)
		os.Exit(1)
	}
}

// router builds the chi router serving both the ops API and the embedded SPA.
// `toolBridge` exposes the NATS tools over HTTP for verification instances.
func (s *server) router(toolBridge bool) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", s.health)
		r.Get("/sandboxes", s.sandboxesList)
		r.Get("/sandboxes/{session}", s.sandboxGet)
		r.Delete("/sandboxes/{session}", s.deleteContainer)
		r.Post("/sandboxes/{session}/exec", s.exec)
		r.Get("/sandboxes/{session}/jobs", s.listJobs)
		r.Get("/sandboxes/{session}/jobs/{jobID}/output", s.jobOutput)
		r.Post("/sandboxes/{session}/jobs/{jobID}/wait", s.jobWait)
		r.Post("/sandboxes/{session}/jobs/{jobID}/stdin", s.jobStdin)
		r.Post("/sandboxes/{session}/jobs/{jobID}/kill", s.kill)
		r.Post("/sandboxes/{session}/read", s.sandboxRead)
		r.Post("/sandboxes/{session}/write", s.sandboxWrite)
		r.Get("/sandboxes/{session}/ws", s.wsProxy)
		r.Get("/sandboxes/{session}/ws/job", s.wsProxyJob)
		r.Post("/deployments", s.deploy)
		r.Get("/infra/k8s/config", s.k8sConfig)
		r.Post("/images/build", s.buildImage)
		r.Get("/builds", s.buildsList)
		r.Get("/builds/{id}", s.buildGet)
		r.Get("/builds/{id}/stream", s.buildStream)
		r.Get("/containerfile-templates", s.containerfileTemplates)
		r.Get("/status", s.status)
		r.Get("/deployments", s.deploymentsList)
		r.Get("/deployments/{name}/pods", s.deploymentPods)
		r.Get("/deployments/{name}/status", s.deploymentStatus)
		r.Post("/deployments/{name}/restart", s.deploymentRestart)
		r.Post("/deployments/{name}/scale", s.deploymentScale)
		r.Post("/deployments/{name}/rollback", s.deploymentRollback)
		r.Get("/deployments/{name}/events", s.deploymentEvents)
		r.Get("/deployments/{name}/revisions", s.deploymentRevisions)
		r.Delete("/deployments/{name}", s.deploymentDelete)
		r.Post("/helm/install", s.helmInstall)
		r.Get("/helm/releases", s.helmList)
		r.Get("/helm/releases/{name}/status", s.helmStatus)
		r.Get("/helm/releases/{name}/values", s.helmValues)
		r.Post("/helm/releases/{name}/rollback", s.helmRollback)
		r.Delete("/helm/releases/{name}", s.helmUninstall)
		r.Get("/packages", s.packagesList)
		r.Get("/images", s.imagesList)
		r.Get("/publish-specs", s.publishSpecsHandler)
		r.Post("/packages/publish", s.packagesPublish)
		if toolBridge {
			r.Post("/tools/{name}", s.callTool)
		}
	})

	// Embedded SPA (served at /; /api/v1 routes registered above win).
	r.Handle("/*", spaHandler())
	return r
}

// callTool bridges a NATS tool over HTTP for verification instances
// (RUCODER_DISABLE_NATS=1). Body: JSON object of tool args.
func (s *server) callTool(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	spec, ok := s.handlers()[name]
	if !ok {
		writeErr(w, http.StatusNotFound, "no such tool: "+name)
		return
	}
	args := map[string]interface{}{}
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil && err.Error() != "EOF" {
		writeErr(w, http.StatusBadRequest, "invalid args body: "+err.Error())
		return
	}
	out, _, err := spec.Execute(r.Context(), args, "http-verify", "", func(string) {})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "result": out})
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]interface{}{"ok": false, "error": msg})
}

// resolveWorkerURL finds the worker URL for a container ID.
func (s *server) resolveWorkerURL(ctx context.Context, cid string) (string, error) {
	list, err := s.k8s.ListContainers(ctx)
	if err != nil {
		return "", err
	}
	for _, c := range list {
		if c.ContainerID == cid || c.PodName == cid {
			if c.WorkerURL == "" {
				return "", fmt.Errorf("worker not ready")
			}
			return c.WorkerURL, nil
		}
	}
	return "", fmt.Errorf("no worker for container %s", cid)
}

func (s *server) workerCommand(ctx context.Context, cid, method string, params map[string]interface{}) (interface{}, error) {
	wu, err := s.resolveWorkerURL(ctx, cid)
	if err != nil {
		return nil, err
	}
	return worker.CommandOnce(ctx, worker.ToWsURL(wu), method, params)
}
