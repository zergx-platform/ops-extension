package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	extensionsdk "rucoder-agent/extension-sdk-go"

	"rucoder-agent/ops-extension/internal/k8s"
	"rucoder-agent/ops-extension/internal/worker"
)

type server struct {
	k8s *k8s.Manager
	ext *extensionsdk.Extension
}

func main() {
	ns := envOr("RUCODER_K8S_NAMESPACE", "temp")
	img := envOr("RUCODER_WORKER_IMAGE", "recoder-dev002.develop.10.199.64.20.nip.io/rucoder-worker:dev")
	natsURL := envOr("NATS_URL", "nats://nats.develop.svc.cluster.local:4222")
	port := envOr("RUCODER_PORT", "8080")

	km, err := k8s.NewManager(k8s.Config{Namespace: ns, WorkerImage: img})
	if err != nil {
		panic(fmt.Sprintf("k8s: %v", err))
	}

	s := &server{k8s: km}

	// Register the NATS extension (tools only in phase 1).
	ext, err := extensionsdk.Register(extensionsdk.Config{
		ID:      "ops-extension",
		Version: "0.1.0",
		NATSURL: natsURL,
		Tools:   s.tools(),
	})
	if err != nil {
		panic(fmt.Sprintf("extension: %v", err))
	}
	s.ext = ext
	defer ext.Close()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", s.health)
		r.Get("/containers", s.listContainers)
		r.Post("/containers", s.createContainer)
		r.Delete("/containers/{cid}", s.deleteContainer)
		r.Post("/containers/{cid}/exec", s.exec)
		r.Get("/containers/{cid}/jobs", s.listJobs)
		r.Get("/containers/{cid}/jobs/{jobID}/output", s.jobOutput)
		r.Post("/containers/{cid}/jobs/{jobID}/wait", s.jobWait)
		r.Post("/containers/{cid}/jobs/{jobID}/stdin", s.jobStdin)
		r.Post("/containers/{cid}/kill/{jobID}", s.kill)
		r.Post("/sandbox/read", s.sandboxRead)
		r.Post("/sandbox/write", s.sandboxWrite)
		r.Post("/deploy", s.deploy)
		r.Get("/infra/k8s/config", s.k8sConfig)
	})

	addr := ":" + port
	fmt.Printf("[ops-extension] listening on %s\n", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		panic(err)
	}
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
