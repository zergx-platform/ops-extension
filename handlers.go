package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "name": "ops-extension"})
}

func (s *server) k8sConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":           true,
		"namespace":    s.k8s.Namespace(),
		"worker_image": s.k8s.WorkerImage(),
	})
}

func (s *server) listContainers(w http.ResponseWriter, r *http.Request) {
	list, err := s.k8s.ListContainers(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := []map[string]interface{}{}
	for _, c := range list {
		out = append(out, map[string]interface{}{
			"container_id": c.ContainerID,
			"pod_name":     c.PodName,
			"worker_url":   c.WorkerURL,
			"status":       c.Status,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"containers": out})
}

type createBody struct {
	Image     *string `json:"image"`
	SessionID *string `json:"session_id"`
	Org       *string `json:"org"`
	Repo      *string `json:"repo"`
	Branch    *string `json:"branch"`
}

func (s *server) createContainer(w http.ResponseWriter, r *http.Request) {
	var b createBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	containerID := uuid.New().String()
	label := containerID
	if b.SessionID != nil && *b.SessionID != "" {
		label = *b.SessionID
	}
	image := ""
	if b.Image != nil {
		image = *b.Image
	}
	info, err := s.k8s.EnsureContainer(r.Context(), label, image)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("container creation failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"container": map[string]interface{}{
			"id":           containerID,
			"name":         info.PodName,
			"image":        image,
			"worker_url":   info.WorkerURL,
			"container_id": info.PodName,
			"session_id":   b.SessionID,
			"org":          b.Org,
			"repo":         b.Repo,
			"branch":       b.Branch,
			"status":       info.Status,
		},
	})
}

func (s *server) deleteContainer(w http.ResponseWriter, r *http.Request) {
	cid := param(r, "cid")
	if err := s.k8s.DestroyContainer(r.Context(), cid); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *server) exec(w http.ResponseWriter, r *http.Request) {
	cid := param(r, "cid")
	var b struct {
		Command string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil || b.Command == "" {
		writeErr(w, http.StatusBadRequest, "command required")
		return
	}
	res, err := s.workerCommand(r.Context(), cid, "execute", map[string]interface{}{"command": b.Command})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	cid := param(r, "cid")
	res, err := s.workerCommand(r.Context(), cid, "jobs", map[string]interface{}{})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"jobs": res})
}

func (s *server) jobOutput(w http.ResponseWriter, r *http.Request) {
	cid := param(r, "cid")
	jobID := param(r, "jobID")
	q := r.URL.Query()
	res, err := s.workerCommand(r.Context(), cid, "job_output", map[string]interface{}{
		"job_id": jobID,
		"stream": q.Get("stream"),
		"start":  parseIntOr(q.Get("start"), -200),
		"end":    parseIntOr(q.Get("end"), -1),
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *server) jobWait(w http.ResponseWriter, r *http.Request) {
	cid := param(r, "cid")
	jobID := param(r, "jobID")
	var b struct {
		TimeoutMS *int64 `json:"timeout_ms"`
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	timeout := int64(30000)
	if b.TimeoutMS != nil {
		timeout = *b.TimeoutMS
	}
	res, err := s.workerCommand(r.Context(), cid, "job_wait", map[string]interface{}{
		"job_id":     jobID,
		"timeout_ms": timeout,
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *server) jobStdin(w http.ResponseWriter, r *http.Request) {
	cid := param(r, "cid")
	jobID := param(r, "jobID")
	var b struct {
		Data  string `json:"data"`
		Close bool   `json:"close"`
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	res, err := s.workerCommand(r.Context(), cid, "job_stdin", map[string]interface{}{
		"job_id": jobID,
		"data":   b.Data,
		"close":  b.Close,
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *server) kill(w http.ResponseWriter, r *http.Request) {
	cid := param(r, "cid")
	jobID := param(r, "jobID")
	res, err := s.workerCommand(r.Context(), cid, "kill", map[string]interface{}{"job_id": jobID})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "result": res})
}

func (s *server) sandboxRead(w http.ResponseWriter, r *http.Request) {
	var b struct {
		ContainerID string `json:"container_id"`
		Path        string `json:"path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	res, err := s.workerCommand(r.Context(), b.ContainerID, "execute", map[string]interface{}{
		"command": fmt.Sprintf("cat %s", shellQuote(b.Path)),
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "content": res})
}

func (s *server) sandboxWrite(w http.ResponseWriter, r *http.Request) {
	var b struct {
		ContainerID string `json:"container_id"`
		Path        string `json:"path"`
		Content     string `json:"content"`
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	b64 := base64Encode(b.Content)
	res, err := s.workerCommand(r.Context(), b.ContainerID, "execute", map[string]interface{}{
		"command": fmt.Sprintf("mkdir -p \"$(dirname %s)\" && echo %s | base64 -d > %s", shellQuote(b.Path), b64, shellQuote(b.Path)),
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "result": res})
}

func (s *server) deploy(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Name     string            `json:"name"`
		Image    string            `json:"image"`
		Replicas int32             `json:"replicas"`
		Port     int32             `json:"port"`
		Env      map[string]string `json:"env"`
	}
	_ = json.NewDecoder(r.Body).Decode(&b)
	if b.Name == "" || b.Image == "" {
		writeErr(w, http.StatusBadRequest, "name/image required")
		return
	}
	if b.Port == 0 {
		b.Port = 8080
	}
	if err := s.k8s.EnsureDeployment(r.Context(), b.Name, b.Image, b.Replicas, b.Port, b.Env); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "name": b.Name, "image": b.Image})
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

var _ = context.Background
