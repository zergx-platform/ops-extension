package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

// Thin jjlab /ops client: ops-extension owns the tool/agent semantics and the
// Containerfile/publish templates; jjlab owns the k8s/buildkit/helm execution.
// Every call goes to the jjlab base (token already injected by addAuth) at
// /api/v1/ops/*, so this struct is a purpose-neutral wrapper over the generic
// primitives the server exposes.

// opsSubmitBuild enqueues an image build on jjlab and returns the task id.
func (s *server) opsSubmitBuild(ctx context.Context, req map[string]interface{}) (string, error) {
	body, err := s.httpPostJSON(ctx, s.jj+"/api/v1/ops/builds", req)
	if err != nil {
		return "", err
	}
	var out struct {
		BuildID string `json:"build_id"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil || out.BuildID == "" {
		return "", fmt.Errorf("no build_id in %s", body)
	}
	return out.BuildID, nil
}

// opsSubmitHelm enqueues a helm install/upgrade on jjlab and returns the task id.
func (s *server) opsSubmitHelm(ctx context.Context, req map[string]interface{}) (string, error) {
	body, err := s.httpPostJSON(ctx, s.jj+"/api/v1/ops/helm/install", req)
	if err != nil {
		return "", err
	}
	var out struct {
		HelmID string `json:"helm_id"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil || out.HelmID == "" {
		return "", fmt.Errorf("no helm_id in %s", body)
	}
	return out.HelmID, nil
}

// opsTask polls a jjlab task until it leaves "running", returning its summary.
func (s *server) opsTask(ctx context.Context, id string) (map[string]interface{}, error) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		body, err := s.httpGetJSON(ctx, s.jj+"/api/v1/ops/tasks/"+url.PathEscape(id))
		if err != nil {
			return nil, err
		}
		var t struct {
			Status string `json:"status"`
			Result string `json:"result"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &t); err != nil {
			return nil, fmt.Errorf("ops task %s: bad body: %w", id, err)
		}
		if t.Status != "running" {
			return map[string]interface{}{
				"status": t.Status, "result": t.Result, "error": t.Error,
			}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
		}
	}
}

// awaitOpsTask drains an ops task to completion and renders the agent result.
func (s *server) awaitOpsTask(ctx context.Context, kind, tag, id string) (string, error) {
	start := time.Now()
	// The tool already has a bounded caller timeout in most cases; add a hard
	// ceiling so a stuck build can never hang a tool call forever.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	res, err := s.opsTask(ctx, id)
	if err != nil {
		return "", err
	}
	if res["status"] != "done" {
		if msg, _ := res["error"].(string); msg != "" {
			return "", fmt.Errorf("%s failed: %s", kind, msg)
		}
		return "", fmt.Errorf("%s failed", kind)
	}
	_ = start
	return fmt.Sprintf("Finished %s %q", kind, tag), nil
}
