package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	abep "abep.dev/sdk"

	"rucoder-agent/ops-extension/internal/k8s"
	"rucoder-agent/ops-extension/internal/worker"
)

// handlers returns the NATS tool handlers. Descriptions/schemas live in
// manifest.yaml (the single declarative protocol source); each handler is
// bound by tool name. Sandbox tools are session-scoped: ops-extension
// resolves the workspace via jj-server, lazily creates/reuses the session's
// worker pod, and syncs the repo tree into it (overlay-only, sandbox-only
// files are never deleted) before running.
func (s *server) handlers() map[string]abep.ToolSpec {
	return map[string]abep.ToolSpec{
		"sandbox-run": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				command := strArg(args, "command")
				if command == "" {
					return "", nil, fmt.Errorf("sandbox-run: missing 'command'")
				}
				sc, err := s.ensureSandbox(ctx, args, sessionName, true)
				if err != nil {
					return "", nil, err
				}
				workerURL, err := s.resolveWorkerURL(ctx, sc.cid)
				if err != nil {
					return "", nil, err
				}

				run := func(rev string) (worker.ExecuteResult, error) {
					return worker.Execute(ctx, worker.ToWsURL(workerURL), command, rev)
				}

				res, err := run(sc.ws.rev)
				if err != nil {
					// Worker may have restarted (synced_rev lost): re-sync once
					// and retry; if it still refuses, execute without the rev
					// gate (content is verified synced on our side).
					if strings.Contains(err.Error(), "need_sync") {
						s.markUnsynced(sc.cid)
						if err := s.ensureSynced(ctx, sc.cid, sc.ws); err != nil {
							return "", nil, err
						}
						if res, err = run(sc.ws.rev); err != nil {
							if res, err = run(""); err != nil {
								return "", nil, fmt.Errorf("sandbox-run failed: %w", err)
							}
						}
					} else {
						return "", nil, fmt.Errorf("sandbox-run failed: %w", err)
					}
				}

				// Sync-wait the job up to timeout_ms (default 10s). The worker
				// always registers a backgrounded job; the per-job SSE stream
				// replays history then streams live output until job.completed.
				// The bus is model-facing and carries no streamed deltas, so
				// the terminal tool result folds the captured output in; the
				// UI reads live output via the gateway's per-worker SSE proxy.
				timeoutMs := int(abep.ArgInt(args, "timeout_ms", 10000))
				if timeoutMs <= 0 {
					timeoutMs = 10000
				}
				streamCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
				defer cancel()

				type streamResult struct {
					done worker.JobDone
					err  error
				}
				resultCh := make(chan streamResult, 1)
				go func() {
					done, err := worker.StreamJobOutput(streamCtx, workerURL, res.JobID, nil)
					resultCh <- streamResult{done: done, err: err}
				}()

				select {
				case sr := <-resultCh:
					if sr.err != nil && !errors.Is(sr.err, context.DeadlineExceeded) {
						return "", nil, fmt.Errorf("sandbox-run stream failed: %w", sr.err)
					}
					content := fmt.Sprintf("Command completed (job %s, exit %d)", res.JobID, sr.done.ExitCode)
					if sr.done.Stdout != "" {
						content += "\n" + sr.done.Stdout
					}
					if sr.done.Stderr != "" {
						content += "\n[stderr]\n" + sr.done.Stderr
					}
					return content, map[string]interface{}{
						"job_id":       res.JobID,
						"exit_code":    sr.done.ExitCode,
						"backgrounded": false,
					}, nil

				case <-streamCtx.Done():
					// Timed out: hand the job to a background watcher and return
					// immediately. The watcher keeps the SSE stream open until
					// completion, then notifies the agent via the session
					// mailbox (payload.content is folded into the chat).
					if s.ext != nil {
						go func(jobID, sid string) {
							bgCtx, bgCancel := context.WithTimeout(context.Background(), 30*time.Minute)
							defer bgCancel()
							done, _ := worker.StreamJobOutput(bgCtx, workerURL, jobID, nil)
							msg := fmt.Sprintf("Background command finished (job %s, exit %d)", jobID, done.ExitCode)
							if done.Stdout != "" {
								msg += "\n" + done.Stdout
							}
							if done.Stderr != "" {
								msg += "\n[stderr]\n" + done.Stderr
							}
							_ = s.ext.PublishMailboxEvent(context.Background(), sid, "event",
								map[string]interface{}{"content": msg})
						}(res.JobID, sessionName)
					}
					content := fmt.Sprintf(
						"Command is still running in the background (job %s); it did not finish within %dms. It keeps running in the background and you will be notified on completion. Meanwhile you can inspect current output with sandbox-job-output, or stop it with sandbox-job-kill.",
						res.JobID, timeoutMs)
					return content, map[string]interface{}{
						"job_id":       res.JobID,
						"backgrounded": true,
					}, nil
				}
			},
		},
		"sandbox-read": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				path := strArg(args, "path")
				sc, err := s.ensureSandbox(ctx, args, sessionName, true)
				if err != nil {
					return "", nil, err
				}
				data, err := s.sandboxFileRead(ctx, sc.cid, path)
				if err != nil {
					return "", nil, fmt.Errorf("sandbox-read failed: %w", err)
				}
				return string(data), nil, nil
			},
		},
		"sandbox-write": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				path := strArg(args, "path")
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return "", nil, err
				}
				if err := s.sandboxFileWrite(ctx, sc.cid, path, []byte(strArg(args, "content"))); err != nil {
					return "", nil, fmt.Errorf("sandbox-write failed: %w", err)
				}
				return fmt.Sprintf("Wrote sandbox file '%s'.", path), nil, nil
			},
		},
		"sandbox-edit": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				path := strArg(args, "path")
				startLine := intArg64(args, "start_line", 0)
				endLine := intArg64(args, "end_line", 0)
				content := strArg(args, "content")
				sc, err := s.ensureSandbox(ctx, args, sessionName, true)
				if err != nil {
					return "", nil, err
				}
				v, err := s.sandboxEdit(ctx, sc.cid, path, startLine, endLine, content)
				return v, nil, err
			},
		},
		"sandbox-job-list": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return "", nil, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "jobs", map[string]interface{}{})
				if err != nil {
					return "", nil, fmt.Errorf("sandbox-job-list failed: %w", err)
				}
				return toJSON(res), nil, nil
			},
		},
		"sandbox-job-output": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return "", nil, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "job_output", jobArgs(args))
				if err != nil {
					return "", nil, fmt.Errorf("sandbox-job-output failed: %w", err)
				}
				return toJSON(res), nil, nil
			},
		},
		"sandbox-job-wait": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return "", nil, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "job_wait", jobArgs(args))
				if err != nil {
					return "", nil, fmt.Errorf("sandbox-job-wait failed: %w", err)
				}
				return toJSON(res), nil, nil
			},
		},
		"sandbox-job-stdin": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return "", nil, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "job_stdin", map[string]interface{}{
					"job_id": strArg(args, "job_id"),
					"data":   strArg(args, "data"),
				})
				if err != nil {
					return "", nil, fmt.Errorf("sandbox-job-stdin failed: %w", err)
				}
				return toJSON(res), nil, nil
			},
		},
		"sandbox-job-kill": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return "", nil, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "kill", map[string]interface{}{
					"job_id": strArg(args, "job_id"),
				})
				if err != nil {
					return "", nil, fmt.Errorf("sandbox-job-kill failed: %w", err)
				}
				return toJSON(res), nil, nil
			},
		},
		"sandbox-port": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return "", nil, err
				}
				v, err := s.portFile(ctx, sc, args)
				if err == nil {
					// The bookmark moved: forget the cached head so the next
					// call re-syncs and observes the ported file.
					s.invalidateWorkspace(sc.session)
				}
				return v, nil, err
			},
		},
		"container-build": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				ws, _, err := s.resolveWorkspace(ctx, args, sessionName)
				if err != nil {
					return "", nil, err
				}
				payload := map[string]interface{}{
					"org":        ws.org,
					"repo":       ws.repo,
					"bookmark":   ws.bookmark,
					"dockerfile": strArg(args, "dockerfile_path"),
					"tag":        strArg(args, "tag"),
					"context":    strArg(args, "context"),
					"push":       true,
					"no_cache":   boolArg(args, "no_cache"),
				}
				res, err := s.httpPostJSON(ctx, selfBase()+"/api/v1/images/build", payload)
				if err != nil {
					return "", nil, fmt.Errorf("container-build failed: %w", err)
				}
				var submit struct {
					BuildID string `json:"build_id"`
				}
				if err := json.Unmarshal([]byte(res), &submit); err != nil || submit.BuildID == "" {
					return "", nil, fmt.Errorf("container-build failed: no build_id in %s", res)
				}
				return s.awaitBuild(ctx, submit.BuildID)
			},
		},
		"container-deploy": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				image := strArg(args, "image")
				name := strArg(args, "name")
				if name == "" {
					name = "app"
				}
				// Deployments belong to the calling session (if one is given),
				// so GET /sandboxes/{session} can list them.
				session := sessionName
				if session == "" {
					if org := strArg(args, "org"); org != "" {
						bm := strArg(args, "bookmark")
						if bm == "" {
							bm = "main"
						}
						session = org + ":" + strArg(args, "repo") + ":" + bm
					}
				}
				rr := resourceRequestFromArgs(args)
				reqs, err := rr.Requirements()
				if err != nil {
					return "", nil, fmt.Errorf("container-deploy failed: %w", err)
				}
				if err := s.k8s.EnsureDeployment(ctx, name, image, 1, 8080, nil, session, reqs); err != nil {
					return "", nil, fmt.Errorf("container-deploy failed: %w", err)
				}
				return fmt.Sprintf("Deployed '%s' from %s.", name, image), nil, nil
			},
		},
		"image-list": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				v, err := s.imageList(ctx)
				return v, nil, err
			},
		},
		"helm-install": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				release := strArg(args, "release_name")
				if release == "" {
					return "", nil, fmt.Errorf("helm-install: missing 'release_name'")
				}
				ws, _, err := s.resolveWorkspace(ctx, args, sessionName)
				if err != nil {
					return "", nil, err
				}
				payload := map[string]interface{}{
					"release_name": release,
					"org":          ws.org,
					"repo":         ws.repo,
					"bookmark":     ws.bookmark,
					"chart_path":   strArg(args, "chart_path"),
				}
				if v := args["values"]; v != nil {
					payload["values"] = v
				}
				res, err := s.httpPostJSON(ctx, selfBase()+"/api/v1/helm/install", payload)
				if err != nil {
					return "", nil, fmt.Errorf("helm-install failed: %w", err)
				}
				var submit struct {
					BuildID string `json:"build_id"`
				}
				if err := json.Unmarshal([]byte(res), &submit); err != nil || submit.BuildID == "" {
					return "", nil, fmt.Errorf("helm-install failed: no build_id in %s", res)
				}
				return s.awaitBuild(ctx, submit.BuildID)
			},
		},
		"helm-list": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				v, err := s.httpGetJSON(ctx, selfBase()+"/api/v1/helm/releases")
				return v, nil, err
			},
		},
		"helm-status": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				name := strArg(args, "release_name")
				if name == "" {
					return "", nil, fmt.Errorf("helm-status: missing 'release_name'")
				}
				v, err := s.httpGetJSON(ctx, selfBase()+"/api/v1/helm/releases/"+urlPathEscape(name)+"/status")
				return v, nil, err
			},
		},
		"helm-uninstall": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				name := strArg(args, "release_name")
				if name == "" {
					return "", nil, fmt.Errorf("helm-uninstall: missing 'release_name'")
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodDelete, selfBase()+"/api/v1/helm/releases/"+urlPathEscape(name), nil)
				if err != nil {
					return "", nil, err
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return "", nil, fmt.Errorf("helm-uninstall failed: %w", err)
				}
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				if resp.StatusCode >= 300 {
					return "", nil, fmt.Errorf("helm-uninstall failed: HTTP %d %s", resp.StatusCode, string(body))
				}
				return fmt.Sprintf("Uninstalled helm release %q", name), nil, nil
			},
		},
		"package-publish": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				protocol := strArg(args, "protocol")
				// Explicit org/repo win; else resolve the session workspace.
				org, repo, bookmark := strArg(args, "org"), strArg(args, "repo"), strArg(args, "bookmark")
				if org == "" || repo == "" {
					ws, _, err := s.resolveWorkspace(ctx, args, sessionName)
					if err != nil {
						return "", nil, err
					}
					if org == "" {
						org, repo = ws.org, ws.repo
					}
					if bookmark == "" {
						bookmark = ws.bookmark
					}
				}
				res, err := s.publishPackage(ctx, protocol, org, repo, bookmark,
					strArg(args, "name"), strArg(args, "version"),
					strArg(args, "file"), strArg(args, "dockerfile_path"))
				return res, nil, err
			},
		},
		"list-registry-packages": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				v, err := s.httpGetJSON(ctx, s.artifact+"/pkgs/system/packages")
				return v, nil, err
			},
		},
		"list-containerfile-templates": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				return toJSON(builtinTemplates()), nil, nil
			},
		},
		"pull-git-repo": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (string, map[string]interface{}, error) {
				gitURL := strArg(args, "git_url")
				if gitURL == "" {
					return "", nil, fmt.Errorf("pull-git-repo: missing 'git_url'")
				}
				repo := inferRepoFromGitURL(gitURL)
				if repo == "" {
					return "", nil, fmt.Errorf("cannot infer repo name from %s", gitURL)
				}
				org := strArg(args, "org")
				if org == "" {
					org = "external"
				}
				body := map[string]interface{}{
					"org":     org,
					"repo":    repo,
					"git_url": gitURL,
				}
				v, err := s.httpPostJSON(ctx, s.jj+"/api/v1/repos/clone", body)
				return v, nil, err
			},
		},
	}
}

// jobArgs lifts the shared job params (id + output window) for job RPCs.
func jobArgs(args map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"job_id": strArg(args, "job_id"),
	}
	if v := args["start"]; v != nil {
		out["start"] = v
	}
	if v := args["end"]; v != nil {
		out["end"] = v
	}
	if g := strArg(args, "grep"); g != "" {
		out["grep"] = g
	}
	if v := args["timeout_ms"]; v != nil {
		out["timeout_ms"] = v
	}
	return out
}

func strArg(args map[string]interface{}, k string) string {
	if v, ok := args[k].(string); ok {
		return v
	}
	return ""
}

func intArg64(args map[string]interface{}, k string, def int64) int64 {
	if v, ok := args[k].(float64); ok {
		return int64(v)
	}
	return def
}

func boolArg(args map[string]interface{}, k string) bool {
	if v, ok := args[k].(bool); ok {
		return v
	}
	return false
}

func toJSON(v interface{}) string {
	b, err := jsonMarshalIndent(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func jsonMarshalIndent(v interface{}) ([]byte, error) {
	return marshalIndent(v)
}

var _ = strings.TrimSpace

// sandboxEdit implements edit as read-modify-write over the worker sandbox
// using the native file_read/file_write RPCs (the minimal worker shell has no
// pipes/redirection).
func (s *server) sandboxEdit(ctx context.Context, cid, path string, startLine, endLine int64, content string) (string, error) {
	data, err := s.sandboxFileRead(ctx, cid, path)
	if err != nil {
		return "", fmt.Errorf("sandbox edit read failed: %w", err)
	}
	current := string(data)
	lines := strings.Split(current, "\n")
	var newLines []string
	if content != "" {
		newLines = strings.Split(content, "\n")
	}
	if endLine < startLine {
		// insert before startLine (1-based)
		insertAt := int(startLine - 1)
		if insertAt > len(lines) {
			insertAt = len(lines)
		}
		v := append([]string{}, lines[:insertAt]...)
		v = append(v, newLines...)
		v = append(v, lines[insertAt:]...)
		lines = v
	} else {
		// replace [startLine, endLine] (1-based, inclusive)
		sIdx := int(startLine - 1)
		eIdx := int(endLine)
		if eIdx > len(lines) {
			eIdx = len(lines)
		}
		if sIdx > len(lines) {
			sIdx = len(lines)
		}
		v := append([]string{}, lines[:sIdx]...)
		v = append(v, newLines...)
		v = append(v, lines[eIdx:]...)
		lines = v
	}
	newContent := strings.Join(lines, "\n")
	if err := s.sandboxFileWrite(ctx, cid, path, []byte(newContent)); err != nil {
		return "", fmt.Errorf("sandbox edit write failed: %w", err)
	}
	return fmt.Sprintf("Edited sandbox file '%s'.", path), nil
}

// rawMap coerces the workerCommand result to map[string]interface{}.
func rawMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{}
}

// portFile copies a file from the sandbox into the session's jj repo
// (Contents API). The workspace bookmark is the destination branch.
func (s *server) portFile(ctx context.Context, sc sandboxCtx, args map[string]interface{}) (string, error) {
	sandboxPath := strArg(args, "sandbox_path")
	repoPath := strArg(args, "repo_path")
	message := strArg(args, "message")
	if message == "" {
		message = "port " + sandboxPath
	}

	// 1. Read from sandbox (worker) via native file_read.
	data, err := s.sandboxFileRead(ctx, sc.cid, sandboxPath)
	if err != nil {
		return "", fmt.Errorf("port sandbox read failed: %w", err)
	}

	// 2. Write to jj-server via Contents API (base64).
	url := fmt.Sprintf("%s/api/v1/repos/%s/%s/%s/contents/%s",
		s.jj, urlPathEscape(sc.ws.org), urlPathEscape(sc.ws.repo), urlPathEscape(sc.ws.bookmark), escapePath(repoPath))
	body := map[string]interface{}{
		"content": base64Encode(string(data)),
		"message": message,
	}
	if _, err := s.httpPutJSON(ctx, url, body); err != nil {
		return "", fmt.Errorf("port write failed: %w", err)
	}
	return fmt.Sprintf("Ported '%s' to repo '%s'.", sandboxPath, repoPath), nil
}

// imageList queries the artifact registry's OCI catalog (GET /v2/_catalog)
// and returns the repository names.
func (s *server) imageList(ctx context.Context) (string, error) {
	return s.httpGetJSON(ctx, s.artifact+"/v2/_catalog")
}

// resourceRequestFromArgs extracts the nested resources arg (if any) from a
// tool argument map, tolerant of missing/malformed entries.
func resourceRequestFromArgs(args map[string]interface{}) k8s.ResourceRequest {
	var rr k8s.ResourceRequest
	raw, ok := args["resources"].(map[string]interface{})
	if !ok {
		return rr
	}
	if reqs, ok := raw["requests"].(map[string]interface{}); ok {
		rr.Requests = &k8s.ResourcePair{
			CPU:    strArg(reqs, "cpu"),
			Memory: strArg(reqs, "memory"),
		}
	}
	if limits, ok := raw["limits"].(map[string]interface{}); ok {
		rr.Limits = &k8s.ResourcePair{
			CPU:    strArg(limits, "cpu"),
			Memory: strArg(limits, "memory"),
		}
	}
	return rr
}
