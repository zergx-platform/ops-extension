package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	extensionsdk "forgejo.develop.10.199.64.20.nip.io/rucoder/extension-sdk-go"

	"rucoder-agent/ops-extension/internal/worker"
)

// tools returns the NATS tool set. Sandbox tools are session-scoped: the
// agent injects `_session` ("org:repo:bookmark"); ops-extension resolves the
// workspace via jj-server, lazily creates/reuses the session's worker pod,
// and syncs the repo tree into it (overlay-only, sandbox-only files are
// never deleted) before running.
func (s *server) tools() map[string]extensionsdk.ToolSpec {
	return map[string]extensionsdk.ToolSpec{
		"sandbox-run": {
			Description: "Run a shell command in the session sandbox. The workspace is synced to the repo bookmark head first (repo files refreshed; sandbox-only files kept).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"command": map[string]interface{}{"type": "string"},
					"workdir": map[string]interface{}{"type": "string"},
				},
				"required": []string{"command"},
			},
			Streaming: true,
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, emit func(string)) (string, map[string]interface{}, error) {
				command := strArg(args, "command")
				if command == "" {
					return "", nil, fmt.Errorf("sandbox-run: missing 'command'")
				}
				sc, err := s.ensureSandbox(ctx, args, true)
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

				// Every command is a streamed job: subscribe to the per-job SSE
				// stream, emit output as deltas, and return a natural-language
				// final once completed.
				done, err := worker.StreamJobOutput(ctx, workerURL, res.JobID, emit)
				if err != nil {
					return "", nil, fmt.Errorf("sandbox-run stream failed: %w", err)
				}
				content := fmt.Sprintf("命令执行完成（job %s, exit %d）", res.JobID, done.ExitCode)
				if done.Stdout != "" {
					content += "\n" + done.Stdout
				}
				if done.Stderr != "" {
					content += "\n[stderr]\n" + done.Stderr
				}
				return content, map[string]interface{}{
					"job_id":    res.JobID,
					"exit_code": done.ExitCode,
				}, nil
			},
		},
		"sandbox-read": {
			Description: "Read a file from the session sandbox filesystem (synced to repo first).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path": map[string]interface{}{"type": "string"},
				},
				"required": []string{"path"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				path := strArg(args, "path")
				sc, err := s.ensureSandbox(ctx, args, true)
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
			Description: "Write a file into the session sandbox filesystem (no sync first — never clobbers in-progress edits).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path":    map[string]interface{}{"type": "string"},
					"content": map[string]interface{}{"type": "string"},
				},
				"required": []string{"path", "content"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				path := strArg(args, "path")
				sc, err := s.ensureSandbox(ctx, args, false)
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
			Description: "Replace or insert lines in a sandbox file by line numbers (synced to repo first).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"start_line": map[string]interface{}{"type": "integer"},
					"end_line":   map[string]interface{}{"type": "integer"},
					"path":       map[string]interface{}{"type": "string"},
					"content":    map[string]interface{}{"type": "string"},
				},
				"required": []string{"path", "start_line", "end_line"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				path := strArg(args, "path")
				startLine := intArg64(args, "start_line", 0)
				endLine := intArg64(args, "end_line", 0)
				content := strArg(args, "content")
				sc, err := s.ensureSandbox(ctx, args, true)
				if err != nil {
					return "", nil, err
				}
				v, err := s.sandboxEdit(ctx, sc.cid, path, startLine, endLine, content)
				return v, nil, err
			},
		},
		"sandbox-job-list": {
			Description: "List jobs (commands) in the session sandbox.",
			InputSchema: map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, false)
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
			Description: "Read output of a sandbox job (supports offset/limit/grep).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
					"start":  map[string]interface{}{"type": "integer"},
					"end":    map[string]interface{}{"type": "integer"},
					"grep":   map[string]interface{}{"type": "string"},
				},
				"required": []string{"job_id"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, false)
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
			Description: "Wait for a sandbox job to finish.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id":     map[string]interface{}{"type": "string"},
					"timeout_ms": map[string]interface{}{"type": "integer"},
				},
				"required": []string{"job_id"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, false)
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
			Description: "Send stdin data to a running sandbox job.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
					"data":   map[string]interface{}{"type": "string"},
				},
				"required": []string{"job_id", "data"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, false)
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
			Description: "Kill a sandbox job.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
				},
				"required": []string{"job_id"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, false)
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
			Description: "Copy a file from the sandbox into the session's repo (bookmark moves; the change syncs back on the next sandbox tool call).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"sandbox_path": map[string]interface{}{"type": "string"},
					"repo_path":    map[string]interface{}{"type": "string"},
					"message":      map[string]interface{}{"type": "string"},
				},
				"required": []string{"sandbox_path", "repo_path"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				sc, err := s.ensureSandbox(ctx, args, false)
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
			Description: "Build a container image from a Containerfile in the repo using the remote buildkit builder.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"dockerfile_path": map[string]interface{}{"type": "string"},
					"tag":             map[string]interface{}{"type": "string"},
					"context":         map[string]interface{}{"type": "string"},
					"no_cache":        map[string]interface{}{"type": "boolean"},
				},
				"required": []string{"dockerfile_path", "tag"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				ws, _, err := s.resolveWorkspace(ctx, args)
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
			Description: "Deploy a container image as a Kubernetes deployment.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"image": map[string]interface{}{"type": "string"},
					"name":  map[string]interface{}{"type": "string"},
				},
				"required": []string{"image"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				image := strArg(args, "image")
				name := strArg(args, "name")
				if name == "" {
					name = "app"
				}
				// Deployments belong to the calling session (if one is given),
				// so GET /sandboxes/{session} can list them.
				session := ""
				if sid := strArg(args, "_session"); sid != "" {
					session = sid
				} else if org := strArg(args, "org"); org != "" {
					bm := strArg(args, "bookmark")
					if bm == "" {
						bm = "main"
					}
					session = org + ":" + strArg(args, "repo") + ":" + bm
				}
				if err := s.k8s.EnsureDeployment(ctx, name, image, 1, 8080, nil, session); err != nil {
					return "", nil, fmt.Errorf("container-deploy failed: %w", err)
				}
				return fmt.Sprintf("Deployed '%s' from %s.", name, image), nil, nil
			},
		},
		"image-list": {
			Description: "List container images in the OCI registry.",
			InputSchema: map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				v, err := s.imageList(ctx)
				return v, nil, err
			},
		},
		"package-publish": {
			Description: "Publish the repo checkout as a package. Runs the protocol's official CLI inside a containerfile build (buildkit, no image export). Protocols: npm,pypi,cargo,rubygems,helm,nuget,maven,go,hex,composer,generic,conan,pub,swift. Manifest-driven protocols (npm/pypi/cargo/...) read name+version from the package manifest; go/hex/composer/maven/swift/generic need explicit name+version (+file for generic).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"protocol":        map[string]interface{}{"type": "string"},
					"org":             map[string]interface{}{"type": "string"},
					"repo":            map[string]interface{}{"type": "string"},
					"bookmark":        map[string]interface{}{"type": "string"},
					"name":            map[string]interface{}{"type": "string"},
					"version":         map[string]interface{}{"type": "string"},
					"file":            map[string]interface{}{"type": "string"},
					"dockerfile_path": map[string]interface{}{"type": "string"},
				},
				"required": []string{"protocol"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				protocol := strArg(args, "protocol")
				// Explicit org/repo win; else resolve the session workspace.
				org, repo, bookmark := strArg(args, "org"), strArg(args, "repo"), strArg(args, "bookmark")
				if org == "" || repo == "" {
					ws, _, err := s.resolveWorkspace(ctx, args)
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
			Description: "List packages and versions stored in the artifact registry (all protocols).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"protocol": map[string]interface{}{"type": "string"},
					"name":     map[string]interface{}{"type": "string"},
				},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				v, err := s.httpGetJSON(ctx, s.artifact+"/pkgs/system/packages")
				return v, nil, err
			},
		},
		"list-containerfile-templates": {
			Description: "List built-in Containerfile/build templates.",
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
				return toJSON(builtinTemplates()), nil, nil
			},
		},
		"pull-git-repo": {
			Description: "Clone a remote git repository into jj-server local storage (org 'external').",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"git_url": map[string]interface{}{"type": "string"},
					"org":     map[string]interface{}{"type": "string"},
				},
				"required": []string{"git_url"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, _ func(string)) (string, map[string]interface{}, error) {
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
		// insert before startLine
		insertAt := int(startLine)
		if insertAt > len(lines) {
			insertAt = len(lines)
		}
		v := append([]string{}, lines[:insertAt]...)
		v = append(v, newLines...)
		v = append(v, lines[insertAt:]...)
		lines = v
	} else {
		sIdx := int(startLine)
		eIdx := int(endLine + 1)
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
