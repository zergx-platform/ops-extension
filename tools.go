package main

import (
	"context"
	"fmt"
	"strings"

	extensionsdk "forgejo.develop.10.199.64.20.nip.io/rucoder/extension-sdk-go"
)

// tools returns the phase-1 NATS tools (sandbox + container). These mirror the
// original sandbox-tools surface (kebab/snake naming preserved).
func (s *server) tools() map[string]extensionsdk.ToolSpec {
	return map[string]extensionsdk.ToolSpec{
		"bash": {
			Description: "Run a command in the sandbox (worker) using a minimal shell.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"command": map[string]interface{}{"type": "string"},
					"workdir": map[string]interface{}{"type": "string"},
				},
				"required": []string{"command"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				command := strArg(args, "command")
				if command == "" {
					return "", fmt.Errorf("bash: missing 'command'")
				}
				res, err := s.workerCommand(ctx, cid, "execute", map[string]interface{}{"command": command})
				if err != nil {
					return "", fmt.Errorf("bash failed: %w", err)
				}
				return toJSON(res), nil
			},
		},
		"read": {
			Description: "Read a file from the sandbox (worker) filesystem.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path": map[string]interface{}{"type": "string"},
				},
				"required": []string{"path"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				path := strArg(args, "path")
				res, err := s.workerCommand(ctx, cid, "execute", map[string]interface{}{
					"command": fmt.Sprintf("cat %s", shellQuote(path)),
				})
				if err != nil {
					return "", fmt.Errorf("sandbox read failed: %w", err)
				}
				return toJSON(res), nil
			},
		},
		"write": {
			Description: "Write a file into the sandbox (worker) filesystem.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path":    map[string]interface{}{"type": "string"},
					"content": map[string]interface{}{"type": "string"},
				},
				"required": []string{"path", "content"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				path := strArg(args, "path")
				content := strArg(args, "content")
				b64 := base64Encode(content)
				cmd := fmt.Sprintf("mkdir -p \"$(dirname %s)\" && echo %s | base64 -d > %s", shellQuote(path), b64, shellQuote(path))
				if _, err := s.workerCommand(ctx, cid, "execute", map[string]interface{}{"command": cmd}); err != nil {
					return "", fmt.Errorf("sandbox write failed: %w", err)
				}
				return fmt.Sprintf("Wrote sandbox file '%s'.", path), nil
			},
		},
		"job_list": {
			Description: "List background jobs in the sandbox.",
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				res, err := s.workerCommand(ctx, cid, "jobs", map[string]interface{}{})
				if err != nil {
					return "", fmt.Errorf("job_list failed: %w", err)
				}
				return toJSON(res), nil
			},
		},
		"job_output": {
			Description: "Read the output of a background job.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
				},
				"required": []string{"job_id"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				jobID := strArg(args, "job_id")
				res, err := s.workerCommand(ctx, cid, "job_output", map[string]interface{}{"job_id": jobID})
				if err != nil {
					return "", fmt.Errorf("job_output failed: %w", err)
				}
				return toJSON(res), nil
			},
		},
		"job_kill": {
			Description: "Kill a background job.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
				},
				"required": []string{"job_id"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				jobID := strArg(args, "job_id")
				res, err := s.workerCommand(ctx, cid, "kill", map[string]interface{}{"job_id": jobID})
				if err != nil {
					return "", fmt.Errorf("job_kill failed: %w", err)
				}
				return toJSON(res), nil
			},
		},
		"job_wait": {
			Description: "Wait for a background job to finish.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
				},
				"required": []string{"job_id"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				jobID := strArg(args, "job_id")
				res, err := s.workerCommand(ctx, cid, "job_wait", map[string]interface{}{"job_id": jobID, "timeout_ms": 30000})
				if err != nil {
					return "", fmt.Errorf("job_wait failed: %w", err)
				}
				return toJSON(res), nil
			},
		},
		"job_stdin": {
			Description: "Send input to the stdin of a running background job.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
					"data":   map[string]interface{}{"type": "string"},
				},
				"required": []string{"job_id", "data"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				jobID := strArg(args, "job_id")
				data := strArg(args, "data")
				res, err := s.workerCommand(ctx, cid, "job_stdin", map[string]interface{}{"job_id": jobID, "data": data})
				if err != nil {
					return "", fmt.Errorf("job_stdin failed: %w", err)
				}
				return toJSON(res), nil
			},
		},
		"list-registry-packages": {
			Description: "List packages and versions published to the registry.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"protocol": map[string]interface{}{"type": "string"},
					"name":     map[string]interface{}{"type": "string"},
				},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				return s.httpGetJSON(ctx, s.registry+"/api/v1/packages/list")
			},
		},
		"package-publish": {
			Description: "Publish a built artifact to the embedded package registry.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"protocol": map[string]interface{}{"type": "string"},
					"image":    map[string]interface{}{"type": "string"},
					"name":     map[string]interface{}{"type": "string"},
					"version":  map[string]interface{}{"type": "string"},
				},
				"required": []string{"protocol"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				body := map[string]interface{}{
					"protocol": strArg(args, "protocol"),
					"image":    strArg(args, "image"),
					"name":     strArg(args, "name"),
					"version":  strArg(args, "version"),
				}
				return s.httpPostJSON(ctx, s.registry+"/api/v1/packages/publish", body)
			},
		},
		"pull-git-repo": {
			Description: "Pull a git repository from a remote URL into local storage.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"git_url": map[string]interface{}{"type": "string"},
					"branch":  map[string]interface{}{"type": "string"},
				},
				"required": []string{"git_url"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				body := map[string]interface{}{
					"git_url": strArg(args, "git_url"),
					"branch":  strArg(args, "branch"),
				}
				return s.httpPostJSON(ctx, s.registry+"/api/v1/packages/pull-git", body)
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
				},
				"required": []string{"dockerfile_path", "tag"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				payload := map[string]interface{}{
					"org":        strArg(args, "_org"),
					"repo":       strArg(args, "_repo"),
					"bookmark":   strArg(args, "_branch"),
					"dockerfile": strArg(args, "dockerfile_path"),
					"tag":        strArg(args, "tag"),
					"context":    strArg(args, "context"),
					"push":       true,
				}
				res, err := s.httpPostJSON(ctx, selfBase()+"/api/v1/images/build", payload)
				if err != nil {
					return "", fmt.Errorf("container-build failed: %w", err)
				}
				return res, nil
			},
		},
		"list-containerfile-templates": {
			Description: "List built-in Containerfile/build templates.",
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				return toJSON(builtinTemplates()), nil
			},
		},
		"edit": {
			Description: "Replace or insert lines in a sandbox file by line numbers.",
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
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				cid := strArg(args, "_container_id")
				path := strArg(args, "path")
				startLine := intArg64(args, "start_line", 0)
				endLine := intArg64(args, "end_line", 0)
				content := strArg(args, "content")
				return s.sandboxEdit(ctx, cid, path, startLine, endLine, content)
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
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				image := strArg(args, "image")
				name := strArg(args, "name")
				if name == "" {
					name = "app"
				}
				if err := s.k8s.EnsureDeployment(ctx, name, image, 1, 8080, nil); err != nil {
					return "", fmt.Errorf("container-deploy failed: %w", err)
				}
				return fmt.Sprintf("Deployed '%s' from %s.", name, image), nil
			},
		},
		"port": {
			Description: "Copy a file from the sandbox into the repo (sandbox -> jj repo).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"sandbox_path": map[string]interface{}{"type": "string"},
					"repo_path":    map[string]interface{}{"type": "string"},
					"message":      map[string]interface{}{"type": "string"},
				},
				"required": []string{"sandbox_path", "repo_path"},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				return s.portFile(ctx, args)
			},
		},
		"image-list": {
			Description: "List container images in the OCI registry.",
			InputSchema: map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
			Execute: func(ctx context.Context, args map[string]interface{}, callID string) (string, error) {
				return s.imageList(ctx)
			},
		},
	}
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

// trimUnused keeps imports valid; remove if linters complain.
var _ = strings.TrimSpace

// sandboxEdit implements edit as read-modify-write over the worker sandbox.
func (s *server) sandboxEdit(ctx context.Context, cid, path string, startLine, endLine int64, content string) (string, error) {
	read, err := s.workerCommand(ctx, cid, "execute", map[string]interface{}{
		"command": fmt.Sprintf("cat %s", shellQuote(path)),
	})
	if err != nil {
		return "", fmt.Errorf("sandbox edit read failed: %w", err)
	}
	current := strVal(rawMap(read))
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
	b64 := base64Encode(newContent)
	cmd := fmt.Sprintf("mkdir -p \"$(dirname %s)\" && echo %s | base64 -d > %s", shellQuote(path), b64, shellQuote(path))
	if _, err := s.workerCommand(ctx, cid, "execute", map[string]interface{}{"command": cmd}); err != nil {
		return "", fmt.Errorf("sandbox edit write failed: %w", err)
	}
	return fmt.Sprintf("Edited sandbox file '%s'.", path), nil
}

// rawMap coerces the workerCommand result to map[string]interface{} without
// importing encoding/json here (already available via util).
func rawMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{}
}

// strVal extracts a string field.
func strVal(v map[string]interface{}) string {
	if s, ok := v["content"].(string); ok {
		return s
	}
	if s, ok := v["output"].(string); ok {
		return s
	}
	if s, ok := v["result"].(string); ok {
		return s
	}
	return ""
}

// portFile copies a file from the sandbox into the jj repo (Contents API).
func (s *server) portFile(ctx context.Context, args map[string]interface{}) (string, error) {
	cid := strArg(args, "_container_id")
	sandboxPath := strArg(args, "sandbox_path")
	repoPath := strArg(args, "repo_path")
	message := strArg(args, "message")
	if message == "" {
		message = "port " + sandboxPath
	}

	// 1. Read from sandbox (worker).
	read, err := s.workerCommand(ctx, cid, "execute", map[string]interface{}{
		"command": fmt.Sprintf("cat %s", shellQuote(sandboxPath)),
	})
	if err != nil {
		return "", fmt.Errorf("port sandbox read failed: %w", err)
	}
	content := strVal(rawMap(read))

	// 2. Write to jj-server via Contents API (base64).
	org := strArg(args, "_org")
	repo := strArg(args, "_repo")
	branch := strArg(args, "_branch")
	if branch == "" {
		branch = "main"
	}
	url := fmt.Sprintf("%s/api/v1/repos/%s/%s/%s/contents/%s",
		s.builder, urlPathEscape(org), urlPathEscape(repo), urlPathEscape(branch), escapePath(repoPath))
	body := map[string]interface{}{
		"content": base64Encode(content),
		"message": message,
	}
	if _, err := s.httpPutJSON(ctx, url, body); err != nil {
		return "", fmt.Errorf("port write failed: %w", err)
	}
	return fmt.Sprintf("Ported '%s' to repo '%s'.", sandboxPath, repoPath), nil
}

// imageList queries the OCI registry catalog (GET /v2/_catalog) and returns the
// repository names. registryHost is the OCI store (e.g. rucoder-zot...).
func (s *server) imageList(ctx context.Context) (string, error) {
	url := fmt.Sprintf("https://%s/v2/_catalog", s.registryHost)
	return s.httpGetJSON(ctx, url)
}
