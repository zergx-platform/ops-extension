package main

import (
	"context"
	"fmt"
	"strings"

	extensionsdk "rucoder-agent/extension-sdk-go"
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
	}
}

func strArg(args map[string]interface{}, k string) string {
	if v, ok := args[k].(string); ok {
		return v
	}
	return ""
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
