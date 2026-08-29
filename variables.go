package main

import (
	"context"

	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/extension"

	"forgejo.develop.10.199.64.20.nip.io/zergx/ops-extension/internal/k8s"
)

// publishSandboxVars projects the session's sandbox state into the shared KV
// (`vars.ops.{token}.*`) after the worker pod is ensured. Values are derived
// from the k8s state (the single source of truth); the KV copy is a cache.

func (s *server) publishSandboxVars(ctx context.Context, sid string, info k8s.ContainerInfo) {
	if s.ext == nil {
		return
	}
	_ = s.ext.SetSessionVariable(ctx, sid, "sandbox-id", info.ContainerID)
	_ = s.ext.SetSessionVariable(ctx, sid, "sandbox-status", info.Status)
}

// resolveSandboxStatus is the authoritative lazy resolver for `vars.ops.sandbox-status`.
func (s *server) resolveSandboxStatus(ctx context.Context, sessionName string) (string, error) {
	key := sessionKey(sessionName)
	info, err := s.k8s.FindContainer(ctx, key)
	if err != nil {
		return "", err
	}
	if info.Status == "" {
		return "not-created", nil
	}
	return info.Status, nil
}

// resolveSandboxID is the authoritative lazy resolver for `vars.ops.sandbox-id`.
// Same k8s source of truth as sandbox-status; the KV projection in
// publishSandboxVars is just a cache.
func (s *server) resolveSandboxID(ctx context.Context, sessionName string) (string, error) {
	key := sessionKey(sessionName)
	info, err := s.k8s.FindContainer(ctx, key)
	if err != nil {
		return "", err
	}
	if info.ContainerID == "" {
		return "not-created", nil
	}
	return info.ContainerID, nil
}

// clearSandboxVars removes a session's projected sandbox variables.
func (s *server) clearSandboxVars(ctx context.Context, sessionName string) {
	if s.ext == nil {
		return
	}
	_ = s.ext.DeleteSessionVariables(ctx, sessionName)
}

var _ = extension.VariableSpec{}
