# ops-extension

Go single-binary extension server replacing the original Rust `executor` +
`sandbox-tools` + `builder` + `artifact-tools` services. No external binaries
(`kubectl`/`buildctl`) — it uses `client-go`, `moby/buildkit`, and
`gorilla/websocket` directly.

## Capabilities

- **NATS extension** (tools + discovery) for the agent via
  `forgejo.develop.10.199.64.20.nip.io/rucoder/extension-sdk-go`.
- **HTTP API** for the recoder-neo frontend (executor + builder surface).
- **Dynamic worker pods** via `client-go` (no `kubectl`).
- **Worker WebSocket RPC** via `gorilla/websocket`.
- **Image builds** via `moby/buildkit` (no `buildctl`).

## Dependency

`extension-sdk-go` is pulled from forgejo by git URL (not a local `replace`):

```
forgejo.develop.10.199.64.20.nip.io/rucoder/extension-sdk-go v0.1.2
```

Build with private-module env:

```bash
GOINSECURE=forgejo.develop.10.199.64.20.nip.io \
GOPRIVATE=forgejo.develop.10.199.64.20.nip.io \
go build ./...
```

## HTTP surface

Executor: `/containers` (+ `/{cid}`, `/{cid}/exec`, `/{cid}/jobs*`, `/{cid}/kill/{job_id}`),
`/sandbox/read`, `/sandbox/write`, `/deploy`, `/infra/k8s/config`.

Builder: `/images/build`, `/containerfile-templates`.

## NATS tools

**Phase 1 (sandbox)** — `bash`, `read`, `write`, `job_list`, `job_output`,
`job_wait`, `job_stdin`, `job_kill`.

**Phase 2 (artifact)** — `list-registry-packages`, `package-publish`,
`pull-git-repo`, `container-build`, `list-containerfile-templates`.

## Config

| Env | Default |
| --- | ------- |
| `RUCODER_K8S_NAMESPACE` | `temp` |
| `RUCODER_WORKER_IMAGE`  | `recoder-dev002.../rucoder-worker:dev` |
| `RUCODER_BUILDKIT_ADDR` | `tcp://rucoder-buildkitd.temp.svc.cluster.local:1234` |
| `RUCODER_REGISTRY`      | `rucoder-zot.temp.10.199.64.20.nip.io` (OCI registry host, for image tags/push) |
| `RUCODER_REGISTRY_URL`  | `http://rucoder-registry...` (package-metadata service) |
| `RUCODER_REPO_MANAGER_URL` | `http://rucoder-repo-manager...` |
| `NATS_URL`              | `nats://nats.develop.svc.cluster.local:4222` |
| `RUCODER_PORT`          | `8080` |
