# ops-extension

Go single-binary extension server replacing the original `executor` +
`sandbox-tools` + (phase 2) `builder` + `artifact-tools` Rust services.

## What it does

- **NATS extension** (tools + discovery) for the agent, via
  `rucoder-agent/extension-sdk-go`.
- **HTTP API** for the recoder-neo frontend (executor surface).
- **Dynamic worker pods**: uses `client-go` to start/stop sandbox pods — no
  `kubectl` binary.
- **Worker WebSocket RPC**: `execute` / `jobs` / `file_*` / `kill` etc. —
  the worker stays a separate service running inside each sandbox pod.

## Build

```bash
# extension-sdk-go must be checked out as a sibling directory.
go build ./...
```

The dependency is a local module (`replace` directive):

```go
replace rucoder-agent/extension-sdk-go => ../extension-sdk-go
```

For a self-contained CI build, vendor the SDK or build from a parent
workspace. The `Dockerfile` assumes both repos are in the same build context.

## HTTP surface (executor parity)

| Method | Path |
| ------ | ---- |
| GET    | `/api/v1/health` |
| GET/POST | `/api/v1/containers` |
| DELETE | `/api/v1/containers/{cid}` |
| POST   | `/api/v1/containers/{cid}/exec` |
| GET    | `/api/v1/containers/{cid}/jobs` |
| GET    | `/api/v1/containers/{cid}/jobs/{job_id}/output` |
| POST   | `/api/v1/containers/{cid}/jobs/{job_id}/wait` |
| POST   | `/api/v1/containers/{cid}/jobs/{job_id}/stdin` |
| POST   | `/api/v1/containers/{cid}/kill/{job_id}` |
| POST   | `/api/v1/sandbox/read` |
| POST   | `/api/v1/sandbox/write` |
| POST   | `/api/v1/deploy` |
| GET    | `/api/v1/infra/k8s/config` |

## NATS tools (phase 1)

`bash`, `read`, `write`, `job_list`, `job_output`, `job_wait`, `job_stdin`,
`job_kill`.

## Config

| Env | Default |
| --- | ------- |
| `RUCODER_K8S_NAMESPACE` | `temp` |
| `RUCODER_WORKER_IMAGE` | `recoder-dev002.../rucoder-worker:dev` |
| `NATS_URL` | `nats://nats.develop.svc.cluster.local:4222` |
| `RUCODER_PORT` | `8080` |
