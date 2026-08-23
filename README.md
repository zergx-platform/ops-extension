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

**Phase 1 (sandbox)** — `bash`, `read`, `write`, `edit`, `job_list`, `job_output`,
`job_wait`, `job_stdin`, `job_kill`, `port`.

**Phase 2 (artifact)** — `list-registry-packages`, `package-publish`,
`pull-git-repo`, `container-build`, `container-deploy`, `image-list`,
`list-containerfile-templates`.

### package-publish (containerfile-based)

Publishing is done by running the protocol's official CLI inside a buildkit
build (no image export, only RUN side effects). The repo checkout from
jj-server (`GET /api/v1/repos/{org}/{repo}/{rev}/archive`) is the build
context; base images are pulled through the artifact OCI pull-through proxy,
so toolchain layers stay cached on the shared buildkitd.

- Manifest-driven protocols read name/version from the package manifest:
  `npm` (npm publish), `pypi` (python -m build + twine), `cargo`
  (cargo publish, sparse index), `rubygems` (gem build/push), `conan`
  (conan 2 create/upload), `pub` (dart pub publish), `helm`
  (helm package → chart-museum API).
- Explicit `name`/`version` required: `go` (module zip), `hex` (tar),
  `composer` (zip), `maven` (groupId + jar), `swift` (`scope.name` +
  archive-source zip), `generic` (raw `file`).
- `dockerfile_path` overrides the built-in template with a containerfile from
  the repo.

Cache-busting: every publish step references `$PUBLISH_TS` (fresh per
invocation) so a same-version re-publish never hits the RUN cache while
toolchain layers remain cached. Build failures return the build log tail so
the agent sees the CLI error.

## Config

| Env | Default |
| --- | ------- |
| `RUCODER_K8S_NAMESPACE` | `temp` |
| `RUCODER_WORKER_IMAGE`  | `recoder-dev002.../rucoder-worker:dev` |
| `RUCODER_BUILDKIT_ADDR` | `tcp://rucoder-buildkitd.temp.svc.cluster.local:1234` |
| `RUCODER_ARTIFACT_URL`  | `http://rucoder-artifact.temp.svc.cluster.local:80` (packages + OCI + metadata) |
| `RUCODER_ARTIFACT_TOKEN`| *(empty — anonymous)* token for artifact write auth (Bearer / X-NuGet-ApiKey / npm _authToken per protocol) |
| `RUCODER_JJ_SERVER_URL` | `http://rucoder-jj-server.temp.svc.cluster.local:80` (archive + contents + clone) |
| `NATS_URL`              | `nats://nats.develop.svc.cluster.local:4222` |
| `RUCODER_PORT`          | `8080` |

> `RUCODER_REPO_MANAGER_URL` is still honored as a fallback for `RUCODER_JJ_SERVER_URL`.
> The old `RUCODER_REGISTRY` / `RUCODER_REGISTRY_URL` (zot + rucoder-registry) are gone.
