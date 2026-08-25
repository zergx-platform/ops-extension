# syntax=docker/dockerfile:1
# ops-extension: single binary with embedded Svelte SPA, built from this repo's
# own directory as the build context (no shared parent context). The Go SDK
# dependency is resolved from go.mod (forgejo module), not copied from a
# sibling directory.
ARG REGISTRY=rucoder-artifact.temp.10.199.64.20.nip.io
ARG NODE_IMAGE=26-alpine
ARG GO_IMAGE=golang:1.26-alpine

FROM ${REGISTRY}/node:${NODE_IMAGE} AS frontend
ARG HTTP_PROXY=http://mihomo.develop.svc.cluster.local:7890
ARG HTTPS_PROXY=http://mihomo.develop.svc.cluster.local:7890
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc,.nip.io,10.199.64.20,.develop.10.199.64.20.nip.io
WORKDIR /fe
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN npm install -g pnpm && (pnpm install --frozen-lockfile || pnpm install)
COPY frontend/ ./
RUN pnpm build

FROM ${REGISTRY}/${GO_IMAGE} AS build
ARG HTTP_PROXY=http://mihomo.develop.svc.cluster.local:7890
ARG HTTPS_PROXY=http://mihomo.develop.svc.cluster.local:7890
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc,.nip.io,10.199.64.20,.develop.10.199.64.20.nip.io \
    GOINSECURE=forgejo.develop.10.199.64.20.nip.io \
    GOPRIVATE=forgejo.develop.10.199.64.20.nip.io \
    GOPROXY=https://proxy.golang.org,direct
RUN apk add --no-cache git \
    && git config --global http.sslVerify false \
    && git config --global url."https://root:devpassword@forgejo.develop.10.199.64.20.nip.io/".insteadOf "https://forgejo.develop.10.199.64.20.nip.io/"
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
COPY --from=frontend /fe/dist /src/frontend/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ops-extension .

FROM scratch
COPY --from=build /out/ops-extension /ops-extension
ENTRYPOINT ["/ops-extension"]
