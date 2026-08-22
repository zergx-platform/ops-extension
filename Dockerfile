# Build both the SDK and ops-extension from a shared parent context.
FROM golang:1.26-alpine AS build
ARG HTTP_PROXY=http://mihomo.develop.svc.cluster.local:7890
ARG HTTPS_PROXY=http://mihomo.develop.svc.cluster.local:7890
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc,.nip.io \
    GOINSECURE=forgejo.develop.10.199.64.20.nip.io \
    GOPRIVATE=forgejo.develop.10.199.64.20.nip.io \
    GOPROXY=https://proxy.golang.org,direct
RUN apk add --no-cache git \
    && git config --global http.sslVerify false \
    && git config --global url."https://root:devpassword@forgejo.develop.10.199.64.20.nip.io/".insteadOf "https://forgejo.develop.10.199.64.20.nip.io/"
WORKDIR /src
COPY extension-sdk-go ./extension-sdk-go
COPY ops-extension ./ops-extension
WORKDIR /src/ops-extension
RUN CGO_ENABLED=0 go build -o /out/ops-extension .

FROM scratch
COPY --from=build /out/ops-extension /ops-extension
ENTRYPOINT ["/ops-extension"]
