# Build both the SDK and ops-extension from a shared parent context.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY extension-sdk-go ./extension-sdk-go
COPY ops-extension ./ops-extension
WORKDIR /src/ops-extension
RUN CGO_ENABLED=0 go build -o /out/ops-extension .

FROM scratch
COPY --from=build /out/ops-extension /ops-extension
ENTRYPOINT ["/ops-extension"]
