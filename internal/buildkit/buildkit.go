// Package buildkit wraps the moby/buildkit client to build + push images via a
// remote buildkitd, replacing buildctl (no external binary).
package buildkit

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/moby/buildkit/client"
	"github.com/tonistiigi/fsutil"
)

// Client is a thin wrapper over moby/buildkit's client.
type Client struct {
	addr string
}

// New returns a buildkit client for a remote buildkitd address.
func New(addr string) *Client {
	return &Client{addr: addr}
}

// Build builds `fullTag` from a context directory using the dockerfile.v0
// frontend and pushes it to the registry (push=true image exporter).
func (c *Client) Build(ctx context.Context, contextDir, dockerfile, fullTag string) (string, error) {
	frontend := "dockerfile.v0"

	// dockerfile path: original API carries a path relative to context root.
	dfPath := dockerfile
	if dfPath == "" {
		dfPath = "Dockerfile"
	}

	cxtMount, err := fsutil.NewFS(contextDir)
	if err != nil {
		return "", fmt.Errorf("local mount for %s: %w", contextDir, err)
	}

	// The dockerfile mount points at the directory containing the requested
	// dockerfile (the original worker expands the tar so the Dockerfile sits at
	// the repo root; keep it simple and mount the context dir itself).
	dockerfileMount, err := fsutil.NewFS(contextDir)
	if err != nil {
		return "", fmt.Errorf("local mount for dockerfile: %w", err)
	}

	cl, err := client.New(ctx, c.addr)
	if err != nil {
		return "", err
	}
	defer cl.Close()

	solveOpt := client.SolveOpt{
		Exports: []client.ExportEntry{
			{
				Type: "image",
				Attrs: map[string]string{
					"name":           fullTag,
					"push":           "true",
					"name-canonical": "",
				},
			},
		},
		LocalMounts: map[string]fsutil.FS{
			"context":    cxtMount,
			"dockerfile": dockerfileMount,
		},
		Frontend:      frontend,
		FrontendAttrs: map[string]string{"filename": filepath.Base(dfPath)},
	}

	ch := make(chan *client.SolveStatus)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
			// drain progress; progressui rendering is optional for the server
		}
	}()

	if _, err := cl.Solve(ctx, nil, solveOpt, ch); err != nil {
		return "", err
	}
	<-done
	return fullTag, nil
}

// Ping reports whether the buildkitd is reachable.
func (c *Client) Ping(ctx context.Context) bool {
	cl, err := client.New(ctx, c.addr)
	if err != nil {
		return false
	}
	defer cl.Close()
	_, err = cl.ListWorkers(ctx)
	return err == nil
}

var _ = io.Discard
var _ = os.Stderr
