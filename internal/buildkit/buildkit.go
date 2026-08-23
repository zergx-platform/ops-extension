// Package buildkit wraps the moby/buildkit client to build + push images via a
// remote buildkitd, replacing buildctl (no external binary).
package buildkit

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

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

// Run executes a containerfile with buildkit but exports nothing — used for
// side-effect stages such as in-container package publishing. Build args are
// passed as dockerfile build-args; the publish templates reference them via
// ARG/ENV so they reach the RUN commands.
//
// On failure the tail of the build log (vertex errors + stdout/stderr) is
// attached so callers can see why e.g. `npm publish` failed.
func (c *Client) Run(ctx context.Context, contextDir, dockerfile string, buildArgs map[string]string) error {
	dfPath := dockerfile
	if dfPath == "" {
		dfPath = "Dockerfile"
	}

	cxtMount, err := fsutil.NewFS(contextDir)
	if err != nil {
		return fmt.Errorf("local mount for %s: %w", contextDir, err)
	}
	dockerfileMount, err := fsutil.NewFS(contextDir)
	if err != nil {
		return fmt.Errorf("local mount for dockerfile: %w", err)
	}

	cl, err := client.New(ctx, c.addr)
	if err != nil {
		return err
	}
	defer cl.Close()

	frontendAttrs := map[string]string{"filename": filepath.Base(dfPath)}
	for k, v := range buildArgs {
		frontendAttrs["build-arg:"+k] = v
	}

	solveOpt := client.SolveOpt{
		LocalMounts: map[string]fsutil.FS{
			"context":    cxtMount,
			"dockerfile": dockerfileMount,
		},
		Frontend:      "dockerfile.v0",
		FrontendAttrs: frontendAttrs,
		// No Exports: the result is discarded, only RUN side effects matter.
	}

	ch := make(chan *client.SolveStatus)
	done := make(chan struct{})
	lg := newLogCollector(200, 32*1024)
	go func() {
		defer close(done)
		for st := range ch {
			lg.consume(st)
		}
	}()

	if _, err := cl.Solve(ctx, nil, solveOpt, ch); err != nil {
		<-done
		return fmt.Errorf("build failed: %w\n--- build log (tail) ---\n%s", err, lg.tail())
	}
	<-done
	return nil
}

// logCollector keeps the tail of a build's vertex errors and log lines,
// bounded in both line count and total bytes.
type logCollector struct {
	mu       sync.Mutex
	maxLines int
	maxBytes int
	total    int
	lines    []string
}

func newLogCollector(maxLines, maxBytes int) *logCollector {
	return &logCollector{maxLines: maxLines, maxBytes: maxBytes}
}

func (l *logCollector) add(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	line = strings.TrimRight(line, "\n")
	if line == "" {
		return
	}
	l.total += len(line)
	if l.total > l.maxBytes {
		return
	}
	l.lines = append(l.lines, line)
	if len(l.lines) > l.maxLines {
		l.lines = l.lines[len(l.lines)-l.maxLines:]
	}
}

func (l *logCollector) consume(st *client.SolveStatus) {
	for _, v := range st.Vertexes {
		if v.Error != "" {
			l.add("ERROR " + v.Name + ": " + v.Error)
		}
	}
	for _, entry := range st.Logs {
		l.add(string(entry.Data))
	}
}

func (l *logCollector) tail() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
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
