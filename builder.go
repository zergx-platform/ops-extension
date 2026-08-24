package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// Built-in containerfile templates (phase 2, mirror original misc.rs).
func builtinTemplates() []map[string]string {
	return []map[string]string{
		{"name": "oci", "content": "FROM scratch\nCOPY . /app\n"},
		{"name": "cargo", "content": "FROM rust:1.97-alpine AS build\nWORKDIR /app\nCOPY . .\nRUN cargo build --release\nFROM alpine:3.20\nCOPY --from=build /app/target/release/app /app\n"},
		{"name": "npm", "content": "FROM node:22-alpine\nWORKDIR /app\nCOPY . .\nRUN npm install\nCMD [\"node\", \"index.js\"]\n"},
	}
}

func (s *server) containerfileTemplates(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"templates": builtinTemplates()})
}

type buildBody struct {
	Org        string `json:"org"`
	Repo       string `json:"repo"`
	Bookmark   string `json:"bookmark"`
	Tag        string `json:"tag"`
	Dockerfile string `json:"dockerfile"`
	Push       bool   `json:"push"`
	Raw        bool   `json:"raw"`
}

// BookmarkOrDefault returns the bookmark or "latest" for raw builds (needed
// to form a valid image reference without a repo).
func (b buildBody) BookmarkOrDefault() string {
	if b.Bookmark == "" {
		return "latest"
	}
	return b.Bookmark
}

// fetchRepoArchive downloads the tar.gz of org/repo@rev from jj-server into a
// fresh temp dir and returns its path (caller must RemoveAll it). The archive
// has a single top-level "{repo}-{rev}" directory which is stripped, so build
// contexts have the repo files at the root.
func (s *server) fetchRepoArchive(ctx context.Context, org, repo, rev string) (string, error) {
	archiveURL := fmt.Sprintf("%s/api/v1/repos/%s/%s/%s/archive",
		s.jj, urlPathEscape(org), urlPathEscape(repo), urlPathEscape(rev))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, archiveURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch context: %d", resp.StatusCode)
	}

	tmpDir := filepath.Join(os.TempDir(), "ops-build-"+uuid.NewString())
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return "", err
	}
	if err := extractTarGz(resp.Body, tmpDir, 1); err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("untar: %w", err)
	}
	return tmpDir, nil
}

func (s *server) buildImage(w http.ResponseWriter, r *http.Request) {
	var b buildBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}

	if !b.Raw {
		if b.Org == "" || b.Repo == "" || b.Bookmark == "" {
			writeErr(w, http.StatusBadRequest, "org/repo/bookmark required")
			return
		}
	}
	if b.Tag == "" {
		b.Tag = "latest"
	}
	if b.Raw && strings.TrimSpace(b.Dockerfile) == "" {
		writeErr(w, http.StatusBadRequest, "raw build requires dockerfile content")
		return
	}

	id := s.startBuildTask(b.Tag, b)
	writeJSON(w, http.StatusAccepted, map[string]interface{}{"ok": true, "build_id": id})
}

// extractTarGz expands a tar.gz stream into dest, optionally stripping the
// first `strip` path components from every entry (like tar --strip-components).
func extractTarGz(r interface{ Read([]byte) (int, error) }, dest string, strip int) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
		name := hdr.Name
		// Normalize: tar paths may carry "./" prefixes.
		name = strings.TrimPrefix(name, "./")
		parts := strings.Split(name, "/")
		if strip > 0 {
			if len(parts) <= strip {
				continue // the stripped top-level entry itself
			}
			parts = parts[strip:]
		}
		target := filepath.Join(append([]string{dest}, parts...)...)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			if _, err := ioCopy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
	}
}

func ioCopy(dst interface{ Write([]byte) (int, error) }, src interface{ Read([]byte) (int, error) }) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return written, werr
			}
			written += int64(n)
		}
		if err != nil {
			if err.Error() == "EOF" {
				return written, nil
			}
			return written, err
		}
	}
}
