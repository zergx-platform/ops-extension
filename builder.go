package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

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
}

func (s *server) buildImage(w http.ResponseWriter, r *http.Request) {
	var b buildBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if b.Org == "" || b.Repo == "" || b.Bookmark == "" {
		writeErr(w, http.StatusBadRequest, "org/repo/bookmark required")
		return
	}
	if b.Tag == "" {
		b.Tag = "latest"
	}

	// 1. Fetch the workspace tar from repo-manager.
	archiveURL := fmt.Sprintf("%s/api/v1/repos/%s/%s/%s/archive",
		s.builder, b.Org, b.Repo, b.Bookmark)

	ctx := r.Context()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, archiveURL, nil)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, fmt.Sprintf("fetch context: %v", err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		writeErr(w, http.StatusBadGateway, fmt.Sprintf("fetch context: %d", resp.StatusCode))
		return
	}

	// 2. Expand the tar into a temp dir.
	tmpDir := filepath.Join(os.TempDir(), "ops-build-"+uuid.NewString())
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer os.RemoveAll(tmpDir)
	if err := extractTarGz(resp.Body, tmpDir); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("untar: %v", err))
		return
	}

	// 3. Full registry-qualified tag (against the OCI registry host).
	fullTag := fmt.Sprintf("%s/%s:%s", s.registryHost, b.Tag, b.Bookmark)
	if b.Dockerfile == "" {
		b.Dockerfile = "Dockerfile"
	}

	// 4. Build + push via moby/buildkit.
	imageID, err := s.buildkit.Build(ctx, tmpDir, b.Dockerfile, fullTag)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Sprintf("build failed: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":         true,
		"image_id":   imageID,
		"image":      fullTag,
		"image_name": fmt.Sprintf("%s:%s", b.Tag, b.Bookmark),
		"pushed":     b.Push,
	})
}

func extractTarGz(r interface{ Read([]byte) (int, error) }, dest string) error {
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
		target := filepath.Join(dest, hdr.Name)
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
