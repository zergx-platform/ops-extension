package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"forgejo.develop.10.199.64.20.nip.io/zergx/ops-extension/internal/jsonwrite"
)

var _ = fmt.Sprintf

// Built-in containerfile templates (phase 2, mirror original misc.rs).
func builtinTemplates() []map[string]string {
	return []map[string]string{
		{"name": "oci", "content": "FROM scratch\nCOPY . /app\n"},
		{"name": "cargo", "content": "FROM rust:1.97-alpine AS build\nWORKDIR /app\nCOPY . .\nRUN cargo build --release\nFROM alpine:3.20\nCOPY --from=build /app/target/release/app /app\n"},
		{"name": "npm", "content": "FROM node:22-alpine\nWORKDIR /app\nCOPY . .\nRUN npm install\nCMD [\"node\", \"index.js\"]\n"},
	}
}

func (s *server) containerfileTemplates(w http.ResponseWriter, r *http.Request) {
	jsonwrite.JSON(w, http.StatusOK, map[string]interface{}{"templates": builtinTemplates()})
}

type buildBody struct {
	Org      string `json:"org"`
	Repo     string `json:"repo"`
	Bookmark string `json:"bookmark"`
	Tag      string `json:"tag"`
	// ImageTag overrides the image reference tag. It decouples the source
	// revision (bookmark) from the image tag so releases can pin immutable
	// semver tags (e.g. v0.0.1) instead of the floating :dev. When empty the
	// historical behavior of tagging by bookmark is preserved.
	ImageTag   string `json:"image_tag"`
	Dockerfile string `json:"dockerfile"`
	Push       bool   `json:"push"`
	Raw        bool   `json:"raw"`
	NoCache    bool   `json:"no_cache"`
}

// ImageRefTag returns the image tag to append to the reference, preferring an
// explicit ImageTag over the bookmark/floating default.
func (b buildBody) ImageRefTag() string {
	if b.ImageTag != "" {
		return b.ImageTag
	}
	return b.BookmarkOrDefault()
}

// ForceNoCache resolves the effective no-cache flag. The raw-content build
// path has no content-addressable context (the Dockerfile is the whole
// context), so it must always invalidate — otherwise buildkit would serve the
// previous build's output forever even when the raw content changed.
func (b buildBody) ForceNoCache() bool {
	return b.NoCache || b.Raw
}

// BookmarkOrDefault returns the bookmark or "latest" for raw builds (needed
// to form a valid image reference without a repo).
func (b buildBody) BookmarkOrDefault() string {
	if b.Bookmark == "" {
		return "latest"
	}
	return b.Bookmark
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

	// Forward to jjlab (it owns buildkitd, the repo store and the export).
	fullImage := s.artifactImageHost + "/" + b.Tag + ":" + b.ImageRefTag()
	req := map[string]interface{}{
		"org":      b.Org,
		"repo":     b.Repo,
		"bookmark": b.Bookmark,
		"raw":      b.Raw,
		"image":    fullImage,
		"export":   "push",
		"no_cache": b.ForceNoCache(),
	}
	if b.Raw {
		req["containerfile"] = b.Dockerfile
	} else if b.Dockerfile != "" {
		req["dockerfile"] = b.Dockerfile
	}
	id, err := s.opsSubmitBuild(r.Context(), req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	// Mirror the jjlab task into the local registry so /builds/{id} (status +
	// SSE log) keeps working unchanged; the poller folds jjlab state in.
	s.mirrorOpsTask(id, "build", b.Tag, fullImage)
	jsonwrite.JSON(w, http.StatusAccepted, map[string]interface{}{"ok": true, "build_id": id})
}

// mirrorOpsTask registers a local task that tracks a jjlab ops task until it
// leaves "running", copying status/result/error into the local view.
func (s *server) mirrorOpsTask(id, kind, tag, image string) {
	t := &buildTask{
		ID:        id,
		Kind:      kind,
		Tag:       tag,
		State:     "running",
		Image:     image,
		StartedAt: time.Now(),
		subs:      map[chan buildLogLine]struct{}{},
	}
	s.builds.Store(id, t)
	s.evictBuilds()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		res, err := s.opsTask(ctx, id)
		if err != nil {
			t.append(buildLogLine{Stream: kind, Line: "ERROR: " + err.Error()})
			t.setResult(image, err.Error())
			return
		}
		if res["status"] == "done" {
			if r, _ := res["result"].(string); r != "" {
				t.append(buildLogLine{Stream: kind, Line: r})
			}
			t.setResult(image, "")
			return
		}
		msg, _ := res["error"].(string)
		if msg == "" {
			msg = "jjlab task " + res["status"].(string)
		}
		t.append(buildLogLine{Stream: kind, Line: "ERROR: " + msg})
		t.setResult(image, msg)
	}()
}
