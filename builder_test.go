package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type tarEntry func(w *tar.Writer) error

func regular(name, body string) tarEntry {
	return func(w *tar.Writer) error {
		if err := w.WriteHeader(&tar.Header{
			Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg,
		}); err != nil {
			return err
		}
		_, err := io.WriteString(w, body)
		return err
	}
}

func dirEntry(name string) tarEntry {
	return func(w *tar.Writer) error {
		return w.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Typeflag: tar.TypeDir})
	}
}

func tarGz(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, mk := range entries {
		if err := mk(tw); err != nil {
			t.Fatalf("write tar entry: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return buf.Bytes()
}

// TestExtractTarGzZipSlip verifies that entries trying to escape the
// destination directory are rejected instead of written outside it.
func TestExtractTarGzZipSlip(t *testing.T) {
	cases := []string{
		"repo-1/../../evil.txt",
		"repo-1/sub/../../../evil2.txt",
		"/abs/evil3.txt",
		"../../evil5.txt",
	}
	for _, name := range cases {
		payload := tarGz(t, []tarEntry{regular(name, "pwn")})
		dest := t.TempDir()
		err := extractTarGz(bytes.NewReader(payload), dest, 1)
		if err == nil {
			t.Fatalf("entry %q: extract succeeded, want zip-slip rejection", name)
		}
	}
}

// TestExtractTarGzCleansInsideDotDots verifies that entries whose path merely
// contains "./" or ".." but resolves back inside the root are still accepted
// (path.Clean semantics), e.g. "repo-1/./../x" → "x" (consumed by strip).
func TestExtractTarGzCleansInsideDotDots(t *testing.T) {
	payload := tarGz(t, []tarEntry{regular("repo-1/./../evil4.txt", "ok")})
	dest := t.TempDir()
	if err := extractTarGz(bytes.NewReader(payload), dest, 1); err != nil {
		t.Fatalf("extract: %v", err)
	}
	if fileExists(filepath.Join(dest, "evil4.txt")) {
		t.Fatal("entry resolved outside the stripped root unexpectedly")
	}
}

// TestExtractTarGzNormal ensures legit archives still extract with strip.
func TestExtractTarGzNormal(t *testing.T) {
	payload := tarGz(t, []tarEntry{
		dirEntry("repo-1/"),
		regular("repo-1/src/main.go", "package main"),
	})
	dest := t.TempDir()
	if err := extractTarGz(bytes.NewReader(payload), dest, 1); err != nil {
		t.Fatalf("extract: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dest, "src", "main.go"))
	if err != nil || string(b) != "package main" {
		t.Fatalf("extracted content = %q, %v", b, err)
	}
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
