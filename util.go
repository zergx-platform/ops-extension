package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// param extracts a URL param from a chi-routed request.
func param(r *http.Request, key string) string {
	return chi.URLParam(r, key)
}

func parseIntOr(s string, def int64) int64 {
	if s == "" {
		return def
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func marshalIndent(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}
