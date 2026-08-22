package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// httpGetJSON fetches a URL and returns the pretty-printed JSON body.
func (s *server) httpGetJSON(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var v interface{}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", err
	}
	return toJSON(v), nil
}

// httpPostJSON POSTs a JSON body and returns the pretty-printed response.
func (s *server) httpPostJSON(ctx context.Context, url string, body interface{}) (string, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var v interface{}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", err
	}
	return toJSON(v), nil
}

// selfBase returns the HTTP base of this instance for self-invoking build.
func selfBase() string {
	return "http://127.0.0.1:" + portValue
}

var portValue = "8080"
