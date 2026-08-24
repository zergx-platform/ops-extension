// Package worker implements the WebSocket RPC client for the sandbox worker.
package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// wsReadTimeout must exceed the worker's job_wait cap (60s) by enough margin
// that a job_wait response always arrives before this deadline. See
// worker-go rpc.go jobWaitMaxMs.
const wsReadTimeout = 65 * time.Second

// CommandOnce opens a WS connection, sends one RPC command, and awaits the
// matching response. Mirrors executor's channel::command_once.
func CommandOnce(ctx context.Context, wsURL, method string, params map[string]interface{}) (interface{}, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("ws connect %s: %w", wsURL, err)
	}
	defer conn.Close()

	id := nextID()
	req := map[string]interface{}{"id": id, "method": method, "params": params}
	if err := conn.WriteJSON(req); err != nil {
		return nil, fmt.Errorf("ws send: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
	for {
		var v map[string]interface{}
		if err := conn.ReadJSON(&v); err != nil {
			return nil, fmt.Errorf("ws recv: %w", err)
		}
		if vid, ok := v["id"].(float64); ok && uint64(vid) == id {
			if e, ok := v["error"].(string); ok && e != "" {
				return nil, fmt.Errorf("%s", e)
			}
			return v["result"], nil
		}
	}
}

var idCounter atomic.Uint64

func nextID() uint64 {
	return idCounter.Add(1)
}

// ToWsURL converts a worker http URL to its websocket form.
func ToWsURL(workerURL string) string {
	switch {
	case hasPrefix(workerURL, "https://"):
		return "wss://" + workerURL[len("https://"):] + "/ws"
	case hasPrefix(workerURL, "http://"):
		return "ws://" + workerURL[len("http://"):] + "/ws"
	default:
		return "ws://" + workerURL + "/ws"
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }
