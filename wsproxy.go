package main

import (
	"net/http"

	"github.com/gorilla/websocket"

	"rucoder-agent/ops-extension/internal/worker"
)

var wsUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// wsProxy forwards a WebSocket connection to the session worker's /ws
// (RPC + job.completed broadcast). suffix "" = /ws, "/job" = /ws/job.
func (s *server) wsProxy(w http.ResponseWriter, r *http.Request) {
	s.proxyWS(w, r, "")
}

func (s *server) wsProxyJob(w http.ResponseWriter, r *http.Request) {
	s.proxyWS(w, r, "/job")
}

func (s *server) proxyWS(w http.ResponseWriter, r *http.Request, suffix string) {
	session := param(r, "session")
	info, err := s.k8s.FindContainer(r.Context(), sessionKey(session))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	if info.WorkerURL == "" {
		writeErr(w, http.StatusServiceUnavailable, "worker not ready")
		return
	}
	upstream := worker.ToWsURL(info.WorkerURL) + suffix
	if q := r.URL.RawQuery; q != "" {
		upstream += "?" + q
	}

	client, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer client.Close()

	backend, _, err := websocket.DefaultDialer.Dial(upstream, nil)
	if err != nil {
		return
	}
	defer backend.Close()

	errCh := make(chan error, 2)
	go pumpWS(client, backend, errCh)
	go pumpWS(backend, client, errCh)
	<-errCh
}

func pumpWS(dst, src *websocket.Conn, errCh chan error) {
	for {
		mt, msg, err := src.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		if err := dst.WriteMessage(mt, msg); err != nil {
			errCh <- err
			return
		}
	}
}
