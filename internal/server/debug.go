package server

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
)

// DebugServer is a debug server with pprof endpoints.
type DebugServer struct {
	srv *http.Server
}

// NewDebug creates a new debug server.
func NewDebug(host string, port int) *DebugServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &DebugServer{
		srv: &http.Server{
			Addr:    net.JoinHostPort(host, strconv.Itoa(port)),
			Handler: mux,
		},
	}
}

// Start starts the debug server.
func (s *DebugServer) Start() error {
	slog.Info("starting debug server", "addr", s.srv.Addr)
	err := s.srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// Stop stops the debug server.
func (s *DebugServer) Stop() error {
	err := s.srv.Close()
	if err != nil {
		return fmt.Errorf("close: %w", err)
	}
	slog.Debug("debug server stopped", "addr", s.srv.Addr)
	return nil
}
