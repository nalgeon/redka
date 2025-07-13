// Package server implements a Redis-compatible (RESP) server.
package server

import (
	"fmt"
	"log/slog"

	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

// Server represents a Redka server.
type Server struct {
	net  string
	addr string
	srv  *redcon.Server
	db   *redka.DB
}

// New creates a new Redka server.
func New(net string, addr string, db *redka.DB) *Server {
	handler := createHandlers(db)
	accept := func(conn redcon.Conn) bool {
		slog.Info("accept connection", "client", conn.RemoteAddr())
		return true
	}
	closed := func(conn redcon.Conn, err error) {
		if err != nil {
			slog.Debug("close connection", "client", conn.RemoteAddr(), "error", err)
		} else {
			slog.Debug("close connection", "client", conn.RemoteAddr())
		}
	}
	return &Server{
		net:  net,
		addr: addr,
		srv:  redcon.NewServerNetwork(net, addr, handler, accept, closed),
		db:   db,
	}
}

// Start starts the server.
// If ready chan is not nil, sends a nil value when the server
// is ready to accept connections, or an error if it fails to start.
func (s *Server) Start(ready chan error) error {
	slog.Info("starting redcon server", "addr", s.addr)
	err := s.srv.ListenServeAndSignal(ready)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// Stop stops the server.
func (s *Server) Stop() error {
	err := s.srv.Close()
	if err != nil {
		return fmt.Errorf("server close: %w", err)
	}
	slog.Debug("redcon server stopped", "addr", s.addr)

	err = s.db.Close()
	if err != nil {
		return fmt.Errorf("db close: %w", err)
	}
	slog.Debug("database closed")

	return nil
}
