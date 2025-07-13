// Package redsrv implements a Redis-compatible (RESP) server.
package redsrv

import (
	"fmt"
	"log/slog"

	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

// A Redis-compatible Redka server that uses the RESP protocol.
// Works with a Redka database instance.
//
// To start the server, call [Server.Start] method and wait
// for the ready channel to receive a nil value (success) or an error.
//
// To stop the server, call [Server.Stop] method.
type Server struct {
	net  string
	addr string
	srv  *redcon.Server
	db   *redka.DB
	log  *slog.Logger
}

// New creates a new Redka server with the given
// network, address and database. Does not start the server.
func New(net string, addr string, db *redka.DB) *Server {
	log := db.Log()
	handler := createHandlers(db)
	accept := func(conn redcon.Conn) bool {
		log.Info("accept connection", "client", conn.RemoteAddr())
		return true
	}
	closed := func(conn redcon.Conn, err error) {
		if err != nil {
			log.Debug("close connection", "client", conn.RemoteAddr(), "error", err)
		} else {
			log.Debug("close connection", "client", conn.RemoteAddr())
		}
	}
	return &Server{
		net:  net,
		addr: addr,
		srv:  redcon.NewServerNetwork(net, addr, handler, accept, closed),
		db:   db,
		log:  log,
	}
}

// Start starts the server.
// If ready chan is not nil, sends a nil value when the server
// is ready to accept connections, or an error if it fails to start.
func (s *Server) Start(ready chan error) error {
	s.log.Info("starting redcon server", "addr", s.addr)
	err := s.srv.ListenServeAndSignal(ready)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// Stop stops the server and closes the database.
func (s *Server) Stop() error {
	err := s.srv.Close()
	if err != nil {
		return fmt.Errorf("server close: %w", err)
	}
	s.log.Debug("redcon server stopped", "addr", s.addr)

	err = s.db.Close()
	if err != nil {
		return fmt.Errorf("db close: %w", err)
	}
	s.log.Debug("database closed")

	return nil
}
