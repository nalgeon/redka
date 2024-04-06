// Redka server
package server

import (
	"log/slog"
	"sync"

	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

// Server represents a Redka server.
type Server struct {
	addr string
	srv  *redcon.Server
	db   *redka.DB
	wg   *sync.WaitGroup
}

// New creates a new Redka server.
func New(addr string, db *redka.DB) *Server {
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
		addr: addr,
		srv:  redcon.NewServer(addr, handler, accept, closed),
		db:   db,
		wg:   &sync.WaitGroup{},
	}
}

// Start starts the server.
func (s *Server) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		slog.Info("serve connections", "addr", s.addr)
		err := s.srv.ListenAndServe()
		if err != nil {
			slog.Error("serve connections", "error", err)
		}
	}()
}

// Stop stops the server.
func (s *Server) Stop() error {
	err := s.srv.Close()
	if err != nil {
		return err
	}
	slog.Debug("close redcon server", "addr", s.addr)

	err = s.db.Close()
	if err != nil {
		return err
	}
	slog.Debug("close database")

	s.wg.Wait()
	return nil
}
