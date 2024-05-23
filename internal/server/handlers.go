package server

import (
	"log/slog"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/tidwall/redcon"
)

// createHandlers returns the server command handlers.
func createHandlers(db *redka.DB) redcon.HandlerFunc {
	h := handlers{
		db:       db,
		monitors: &monitors{},
	}
	return h.logging(
		h.parse(
			h.monitor(
				h.multi(
					h.handle(),
				),
			),
		),
	)
}

type handlers struct {
	db       *redka.DB
	monitors *monitors
}

// logging logs the command processing time.
func (h *handlers) logging(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		start := time.Now()
		next(conn, cmd)
		slog.Debug("process command", "client", conn.RemoteAddr(),
			"name", string(cmd.Args[0]), "time", time.Since(start))
	}
}

// parse parses the command arguments.
func (h *handlers) parse(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		pcmd, err := command.Parse(cmd.Args)
		if err != nil {
			conn.WriteError(pcmd.Error(err))
			return
		}
		state := getState(conn)
		state.push(pcmd)
		next(conn, cmd)
	}
}

// multi handles the MULTI, EXEC, and DISCARD commands and delegates
// the rest to the next handler either in multi or single mode.
func (h *handlers) multi(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		name := normName(cmd)
		state := getState(conn)
		if state.inMulti {
			switch name {
			case "multi":
				h.monitors.monitor(time.Now(), 0, conn, cmd)
				state.pop()
				conn.WriteError(redis.ErrNestedMulti.Error())
			case "exec":
				state.pop()
				conn.WriteArray(len(state.cmds))
				next(conn, cmd)
				h.monitors.monitor(time.Now(), 0, conn, cmd)
				state.inMulti = false
			case "discard":
				h.monitors.monitor(time.Now(), 0, conn, cmd)
				state.clear()
				conn.WriteString("OK")
				state.inMulti = false
			default:
				conn.WriteString("QUEUED")
			}
		} else {
			switch name {
			case "multi":
				h.monitors.monitor(time.Now(), 0, conn, cmd)
				state.inMulti = true
				state.pop()
				conn.WriteString("OK")
			case "exec":
				h.monitors.monitor(time.Now(), 0, conn, cmd)
				state.pop()
				conn.WriteError(redis.ErrNotInMulti.Error())
			case "discard":
				h.monitors.monitor(time.Now(), 0, conn, cmd)
				state.pop()
				conn.WriteError(redis.ErrNotInMulti.Error())
			default:
				next(conn, cmd)
			}
		}
	}
}

func (h *handlers) monitor(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		name := normName(cmd)
		state := getState(conn)
		switch name {
		case "monitor":
			h.monitors.monitor(time.Now(), 0, conn, cmd)
			state.pop()
			detached := conn.Detach()
			detached.WriteString("OK")
			detached.Flush()
			h.monitors.subscribe(detached)
		default:
			next(conn, cmd)
		}
	}
}

// handle processes the command in either multi or single mode.
func (h *handlers) handle() redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		state := getState(conn)
		if state.inMulti {
			h.handleMulti(conn, cmd, state)
		} else {
			h.handleSingle(conn, cmd, state)
		}
		state.clear()
	}
}

// handleMulti processes a batch of commands in a transaction.
func (h *handlers) handleMulti(conn redcon.Conn, cmd redcon.Command, state *connState) {
	err := h.db.Update(func(tx *redka.Tx) error {
		for _, pcmd := range state.cmds {
			h.monitors.monitor(time.Now(), 0, conn, cmd)
			_, err := pcmd.Run(conn, redis.RedkaTx(tx))
			if err != nil {
				slog.Warn("run multi command", "client", conn.RemoteAddr(),
					"name", pcmd.Name(), "err", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		slog.Warn("run multi", "client", conn.RemoteAddr(), "err", err)
	}
}

// handleSingle processes a single command.
func (h *handlers) handleSingle(conn redcon.Conn, cmd redcon.Command, state *connState) {
	h.monitors.monitor(time.Now(), 0, conn, cmd)
	pcmd := state.pop()
	_, err := pcmd.Run(conn, redis.RedkaDB(h.db))
	if err != nil {
		slog.Warn("run single command", "client", conn.RemoteAddr(),
			"name", pcmd.Name(), "err", err)
		return
	}
}
