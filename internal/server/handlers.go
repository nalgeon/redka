package server

import (
	"log/slog"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/command"
	"github.com/tidwall/redcon"
)

// createHandlers returns the server command handlers.
func createHandlers(db *redka.DB) redcon.HandlerFunc {
	return logging(parse(multi(handle(db))))
}

// logging logs the command processing time.
func logging(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		start := time.Now()
		next(conn, cmd)
		slog.Debug("process command", "client", conn.RemoteAddr(),
			"name", string(cmd.Args[0]), "time", time.Since(start))
	}
}

// parse parses the command arguments.
func parse(next redcon.HandlerFunc) redcon.HandlerFunc {
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
func multi(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		name := normName(cmd)
		state := getState(conn)
		if state.inMulti {
			switch name {
			case "multi":
				state.pop()
				conn.WriteError(command.ErrNestedMulti.Error())
			case "exec":
				state.pop()
				conn.WriteArray(len(state.cmds))
				next(conn, cmd)
				state.inMulti = false
			case "discard":
				state.clear()
				conn.WriteString("OK")
				state.inMulti = false
			default:
				conn.WriteString("QUEUED")
			}
		} else {
			switch name {
			case "multi":
				state.inMulti = true
				state.pop()
				conn.WriteString("OK")
			case "exec":
				state.pop()
				conn.WriteError(command.ErrNotInMulti.Error())
			case "discard":
				state.pop()
				conn.WriteError(command.ErrNotInMulti.Error())
			default:
				next(conn, cmd)
			}
		}
	}
}

// handle processes the command in either multi or single mode.
func handle(db *redka.DB) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		state := getState(conn)
		if state.inMulti {
			handleMulti(conn, state, db)
		} else {
			handleSingle(conn, state, db)
		}
		state.clear()
	}
}

// handleMulti processes a batch of commands in a transaction.
func handleMulti(conn redcon.Conn, state *connState, db *redka.DB) {
	err := db.Update(func(tx *redka.Tx) error {
		for _, pcmd := range state.cmds {
			_, err := pcmd.Run(conn, command.RedkaTx(tx))
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
func handleSingle(conn redcon.Conn, state *connState, db *redka.DB) {
	pcmd := state.pop()
	_, err := pcmd.Run(conn, command.RedkaDB(db))
	if err != nil {
		slog.Warn("run single command", "client", conn.RemoteAddr(),
			"name", pcmd.Name(), "err", err)
		return
	}
}
