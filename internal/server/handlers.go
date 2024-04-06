package server

import (
	"log/slog"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/command"
	"github.com/tidwall/redcon"
)

func createHandlers(db *redka.DB) redcon.HandlerFunc {
	return logging(parse(multi(handle(db))))
}

func logging(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		start := time.Now()
		next(conn, cmd)
		slog.Debug("process command", "client", conn.RemoteAddr(),
			"name", string(cmd.Args[0]), "time", time.Since(start))
	}
}

func parse(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		pcmd, err := command.Parse(cmd.Args)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		state := getState(conn)
		state.push(pcmd)
		next(conn, cmd)
	}
}

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

func handleMulti(conn redcon.Conn, state *connState, db *redka.DB) {
	err := db.Update(func(tx *redka.Tx) error {
		for _, pcmd := range state.cmds {
			out, err := pcmd.Run(conn, tx)
			if err != nil {
				slog.Debug("run multi command", "client", conn.RemoteAddr(),
					"name", pcmd.Name(), "out", out, "err", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		conn.WriteError(err.Error())
	}
}

func handleSingle(conn redcon.Conn, state *connState, db *redka.DB) {
	pcmd := state.pop()
	out, err := pcmd.Run(conn, db)
	if err != nil {
		slog.Debug("run single command", "client", conn.RemoteAddr(),
			"name", pcmd.Name(), "out", out, "err", err)
		return
	}

}
