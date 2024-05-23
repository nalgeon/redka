package server

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/tidwall/redcon"
)

// monitors represents a set of connections that should receive a log of command requests.
// Connections run in a detached state so we must be careful to flush and close
// these connections ourselves.
type monitors struct {
	mu   sync.RWMutex
	subs map[redcon.DetachedConn]bool
}

// subscribe will cause a connection to receive all monitored commands.
func (m *monitors) subscribe(detached redcon.DetachedConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subs == nil {
		m.subs = make(map[redcon.DetachedConn]bool)
	}
	m.subs[detached] = true

	go func() {
		for {
			cmd, err := detached.ReadCommand()
			if err != nil {
				m.unsubscribe(detached)
				if err != io.EOF {
					slog.Error("err on read", "client", detached.RemoteAddr(), "err", err)
				}
				return
			}
			name := normName(cmd)
			switch name {
			case "quit":
				m.unsubscribe(detached)
				detached.WriteString("OK")
				_ = detached.Flush()
				_ = detached.Close()
				return
			default:
				detached.WriteError(redis.ErrReplicaInteract.Error())
				_ = detached.Flush()
			}
		}
	}()
}

// unsubscribe will prevent a detached connection from receiving any monitored commands.
func (m *monitors) unsubscribe(detached redcon.DetachedConn) {
	m.mu.Lock()
	delete(m.subs, detached)
	m.mu.Unlock()
}

// monitor sends a client's command log to all subscribed connections.
func (m *monitors) monitor(t time.Time, db int, client redcon.Conn, cmd redcon.Command) {
	if len(m.subs) == 0 {
		return
	}

	secs := t.Unix()
	microsecs := t.UnixNano() % 1e9 / 1e3
	quotedCommand := bytes.Join(cmd.Args, []byte(`" "`))
	line := fmt.Sprintf("%d.%06d [%d %s] \"%s\"", secs, microsecs, db, client.RemoteAddr(), quotedCommand)

	m.mu.RLock()
	defer m.mu.RUnlock()
	for detached := range m.subs {
		detached.WriteString(line)
		detached.Flush() // TODO: Maybe flush periodically in the background to reduce io overhead?
	}
}
