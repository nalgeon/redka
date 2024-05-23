package server

import (
	"regexp"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

// TestMonitor ensures that the MONITOR command starts and stops appropriately
func TestMonitor(t *testing.T) {
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}

	mux := createHandlers(db)

	monConn := newMonitorTestConn()

	// MONITOR _must_ be the first command in the list for this to work
	serveRESP := func(conn *monitorConn, cmd redcon.Command) string {
		name := normName(cmd)
		switch name {
		// monitor command handled normally
		case "monitor":
			mux.ServeRESP(conn, cmd)
		// subsequent commands handled in detached state
		default:
			conn.sendClientCommand(cmd)
		}
		return <-conn.lastOutCh
	}

	// send test client commands while monitoring.
	tests := []struct {
		cmd  redcon.Command
		want string
	}{
		{
			cmd: redcon.Command{
				Raw:  []byte("MONITOR"),
				Args: [][]byte{[]byte("MONITOR")},
			},
			want: "OK",
		},
		{
			cmd: redcon.Command{
				Raw:  []byte("GET foo"),
				Args: [][]byte{[]byte("GET"), []byte("foo")},
			},
			want: "ERR Replica can't interact with the keyspace",
		},
		{
			cmd: redcon.Command{
				Raw:  []byte("QUIT"),
				Args: [][]byte{[]byte("QUIT")},
			},
			want: "OK",
		},
	}
	for _, test := range tests {
		out := serveRESP(monConn, test.cmd)
		if out != test.want {
			t.Fatalf("want '%s', got '%s'", test.want, out)
		}
	}
}

// TestMonitor_OtherClient ensures that the MONITOR command gets notified when other clients send requests
func TestMonitor_OtherClient(t *testing.T) {
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}

	mux := createHandlers(db)

	// Start monitoring
	monConn := newMonitorTestConn()
	mux.ServeRESP(monConn, redcon.Command{
		Raw:  []byte("MONITOR"),
		Args: [][]byte{[]byte("MONITOR")},
	})
	if out := <-monConn.lastOutCh; out != "OK" {
		t.Fatalf("want '%s', got '%s'", "OK", out)
	}

	// send test client commands while monitoring.
	tests := []struct {
		cmd       redcon.Command
		wantRegex string
	}{
		{
			cmd: redcon.Command{
				Raw:  []byte("ECHO hello"),
				Args: [][]byte{[]byte("ECHO"), []byte("hello")},
			},
			wantRegex: `^\d{10}\.\d{6} \[0 localhost:22222\] "ECHO" "hello"$`,
		},
		{
			cmd: redcon.Command{
				Raw:  []byte("GET foo"),
				Args: [][]byte{[]byte("GET"), []byte("foo")},
			},
			wantRegex: `^\d{10}\.\d{6} \[0 localhost:22222\] "GET" "foo"$`,
		},
	}
	for _, test := range tests {
		cmdConn := &fakeConn{addr: "localhost:22222"}
		mux.ServeRESP(cmdConn, test.cmd)

		out := <-monConn.lastOutCh
		matched, err := regexp.MatchString(test.wantRegex, out)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !matched {
			t.Fatalf("want '%s', got '%s'", test.wantRegex, out)
		}
	}
}

type monitorConn struct {
	*fakeConn
	clientCommands chan redcon.Command
	lastOutCh      chan string
}

func newMonitorTestConn() *monitorConn {
	return &monitorConn{
		fakeConn:       new(fakeConn),
		clientCommands: make(chan redcon.Command, 1),
		lastOutCh:      make(chan string, 1),
	}
}

func (c *monitorConn) RemoteAddr() string {
	return "localhost:11111"
}

func (c *monitorConn) Detach() redcon.DetachedConn {
	return c
}

func (c *monitorConn) Close() error {
	close(c.clientCommands)
	return nil
}

func (dc *monitorConn) Flush() error {
	dc.lastOutCh <- dc.parts[len(dc.parts)-1]
	return nil
}

func (dc *monitorConn) ReadCommand() (redcon.Command, error) {
	return <-dc.clientCommands, nil
}

func (c *monitorConn) sendClientCommand(cmd redcon.Command) {
	c.clientCommands <- cmd
}
