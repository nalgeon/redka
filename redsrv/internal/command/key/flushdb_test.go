package key

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestFlushDBParse(t *testing.T) {
	tests := []struct {
		cmd string
		err error
	}{
		{
			cmd: "flushdb",
			err: nil,
		},
		{
			cmd: "flushdb name",
			err: redis.ErrSyntaxError,
		},
		{
			cmd: "flushdb 1",
			err: redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseFlushDB, test.cmd)
			be.Equal(t, err, test.err)
			if err != nil {
				be.Equal(t, cmd, FlushDB{})
			}
		})
	}
}

func TestFlushDBExec(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		cmd := redis.MustParse(ParseFlushDB, "flushdb")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		keys, _ := red.Key().Keys("*")
		be.Equal(t, len(keys), 0)
	})

	t.Run("empty", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseFlushDB, "flushdb")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		keys, _ := red.Key().Keys("*")
		be.Equal(t, len(keys), 0)
	})
}
