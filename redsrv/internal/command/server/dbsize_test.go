package server

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestDBSizeParse(t *testing.T) {
	tests := []struct {
		cmd string
		err error
	}{
		{
			cmd: "dbsize",
			err: nil,
		},
		{
			cmd: "dbsize name",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseDBSize, test.cmd)
			be.Equal(t, err, test.err)
			if err != nil {
				be.Equal(t, cmd, DBSize{})
			}
		})
	}
}

func TestDBSizeExec(t *testing.T) {
	t.Run("dbsize", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		cmd := redis.MustParse(ParseDBSize, "dbsize")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")
	})

	t.Run("empty", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseDBSize, "dbsize")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
