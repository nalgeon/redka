package hash

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestHGetParse(t *testing.T) {
	tests := []struct {
		cmd   string
		key   string
		field string
		err   error
	}{
		{
			cmd:   "hget",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hget person",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hget person name",
			key:   "person",
			field: "name",
			err:   nil,
		},
		{
			cmd:   "hget person name age",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHGet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.field, test.field)
			} else {
				be.Equal(t, cmd, HGet{})
			}
		})
	}
}

func TestHGetExec(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHGet, "hget person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value("alice"))
		be.Equal(t, conn.Out(), "alice")
	})
	t.Run("field not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHGet, "hget person age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseHGet, "hget person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
}
