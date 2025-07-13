package hash

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestHLenParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "hlen",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "hlen person",
			key: "person",
			err: nil,
		},
		{
			cmd: "hlen person name",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHLen, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
			} else {
				be.Equal(t, cmd, HLen{})
			}
		})
	}
}

func TestHLenExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHLen, "hlen person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseHLen, "hlen person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
