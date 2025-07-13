package hash

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestHGetAllParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "hgetall",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "hgetall person",
			key: "person",
			err: nil,
		},
		{
			cmd: "hgetall person name",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHGetAll, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
			} else {
				be.Equal(t, cmd, HGetAll{})
			}
		})
	}
}

func TestHGetAllExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHGetAll, "hgetall person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res.(map[string]core.Value), map[string]core.Value{
			"name": core.Value("alice"), "age": core.Value("25"),
		})
		be.Equal(t,
			conn.Out() == "4,name,alice,age,25" || conn.Out() == "4,age,25,name,alice",
			true)
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseHGetAll, "hgetall person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res.(map[string]core.Value), map[string]core.Value{})
		be.Equal(t, conn.Out(), "0")
	})
}
