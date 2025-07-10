package key

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestTTLParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "ttl",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "ttl name",
			key: "name",
			err: nil,
		},
		{
			cmd: "ttl name age",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseTTL, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
			} else {
				be.Equal(t, cmd, TTL{})
			}
		})
	}
}

func TestTTLExec(t *testing.T) {
	t.Run("has ttl", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := redis.MustParse(ParseTTL, "ttl name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 60)
		be.Equal(t, conn.Out(), "60")
	})

	t.Run("no ttl", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseTTL, "ttl name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, -1)
		be.Equal(t, conn.Out(), "-1")
	})

	t.Run("not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseTTL, "ttl name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, -2)
		be.Equal(t, conn.Out(), "-2")
	})
}
