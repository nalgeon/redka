package hash

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestHIncrByParse(t *testing.T) {
	tests := []struct {
		cmd   string
		key   string
		field string
		delta int
		err   error
	}{
		{
			cmd:   "hincrby",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hincrby person",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hincrby person age",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hincrby person age 10",
			key:   "person",
			field: "age",
			delta: 10,
			err:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHIncrBy, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.field, test.field)
				be.Equal(t, cmd.delta, test.delta)
			} else {
				be.Equal(t, cmd, HIncrBy{})
			}
		})
	}
}

func TestHIncrByExec(t *testing.T) {
	t.Run("incr field", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age 10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 35)
		be.Equal(t, conn.Out(), "35")

		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age, core.Value("35"))
	})
	t.Run("decr field", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 15)
		be.Equal(t, conn.Out(), "15")

		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age, core.Value("15"))
	})
	t.Run("create field", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age 10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 10)
		be.Equal(t, conn.Out(), "10")

		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age, core.Value("10"))
	})
	t.Run("create key", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age 10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 10)
		be.Equal(t, conn.Out(), "10")

		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age, core.Value("10"))
	})
}
