package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestIncrByParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want IncrBy
		err  error
	}{
		{
			cmd:  "incrby",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrby age",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrby age 42",
			want: IncrBy{key: "age", delta: 42},
			err:  nil,
		},
	}

	parse := func(b redis.BaseCmd) (IncrBy, error) {
		return ParseIncrBy(b, 1)
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(parse, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.delta, test.want.delta)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestIncrByExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (IncrBy, error) {
		return ParseIncrBy(b, 1)
	}

	t.Run("create", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(parse, "incrby age 42")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 42)
		be.Equal(t, conn.Out(), "42")

		age, _ := red.Str().Get("age")
		be.Equal(t, age.MustInt(), 42)
	})

	t.Run("incrby", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("age", "25")

		cmd := redis.MustParse(parse, "incrby age 42")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 67)
		be.Equal(t, conn.Out(), "67")

		age, _ := red.Str().Get("age")
		be.Equal(t, age.MustInt(), 67)
	})
}
