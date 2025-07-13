package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestDecrByParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want IncrBy
		err  error
	}{
		{
			cmd:  "decrby",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "decrby age",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "decrby age 42",
			want: IncrBy{key: "age", delta: -42},
			err:  nil,
		},
	}

	parse := func(b redis.BaseCmd) (IncrBy, error) {
		return ParseIncrBy(b, -1)
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

func TestDecrByExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (IncrBy, error) {
		return ParseIncrBy(b, -1)
	}

	t.Run("create", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(parse, "decrby age 12")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, -12)
		be.Equal(t, conn.Out(), "-12")

		age, _ := red.Str().Get("age")
		be.Equal(t, age.MustInt(), -12)
	})

	t.Run("decrby", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("age", "25")

		cmd := redis.MustParse(parse, "decrby age 12")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 13)
		be.Equal(t, conn.Out(), "13")

		age, _ := red.Str().Get("age")
		be.Equal(t, age.MustInt(), 13)
	})
}
