package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestDecrParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want Incr
		err  error
	}{
		{
			cmd:  "decr",
			want: Incr{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "decr age",
			want: Incr{key: "age", delta: -1},
			err:  nil,
		},
		{
			cmd:  "decr age 42",
			want: Incr{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	parse := func(b redis.BaseCmd) (Incr, error) {
		return ParseIncr(b, -1)
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

func TestDecrExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (Incr, error) {
		return ParseIncr(b, -1)
	}

	t.Run("create", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(parse, "decr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, -1)
		be.Equal(t, conn.Out(), "-1")

		age, _ := red.Str().Get("age")
		be.Equal(t, age.MustInt(), -1)
	})

	t.Run("decr", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("age", "25")

		cmd := redis.MustParse(parse, "decr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 24)
		be.Equal(t, conn.Out(), "24")

		age, _ := red.Str().Get("age")
		be.Equal(t, age.MustInt(), 24)
	})
}
