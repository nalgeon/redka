package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestIncrByFloatParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want IncrByFloat
		err  error
	}{
		{
			cmd:  "incrbyfloat",
			want: IncrByFloat{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrbyfloat age",
			want: IncrByFloat{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrbyfloat age 4.2",
			want: IncrByFloat{key: "age", delta: 4.2},
			err:  nil,
		},
		{
			cmd:  "incrbyfloat age -4.2",
			want: IncrByFloat{key: "age", delta: -4.2},
			err:  nil,
		},
		{
			cmd:  "incrbyfloat age 2.0e2",
			want: IncrByFloat{key: "age", delta: 2.0e2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseIncrByFloat, test.cmd)
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

func TestIncrByFloatExec(t *testing.T) {
	red := getRedka(t)

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "incrbyfloat age 4.2",
			res: 29.2,
			out: "29.2",
		},
		{
			cmd: "incrbyfloat age -4.2",
			res: 20.8,
			out: "20.8",
		},
		{
			cmd: "incrbyfloat age 0",
			res: 25.0,
			out: "25",
		},
		{
			cmd: "incrbyfloat age 2.0e2",
			res: 225.0,
			out: "225",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			_ = red.Str().Set("age", 25)

			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseIncrByFloat, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
