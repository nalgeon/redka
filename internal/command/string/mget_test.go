package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestMGetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want []string
		err  error
	}{
		{
			cmd:  "mget",
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "mget name",
			want: []string{"name"},
			err:  nil,
		},
		{
			cmd:  "mget name age",
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseMGet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.keys, test.want)
			} else {
				be.Equal(t, cmd, MGet{})
			}
		})
	}
}

func TestMGetExec(t *testing.T) {
	red := getRedka(t)

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "mget name",
			res: []core.Value{core.Value("alice")},
			out: "1,alice",
		},
		{
			cmd: "mget name age",
			res: []core.Value{core.Value("alice"), core.Value("25")},
			out: "2,alice,25",
		},
		{
			cmd: "mget name city age",
			res: []core.Value{core.Value("alice"), core.Value(nil), core.Value("25")},
			out: "3,alice,(nil),25",
		},
		{
			cmd: "mget one two",
			res: []core.Value{core.Value(nil), core.Value(nil)},
			out: "2,(nil),(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseMGet, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
