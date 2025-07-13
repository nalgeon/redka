package key

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestExistsParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want []string
		err  error
	}{
		{
			cmd:  "exists",
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "exists name",
			want: []string{"name"},
			err:  nil,
		},
		{
			cmd:  "exists name age",
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseExists, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.keys, test.want)
			} else {
				be.Equal(t, cmd, Exists{})
			}
		})
	}
}

func TestExistsExec(t *testing.T) {
	red := getRedka(t)

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 50)
	_ = red.Str().Set("city", "paris")

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "exists name",
			res: 1,
			out: "1",
		},
		{
			cmd: "exists name age",
			res: 2,
			out: "2",
		},
		{
			cmd: "exists name age street",
			res: 2,
			out: "2",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseExists, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
