package key

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestDelParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want []string
		err  error
	}{
		{
			cmd:  "del",
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "del name",
			want: []string{"name"},
			err:  nil,
		},
		{
			cmd:  "del name age",
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseDel, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.keys, test.want)
			} else {
				be.Equal(t, cmd, Del{})
			}
		})
	}
}

func TestDelExec(t *testing.T) {
	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "del name",
			res: 1,
			out: "1",
		},
		{
			cmd: "del name age",
			res: 2,
			out: "2",
		},
		{
			cmd: "del name age street",
			res: 2,
			out: "2",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			red := getRedka(t)

			_ = red.Str().Set("name", "alice")
			_ = red.Str().Set("age", 50)
			_ = red.Str().Set("city", "paris")

			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseDel, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)

			_, err = red.Str().Get("name")
			be.Err(t, err, core.ErrNotFound)
			city, _ := red.Str().Get("city")
			be.Equal(t, city.String(), "paris")
		})
	}
}
