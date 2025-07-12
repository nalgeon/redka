package key

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestKeysParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want string
		err  error
	}{
		{
			cmd:  "keys",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "keys *",
			want: "*",
			err:  nil,
		},
		{
			cmd:  "keys k2*",
			want: "k2*",
			err:  nil,
		},
		{
			cmd:  "keys * k2*",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseKeys, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.pattern, test.want)
			} else {
				be.Equal(t, cmd, Keys{})
			}
		})
	}
}

func TestKeysExec(t *testing.T) {
	red := getRedka(t)

	_ = red.Str().Set("k11", "11")
	_ = red.Str().Set("k12", "12")
	_ = red.Str().Set("k21", "21")
	_ = red.Str().Set("k22", "22")
	_ = red.Str().Set("k31", "31")

	tests := []struct {
		cmd string
		res []string
		out string
	}{
		{
			cmd: "keys *",
			res: []string{"k11", "k12", "k21", "k22", "k31"},
			out: "5,k11,k12,k21,k22,k31",
		},
		{
			cmd: "keys k2*",
			res: []string{"k21", "k22"},
			out: "2,k21,k22",
		},
		{
			cmd: "keys k12",
			res: []string{"k12"},
			out: "1,k12",
		},
		{
			cmd: "keys name",
			res: []string{},
			out: "0",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseKeys, test.cmd)
			keys, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			for i, key := range keys.([]core.Key) {
				be.Equal(t, key.Key, test.res[i])
			}
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
