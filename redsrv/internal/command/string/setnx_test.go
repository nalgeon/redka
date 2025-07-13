package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestSetNXParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SetNX
		err  error
	}{
		{
			cmd:  "setnx",
			want: SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "setnx name",
			want: SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "setnx name alice",
			want: SetNX{key: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			cmd:  "setnx name alice 60",
			want: SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSetNX, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.value, test.want.value)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSetNXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSetNX, "setnx name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "1")

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseSetNX, "setnx name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, false)
		be.Equal(t, conn.Out(), "0")

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "alice")
	})
}
