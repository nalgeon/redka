package key

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestTypeParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "type",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "type name",
			key: "name",
			err: nil,
		},
		{
			cmd: "type name age",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseType, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
			} else {
				be.Equal(t, cmd, Type{})
			}
		})
	}
}

func TestTypeExec(t *testing.T) {
	red := getRedka(t)

	_ = red.Str().Set("kstr", "string")
	_, _ = red.List().PushBack("klist", "list")
	_, _ = red.Hash().Set("khash", "field", "hash")
	_, _ = red.ZSet().Add("kzset", "zset", 1)

	tests := []struct {
		key  string
		want string
	}{
		{key: "kstr", want: "string"},
		{key: "klist", want: "list"},
		{key: "khash", want: "hash"},
		{key: "kzset", want: "zset"},
		{key: "knone", want: "none"},
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			cmd := redis.MustParse(ParseType, "type "+test.key)
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res.(string), test.want)
			be.Equal(t, conn.Out(), test.want)
		})
	}
}
