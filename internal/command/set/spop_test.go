package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestSPopParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SPop
		err  error
	}{
		{
			cmd:  "spop",
			want: SPop{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "spop key",
			want: SPop{key: "key"},
			err:  nil,
		},
		{
			cmd:  "spop key 5",
			want: SPop{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSPop, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSPopExec(t *testing.T) {
	t.Run("pop", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSPop, "spop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		s := res.(core.Value).String()
		be.Equal(t, s == "one" || s == "two" || s == "thr", true)
		s = conn.Out()
		be.Equal(t, s == "one" || s == "two" || s == "thr", true)

		slen, _ := red.Set().Len("key")
		be.Equal(t, slen, 2)
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSPop, "spop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "value")

		cmd := redis.MustParse(ParseSPop, "spop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
}
