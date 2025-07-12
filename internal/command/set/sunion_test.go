package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestSUnionParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SUnion
		err  error
	}{
		{
			cmd:  "sunion",
			want: SUnion{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sunion key",
			want: SUnion{keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "sunion k1 k2",
			want: SUnion{keys: []string{"k1", "k2"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSUnion, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.keys, test.want.keys)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSUnionExec(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one", "two")
		_, _ = red.Set().Add("key2", "two", "thr")
		_, _ = red.Set().Add("key3", "thr", "fou")

		cmd := redis.MustParse(ParseSUnion, "sunion key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 4)
		be.Equal(t, conn.Out(), "4,fou,one,thr,two")
	})
	t.Run("no keys", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSUnion, "sunion key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("single key", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one", "two", "thr")

		cmd := redis.MustParse(ParseSUnion, "sunion key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 3)
		be.Equal(t, conn.Out(), "3,one,thr,two")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_, _ = red.Set().Add("key2", "two")

		cmd := redis.MustParse(ParseSUnion, "sunion key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 2)
		be.Equal(t, conn.Out(), "2,one,two")
	})
	t.Run("all not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSUnion, "sunion key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_ = red.Str().Set("key2", "two")
		_, _ = red.Set().Add("key3", "thr")

		cmd := redis.MustParse(ParseSUnion, "sunion key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 2)
		be.Equal(t, conn.Out(), "2,one,thr")
	})
}
