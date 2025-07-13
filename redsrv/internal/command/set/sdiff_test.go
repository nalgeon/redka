package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestSDiffParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SDiff
		err  error
	}{
		{
			cmd:  "sdiff",
			want: SDiff{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sdiff key",
			want: SDiff{keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "sdiff k1 k2",
			want: SDiff{keys: []string{"k1", "k2"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSDiff, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.keys, test.want.keys)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSDiffExec(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one", "two", "thr", "fiv")
		_, _ = red.Set().Add("key2", "two", "fou", "six")
		_, _ = red.Set().Add("key3", "thr", "six")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 2)
		be.Equal(t, conn.Out(), "2,fiv,one", "2,one,fiv")
	})
	t.Run("no keys", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSDiff, "sdiff key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("single key", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one", "two", "thr")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 3)
		be.Equal(t, conn.Out(), "3,one,thr,two", "3,one,two,thr")
	})
	t.Run("empty", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one", "two")
		_, _ = red.Set().Add("key2", "one", "fou")
		_, _ = red.Set().Add("key3", "two", "fiv")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("first not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key2", "two")
		_, _ = red.Set().Add("key3", "thr")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("rest not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_, _ = red.Set().Add("key2", "two")

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 1)
		be.Equal(t, conn.Out(), "1,one")
	})
	t.Run("all not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
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

		cmd := redis.MustParse(ParseSDiff, "sdiff key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]core.Value)), 1)
		be.Equal(t, conn.Out(), "1,one")
	})
}
