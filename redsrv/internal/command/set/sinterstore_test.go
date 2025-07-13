package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestSInterStoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SInterStore
		err  error
	}{
		{
			cmd:  "sinterstore",
			want: SInterStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sinterstore dest",
			want: SInterStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sinterstore dest key",
			want: SInterStore{dest: "dest", keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "sinterstore dest k1 k2",
			want: SInterStore{dest: "dest", keys: []string{"k1", "k2"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSInterStore, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.dest, test.want.dest)
				be.Equal(t, cmd.keys, test.want.keys)
			}
		})
	}
}

func TestSInterStoreExec(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one", "two", "thr")
		_, _ = red.Set().Add("key2", "two", "thr", "fou")
		_, _ = red.Set().Add("key3", "one", "two", "thr", "fou")

		cmd := redis.MustParse(ParseSInterStore, "sinterstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		items, _ := red.Set().Items("dest")
		sortValues(items)
		be.Equal(t, items, []core.Value{core.Value("thr"), core.Value("two")})
	})
	t.Run("rewrite dest", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_, _ = red.Set().Add("key2", "one")
		_, _ = red.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSInterStore, "sinterstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		items, _ := red.Set().Items("dest")
		be.Equal(t, items, []core.Value{core.Value("one")})
	})
	t.Run("single key", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one", "two")
		_, _ = red.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSInterStore, "sinterstore dest key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		items, _ := red.Set().Items("dest")
		sortValues(items)
		be.Equal(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("empty", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_, _ = red.Set().Add("key2", "two")
		_, _ = red.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSInterStore, "sinterstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		items, _ := red.Set().Items("dest")
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("source key not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_, _ = red.Set().Add("key2", "one")
		_, _ = red.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSInterStore, "sinterstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		items, _ := red.Set().Items("dest")
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_, _ = red.Set().Add("key2", "one")
		_ = red.Str().Set("key3", "one")

		cmd := redis.MustParse(ParseSInterStore, "sinterstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		items, _ := red.Set().Items("dest")
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key1", "one")
		_, _ = red.Set().Add("key2", "one")
		_ = red.Str().Set("dest", "old")

		cmd := redis.MustParse(ParseSInterStore, "sinterstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), core.ErrKeyType.Error()+" (sinterstore)")

		sval, _ := red.Str().Get("dest")
		be.Equal(t, sval, core.Value("old"))
	})
}
