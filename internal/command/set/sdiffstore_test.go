package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestSDiffStoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SDiffStore
		err  error
	}{
		{
			cmd:  "sdiffstore",
			want: SDiffStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sdiffstore dest",
			want: SDiffStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sdiffstore dest key",
			want: SDiffStore{dest: "dest", keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "sdiffstore dest k1 k2",
			want: SDiffStore{dest: "dest", keys: []string{"k1", "k2"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSDiffStore, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.dest, test.want.dest)
				be.Equal(t, cmd.keys, test.want.keys)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSDiffStoreExec(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two", "thr", "fiv")
		_, _ = db.Set().Add("key2", "two", "fou", "six")
		_, _ = db.Set().Add("key3", "thr", "six")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		items, _ := db.Set().Items("dest")
		sortValues(items)
		be.Equal(t, items, []core.Value{core.Value("fiv"), core.Value("one")})
	})
	t.Run("rewrite dest", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "two")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		items, _ := db.Set().Items("dest")
		be.Equal(t, items, []core.Value{core.Value("one")})
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		items, _ := db.Set().Items("dest")
		sortValues(items)
		be.Equal(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "one")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		items, _ := db.Set().Items("dest")
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("first not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key2", "two")
		_, _ = db.Set().Add("key3", "thr")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		items, _ := db.Set().Items("dest")
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("rest not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "two")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		items, _ := db.Set().Items("dest")
		be.Equal(t, items, []core.Value{core.Value("one")})
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "two")
		_ = db.Str().Set("key3", "thr")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		items, _ := db.Set().Items("dest")
		be.Equal(t, items, []core.Value{core.Value("one")})
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "two")
		_ = db.Str().Set("dest", "old")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), core.ErrKeyType.Error()+" (sdiffstore)")

		sval, _ := db.Str().Get("dest")
		be.Equal(t, sval, core.Value("old"))
	})
}
