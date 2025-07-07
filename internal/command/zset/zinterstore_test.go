package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestZInterStoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZInterStore
		err  error
	}{
		{
			cmd:  "zinterstore",
			want: ZInterStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zinterstore dest",
			want: ZInterStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zinterstore dest 1",
			want: ZInterStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zinterstore dest 1 key",
			want: ZInterStore{dest: "dest", keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "zinterstore dest 2 k1 k2",
			want: ZInterStore{dest: "dest", keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			cmd:  "zinterstore dest 1 k1 k2",
			want: ZInterStore{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zinterstore dest 2 k1 k2 min",
			want: ZInterStore{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zinterstore dest 2 k1 k2 aggregate min",
			want: ZInterStore{dest: "dest", keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			cmd:  "zinterstore dest 2 k1 k2 aggregate avg",
			want: ZInterStore{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZInterStore, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.dest, test.want.dest)
				be.Equal(t, cmd.keys, test.want.keys)
				be.Equal(t, cmd.aggregate, test.want.aggregate)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZInterStoreExec(t *testing.T) {
	t.Run("inter", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.ZSet().AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.ZSet().AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		count, _ := db.ZSet().Len("dest")
		be.Equal(t, count, 2)
	})
	t.Run("overwrite", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.ZSet().AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.ZSet().AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		count, _ := db.ZSet().Len("dest")
		be.Equal(t, count, 2)
		_, err = db.ZSet().GetScore("dest", "one")
		be.Equal(t, err, core.ErrNotFound)
	})
	t.Run("aggregate", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.ZSet().AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.ZSet().AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 3 key1 key2 key3 aggregate min")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		two, _ := db.ZSet().GetScore("dest", "two")
		be.Equal(t, two, 2.0)
		thr, _ := db.ZSet().GetScore("dest", "thr")
		be.Equal(t, thr, 3.0)
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 1 key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 3)
		be.Equal(t, conn.Out(), "3")

		count, _ := db.ZSet().Len("dest")
		be.Equal(t, count, 3)
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key2", "two", 1)
		_, _ = db.ZSet().Add("key3", "thr", 1)

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := db.ZSet().Len("dest")
		be.Equal(t, count, 0)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 2 key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := db.ZSet().Len("dest")
		be.Equal(t, count, 0)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		count, _ := db.ZSet().Len("dest")
		be.Equal(t, count, 0)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_ = db.Str().Set("dest", "value")

		cmd := redis.MustParse(ParseZInterStore, "zinterstore dest 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), core.ErrKeyType.Error()+" (zinterstore)")
	})
}
