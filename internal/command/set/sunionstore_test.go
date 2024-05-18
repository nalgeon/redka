package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSUnionStoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SUnionStore
		err  error
	}{
		{
			cmd:  "sunionstore",
			want: SUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sunionstore dest",
			want: SUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sunionstore dest key",
			want: SUnionStore{dest: "dest", keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "sunionstore dest k1 k2",
			want: SUnionStore{dest: "dest", keys: []string{"k1", "k2"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSUnionStore, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.dest, test.want.dest)
				testx.AssertEqual(t, cmd.keys, test.want.keys)
			}
		})
	}
}

func TestSUnionStoreExec(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two", "thr")
		_, _ = db.Set().Add("key2", "two", "thr", "fou")
		_, _ = db.Set().Add("key3", "one", "two", "thr", "fou")

		cmd := redis.MustParse(ParseSUnionStore, "sunionstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.Out(), "4")

		items, _ := db.Set().Items("dest")
		sortValues(items)
		testx.AssertEqual(t, items, []core.Value{
			core.Value("fou"), core.Value("one"), core.Value("thr"), core.Value("two"),
		})
	})
	t.Run("rewrite dest", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "one")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSUnionStore, "sunionstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		items, _ := db.Set().Items("dest")
		testx.AssertEqual(t, items, []core.Value{core.Value("one")})
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSUnionStore, "sunionstore dest key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		items, _ := db.Set().Items("dest")
		sortValues(items)
		testx.AssertEqual(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSUnionStore, "sunionstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		items, _ := db.Set().Items("dest")
		testx.AssertEqual(t, items, []core.Value(nil))
	})
	t.Run("source key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "one")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSUnionStore, "sunionstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		items, _ := db.Set().Items("dest")
		testx.AssertEqual(t, items, []core.Value{core.Value("one")})
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "one")
		_ = db.Str().Set("key3", "one")

		cmd := redis.MustParse(ParseSUnionStore, "sunionstore dest key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		items, _ := db.Set().Items("dest")
		testx.AssertEqual(t, items, []core.Value{core.Value("one")})
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "one")
		_ = db.Str().Set("dest", "old")

		cmd := redis.MustParse(ParseSUnionStore, "sunionstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), core.ErrKeyType.Error()+" (sunionstore)")

		sval, _ := db.Str().Get("dest")
		testx.AssertEqual(t, sval, core.Value("old"))
	})
}
