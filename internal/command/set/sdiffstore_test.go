package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
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
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.dest, test.want.dest)
				testx.AssertEqual(t, cmd.keys, test.want.keys)
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
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		items, _ := db.Set().Items("dest")
		sortValues(items)
		testx.AssertEqual(t, items, []core.Value{core.Value("fiv"), core.Value("one")})
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

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1")
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
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "one")
		_, _ = db.Set().Add("dest", "old")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		items, _ := db.Set().Items("dest")
		testx.AssertEqual(t, items, []core.Value(nil))
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
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		items, _ := db.Set().Items("dest")
		testx.AssertEqual(t, items, []core.Value(nil))
	})
	t.Run("rest not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "two")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2 key3")
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
		_, _ = db.Set().Add("key2", "two")
		_ = db.Str().Set("key3", "thr")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2 key3")
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
		_, _ = db.Set().Add("key2", "two")
		_ = db.Str().Set("dest", "old")

		cmd := redis.MustParse(ParseSDiffStore, "sdiffstore dest key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), core.ErrKeyType.Error()+" (sdiffstore)")

		sval, _ := db.Str().Get("dest")
		testx.AssertEqual(t, sval, core.Value("old"))
	})
}
