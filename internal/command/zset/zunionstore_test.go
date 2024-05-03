package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZUnionStoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZUnionStore
		err  error
	}{
		{
			cmd:  "zunionstore",
			want: ZUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zunionstore dest",
			want: ZUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zunionstore dest 1",
			want: ZUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zunionstore dest 1 key",
			want: ZUnionStore{dest: "dest", keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "zunionstore dest 2 k1 k2",
			want: ZUnionStore{dest: "dest", keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			cmd:  "zunionstore dest 1 k1 k2",
			want: ZUnionStore{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zunionstore dest 2 k1 k2 min",
			want: ZUnionStore{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zunionstore dest 2 k1 k2 aggregate min",
			want: ZUnionStore{dest: "dest", keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			cmd:  "zunionstore dest 2 k1 k2 aggregate avg",
			want: ZUnionStore{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZUnionStore, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.dest, test.want.dest)
				testx.AssertEqual(t, cmd.keys, test.want.keys)
				testx.AssertEqual(t, cmd.aggregate, test.want.aggregate)
			}
		})
	}
}

func TestZUnionStoreExec(t *testing.T) {
	t.Run("union", func(t *testing.T) {
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

		cmd := redis.MustParse(ParseZUnionStore, "zunionstore dest 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.Out(), "4")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 4)
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
		_, _ = db.ZSet().Add("dest", "fiv", 1)

		cmd := redis.MustParse(ParseZUnionStore, "zunionstore dest 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.Out(), "4")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 4)
		_, err = db.ZSet().GetScore("dest", "fiv")
		testx.AssertEqual(t, err, core.ErrNotFound)
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

		cmd := redis.MustParse(ParseZUnionStore, "zunionstore dest 3 key1 key2 key3 aggregate min")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.Out(), "4")

		two, _ := db.ZSet().GetScore("dest", "two")
		testx.AssertEqual(t, two, 2.0)
		thr, _ := db.ZSet().GetScore("dest", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := redis.MustParse(ParseZUnionStore, "zunionstore dest 1 key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 3)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key1", "two", 1)
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := redis.MustParse(ParseZUnionStore, "zunionstore dest 2 key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 2)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key1", "two", 2)
		_ = db.Str().Set("key2", "two")
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := redis.MustParse(ParseZUnionStore, "zunionstore dest 2 key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 2)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_ = db.Str().Set("dest", "value")

		cmd := redis.MustParse(ParseZUnionStore, "zunionstore dest 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), core.ErrKeyType.Error()+" (zunionstore)")
	})
}
