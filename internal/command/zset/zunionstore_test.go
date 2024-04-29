package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZUnionStoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZUnionStore
		err  error
	}{
		{
			name: "zunionstore",
			args: command.BuildArgs("zunionstore"),
			want: zset.ZUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zunionstore dest",
			args: command.BuildArgs("zunionstore", "dest"),
			want: zset.ZUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zunionstore dest 1",
			args: command.BuildArgs("zunionstore", "dest", "1"),
			want: zset.ZUnionStore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zunionstore dest 1 key",
			args: command.BuildArgs("zunionstore", "dest", "1", "key"),
			want: zset.ZUnionStore{Dest: "dest", Keys: []string{"key"}},
			err:  nil,
		},
		{
			name: "zunionstore dest 2 k1 k2",
			args: command.BuildArgs("zunionstore", "dest", "2", "k1", "k2"),
			want: zset.ZUnionStore{Dest: "dest", Keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			name: "zunionstore dest 1 k1 k2",
			args: command.BuildArgs("zunionstore", "dest", "1", "k1", "k2"),
			want: zset.ZUnionStore{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zunionstore dest 2 k1 k2 min",
			args: command.BuildArgs("zunionstore", "dest", "2", "k1", "k2", "min"),
			want: zset.ZUnionStore{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zunionstore dest 2 k1 k2 aggregate min",
			args: command.BuildArgs("zunionstore", "dest", "2", "k1", "k2", "aggregate", "min"),
			want: zset.ZUnionStore{Dest: "dest", Keys: []string{"k1", "k2"}, Aggregate: "min"},
			err:  nil,
		},
		{
			name: "zunionstore dest 2 k1 k2 aggregate avg",
			args: command.BuildArgs("zunionstore", "dest", "2", "k1", "k2", "aggregate", "avg"),
			want: zset.ZUnionStore{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZUnionStore)
				testx.AssertEqual(t, cm.Dest, test.want.Dest)
				testx.AssertEqual(t, cm.Keys, test.want.Keys)
				testx.AssertEqual(t, cm.Aggregate, test.want.Aggregate)
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

		cmd := command.MustParse[*zset.ZUnionStore]("zunionstore dest 3 key1 key2 key3")
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

		cmd := command.MustParse[*zset.ZUnionStore]("zunionstore dest 3 key1 key2 key3")
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

		cmd := command.MustParse[*zset.ZUnionStore]("zunionstore dest 3 key1 key2 key3 aggregate min")
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

		cmd := command.MustParse[*zset.ZUnionStore]("zunionstore dest 1 key1")
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

		cmd := command.MustParse[*zset.ZUnionStore]("zunionstore dest 2 key1 key2")
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

		cmd := command.MustParse[*zset.ZUnionStore]("zunionstore dest 2 key1 key2")
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

		cmd := command.MustParse[*zset.ZUnionStore]("zunionstore dest 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 1)
	})
}
