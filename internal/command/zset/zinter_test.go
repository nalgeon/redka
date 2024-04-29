package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZInterParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZInter
		err  error
	}{
		{
			name: "zinter",
			args: command.BuildArgs("zinter"),
			want: zset.ZInter{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zinter 1",
			args: command.BuildArgs("zinter", "1"),
			want: zset.ZInter{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zinter 1 key",
			args: command.BuildArgs("zinter", "1", "key"),
			want: zset.ZInter{Keys: []string{"key"}},
			err:  nil,
		},
		{
			name: "zinter 2 k1 k2",
			args: command.BuildArgs("zinter", "2", "k1", "k2"),
			want: zset.ZInter{Keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			name: "zinter 1 k1 k2",
			args: command.BuildArgs("zinter", "1", "k1", "k2"),
			want: zset.ZInter{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zinter 2 k1 k2 min",
			args: command.BuildArgs("zinter", "2", "k1", "k2", "min"),
			want: zset.ZInter{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zinter 2 k1 k2 aggregate min",
			args: command.BuildArgs("zinter", "2", "k1", "k2", "aggregate", "min"),
			want: zset.ZInter{Keys: []string{"k1", "k2"}, Aggregate: "min"},
			err:  nil,
		},
		{
			name: "zinter 2 k1 k2 aggregate avg",
			args: command.BuildArgs("zinter", "2", "k1", "k2", "aggregate", "avg"),
			want: zset.ZInter{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zinter 2 k1 k2 withscores",
			args: command.BuildArgs("zinter", "2", "k1", "k2", "withscores"),
			want: zset.ZInter{Keys: []string{"k1", "k2"}, WithScores: true},
			err:  nil,
		},
		{
			name: "zinter 3 k1 k2 k3 withscores aggregate sum",
			args: command.BuildArgs("zinter", "3", "k1", "k2", "k3", "withscores", "aggregate", "sum"),
			want: zset.ZInter{Keys: []string{"k1", "k2", "k3"}, Aggregate: "sum", WithScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZInter)
				testx.AssertEqual(t, cm.Keys, test.want.Keys)
				testx.AssertEqual(t, cm.Aggregate, test.want.Aggregate)
				testx.AssertEqual(t, cm.WithScores, test.want.WithScores)
			}
		})
	}
}

func TestZInterExec(t *testing.T) {
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

		cmd := command.MustParse[*zset.ZInter]("zinter 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
		testx.AssertEqual(t, conn.Out(), "2,thr,two")
	})
	t.Run("withscores", func(t *testing.T) {
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

		cmd := command.MustParse[*zset.ZInter]("zinter 3 key1 key2 key3 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
		testx.AssertEqual(t, conn.Out(), "4,thr,9,two,222")
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

		cmd := command.MustParse[*zset.ZInter]("zinter 3 key1 key2 key3 aggregate min withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
		testx.AssertEqual(t, conn.Out(), "4,two,2,thr,3")
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := command.MustParse[*zset.ZInter]("zinter 1 key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.Out(), "3,one,two,thr")
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key2", "two", 1)
		_, _ = db.ZSet().Add("key3", "thr", 1)

		cmd := command.MustParse[*zset.ZInter]("zinter 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZInter]("zinter 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := command.MustParse[*zset.ZInter]("zinter 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
