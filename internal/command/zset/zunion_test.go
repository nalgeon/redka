package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZUnionParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZUnion
		err  error
	}{
		{
			name: "zunion",
			args: command.BuildArgs("zunion"),
			want: zset.ZUnion{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zunion 1",
			args: command.BuildArgs("zunion", "1"),
			want: zset.ZUnion{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zunion 1 key",
			args: command.BuildArgs("zunion", "1", "key"),
			want: zset.ZUnion{Keys: []string{"key"}},
			err:  nil,
		},
		{
			name: "zunion 2 k1 k2",
			args: command.BuildArgs("zunion", "2", "k1", "k2"),
			want: zset.ZUnion{Keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			name: "zunion 1 k1 k2",
			args: command.BuildArgs("zunion", "1", "k1", "k2"),
			want: zset.ZUnion{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zunion 2 k1 k2 min",
			args: command.BuildArgs("zunion", "2", "k1", "k2", "min"),
			want: zset.ZUnion{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zunion 2 k1 k2 aggregate min",
			args: command.BuildArgs("zunion", "2", "k1", "k2", "aggregate", "min"),
			want: zset.ZUnion{Keys: []string{"k1", "k2"}, Aggregate: "min"},
			err:  nil,
		},
		{
			name: "zunion 2 k1 k2 aggregate avg",
			args: command.BuildArgs("zunion", "2", "k1", "k2", "aggregate", "avg"),
			want: zset.ZUnion{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zunion 2 k1 k2 withscores",
			args: command.BuildArgs("zunion", "2", "k1", "k2", "withscores"),
			want: zset.ZUnion{Keys: []string{"k1", "k2"}, WithScores: true},
			err:  nil,
		},
		{
			name: "zunion 3 k1 k2 k3 withscores aggregate sum",
			args: command.BuildArgs("zunion", "3", "k1", "k2", "k3", "withscores", "aggregate", "sum"),
			want: zset.ZUnion{Keys: []string{"k1", "k2", "k3"}, Aggregate: "sum", WithScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZUnion)
				testx.AssertEqual(t, cm.Keys, test.want.Keys)
				testx.AssertEqual(t, cm.Aggregate, test.want.Aggregate)
				testx.AssertEqual(t, cm.WithScores, test.want.WithScores)
			}
		})
	}
}

func TestZUnionExec(t *testing.T) {
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

		cmd := command.MustParse[*zset.ZUnion]("zunion 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.Out(), "4,one,thr,two,fou")
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

		cmd := command.MustParse[*zset.ZUnion]("zunion 3 key1 key2 key3 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.Out(), "8,one,2,thr,9,two,222,fou,404")
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

		cmd := command.MustParse[*zset.ZUnion]("zunion 3 key1 key2 key3 aggregate min withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.Out(), "8,one,1,two,2,thr,3,fou,4")
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := command.MustParse[*zset.ZUnion]("zunion 1 key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.Out(), "3,one,two,thr")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)

		cmd := command.MustParse[*zset.ZUnion]("zunion 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
		testx.AssertEqual(t, conn.Out(), "1,one")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_ = db.Str().Set("key2", "value")

		cmd := command.MustParse[*zset.ZUnion]("zunion 2 key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
		testx.AssertEqual(t, conn.Out(), "1,one")
	})
}
