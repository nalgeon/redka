package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZUnionParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZUnion
		err  error
	}{
		{
			cmd:  "zunion",
			want: ZUnion{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zunion 1",
			want: ZUnion{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zunion 1 key",
			want: ZUnion{keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "zunion 2 k1 k2",
			want: ZUnion{keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			cmd:  "zunion 1 k1 k2",
			want: ZUnion{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zunion 2 k1 k2 min",
			want: ZUnion{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zunion 2 k1 k2 aggregate min",
			want: ZUnion{keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			cmd:  "zunion 2 k1 k2 aggregate avg",
			want: ZUnion{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zunion 2 k1 k2 withscores",
			want: ZUnion{keys: []string{"k1", "k2"}, withScores: true},
			err:  nil,
		},
		{
			cmd:  "zunion 3 k1 k2 k3 withscores aggregate sum",
			want: ZUnion{keys: []string{"k1", "k2", "k3"}, aggregate: "sum", withScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZUnion, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.keys, test.want.keys)
				testx.AssertEqual(t, cmd.aggregate, test.want.aggregate)
				testx.AssertEqual(t, cmd.withScores, test.want.withScores)
			} else {
				testx.AssertEqual(t, cmd, test.want)
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

		cmd := redis.MustParse(ParseZUnion, "zunion 3 key1 key2 key3")
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

		cmd := redis.MustParse(ParseZUnion, "zunion 3 key1 key2 key3 withscores")
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

		cmd := redis.MustParse(ParseZUnion, "zunion 3 key1 key2 key3 aggregate min withscores")
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

		cmd := redis.MustParse(ParseZUnion, "zunion 1 key1")
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

		cmd := redis.MustParse(ParseZUnion, "zunion 3 key1 key2 key3")
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

		cmd := redis.MustParse(ParseZUnion, "zunion 2 key1 key2")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
		testx.AssertEqual(t, conn.Out(), "1,one")
	})
}
