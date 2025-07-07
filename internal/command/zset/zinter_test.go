package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
)

func TestZInterParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZInter
		err  error
	}{
		{
			cmd:  "zinter",
			want: ZInter{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zinter 1",
			want: ZInter{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zinter 1 key",
			want: ZInter{keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "zinter 2 k1 k2",
			want: ZInter{keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			cmd:  "zinter 1 k1 k2",
			want: ZInter{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zinter 2 k1 k2 min",
			want: ZInter{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zinter 2 k1 k2 aggregate min",
			want: ZInter{keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			cmd:  "zinter 2 k1 k2 aggregate avg",
			want: ZInter{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "zinter 2 k1 k2 withscores",
			want: ZInter{keys: []string{"k1", "k2"}, withScores: true},
			err:  nil,
		},
		{
			cmd:  "zinter 3 k1 k2 k3 withscores aggregate sum",
			want: ZInter{keys: []string{"k1", "k2", "k3"}, aggregate: "sum", withScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZInter, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.keys, test.want.keys)
				be.Equal(t, cmd.aggregate, test.want.aggregate)
				be.Equal(t, cmd.withScores, test.want.withScores)
			} else {
				be.Equal(t, cmd, test.want)
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

		cmd := redis.MustParse(ParseZInter, "zinter 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 2)
		be.Equal(t, conn.Out(), "2,thr,two")
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

		cmd := redis.MustParse(ParseZInter, "zinter 3 key1 key2 key3 withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 2)
		be.Equal(t, conn.Out(), "4,thr,9,two,222")
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

		cmd := redis.MustParse(ParseZInter, "zinter 3 key1 key2 key3 aggregate min withscores")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 2)
		be.Equal(t, conn.Out(), "4,two,2,thr,3")
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := redis.MustParse(ParseZInter, "zinter 1 key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 3)
		be.Equal(t, conn.Out(), "3,one,two,thr")
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key2", "two", 1)
		_, _ = db.ZSet().Add("key3", "thr", 1)

		cmd := redis.MustParse(ParseZInter, "zinter 3 key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZInter, "zinter 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZInter, "zinter 1 key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(res.([]rzset.SetItem)), 0)
		be.Equal(t, conn.Out(), "0")
	})
}
