package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZAddParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZAdd
		err  error
	}{
		{
			cmd:  "zadd",
			want: ZAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zadd key",
			want: ZAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zadd key one",
			want: ZAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zadd key 1.1 one",
			want: ZAdd{key: "key", items: map[any]float64{"one": 1.1}},
			err:  nil,
		},
		{
			cmd:  "zadd key 1.1 one 2.2",
			want: ZAdd{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd: "zadd key 1.1 one 2.2 two",
			want: ZAdd{key: "key", items: map[any]float64{
				"one": 1.1,
				"two": 2.2,
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZAdd, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.items, test.want.items)
			}
		})
	}
}

func TestZAddExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZAdd, "zadd key 11 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 11.0)
	})
	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZAdd, "zadd key 11 one 22 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		one, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, one, 11.0)
		two, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, two, 22.0)
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := redis.MustParse(ParseZAdd, "zadd key 12 one 22 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		one, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, one, 12.0)
		two, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, two, 22.0)
	})
	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := redis.MustParse(ParseZAdd, "zadd key 12 one 23 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		one, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, one, 12.0)
		two, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, two, 23.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZAdd, "zadd key 11 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), core.ErrKeyType.Error()+" (zadd)")
	})
}
