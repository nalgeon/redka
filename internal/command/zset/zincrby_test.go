package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZIncrByParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZIncrBy
		err  error
	}{
		{
			cmd:  "zincrby",
			want: ZIncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zincrby key",
			want: ZIncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zincrby key one",
			want: ZIncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zincrby key 11 one",
			want: ZIncrBy{key: "key", member: "one", delta: 11.0},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZIncrBy, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.member, test.want.member)
				testx.AssertEqual(t, cmd.delta, test.want.delta)
			}
		})
	}
}

func TestZIncrByExec(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 25.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 25.5)
		testx.AssertEqual(t, conn.Out(), "25.5")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 25.5)
	})
	t.Run("create field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 25.5 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 25.5)
		testx.AssertEqual(t, conn.Out(), "25.5")

		score, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, score, 25.5)
	})
	t.Run("update field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 25.5)

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 10.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 36.0)
		testx.AssertEqual(t, conn.Out(), "36")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 36.0)
	})
	t.Run("decrement", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 25.5)

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key -10.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 15.0)
		testx.AssertEqual(t, conn.Out(), "15")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 15.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = red.Str().Set("key", "one")

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 25.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 25.5)
		testx.AssertEqual(t, conn.Out(), "25.5")
	})
}
