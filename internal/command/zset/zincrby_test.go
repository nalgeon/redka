package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZIncrByParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZIncrBy
		err  error
	}{
		{
			name: "zincrby",
			args: command.BuildArgs("zincrby"),
			want: zset.ZIncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zincrby key",
			args: command.BuildArgs("zincrby", "key"),
			want: zset.ZIncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zincrby key one",
			args: command.BuildArgs("zincrby", "key", "one"),
			want: zset.ZIncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zincrby key 11 one",
			args: command.BuildArgs("zincrby", "key", "11", "one"),
			want: zset.ZIncrBy{Key: "key", Member: "one", Delta: 11.0},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZIncrBy)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Member, test.want.Member)
				testx.AssertEqual(t, cm.Delta, test.want.Delta)
			}
		})
	}
}

func TestZIncrByExec(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZIncrBy]("zincrby key 25.5 one")
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

		cmd := command.MustParse[*zset.ZIncrBy]("zincrby key 25.5 two")
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

		cmd := command.MustParse[*zset.ZIncrBy]("zincrby key 10.5 one")
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

		cmd := command.MustParse[*zset.ZIncrBy]("zincrby key -10.5 one")
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

		cmd := command.MustParse[*zset.ZIncrBy]("zincrby key 25.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 25.5)
		testx.AssertEqual(t, conn.Out(), "25.5")
	})
}
