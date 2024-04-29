package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRevRankParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZRevRank
		err  error
	}{
		{
			name: "zrevrank",
			args: command.BuildArgs("zrevrank"),
			want: zset.ZRevRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrank key",
			args: command.BuildArgs("zrevrank", "key"),
			want: zset.ZRevRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrevrank key member",
			args: command.BuildArgs("zrevrank", "key", "member"),
			want: zset.ZRevRank{Key: "key", Member: "member"},
			err:  nil,
		},
		{
			name: "zrevrank key member withscore",
			args: command.BuildArgs("zrevrank", "key", "member", "withscore"),
			want: zset.ZRevRank{Key: "key", Member: "member", WithScore: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZRevRank)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Member, test.want.Member)
				testx.AssertEqual(t, cm.WithScore, test.want.WithScore)
			}
		})
	}
}

func TestZRevRankExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := command.MustParse[*zset.ZRevRank]("zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("with score", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := command.MustParse[*zset.ZRevRank]("zrevrank key two withscore")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "2,0,22")
	})
	t.Run("member not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := command.MustParse[*zset.ZRevRank]("zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZRevRank]("zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := command.MustParse[*zset.ZRevRank]("zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
