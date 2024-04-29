package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZRankParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZRank
		err  error
	}{
		{
			name: "zrank",
			args: command.BuildArgs("zrank"),
			want: zset.ZRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrank key",
			args: command.BuildArgs("zrank", "key"),
			want: zset.ZRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zrank key member",
			args: command.BuildArgs("zrank", "key", "member"),
			want: zset.ZRank{Key: "key", Member: "member"},
			err:  nil,
		},
		{
			name: "zrank key member withscore",
			args: command.BuildArgs("zrank", "key", "member", "withscore"),
			want: zset.ZRank{Key: "key", Member: "member", WithScore: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZRank)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Member, test.want.Member)
				testx.AssertEqual(t, cm.WithScore, test.want.WithScore)
			}
		})
	}
}

func TestZRankExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := command.MustParse[*zset.ZRank]("zrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")
	})
	t.Run("with score", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := command.MustParse[*zset.ZRank]("zrank key two withscore")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "2,1,22")
	})
	t.Run("member not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := command.MustParse[*zset.ZRank]("zrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZRank]("zrank key two")
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

		cmd := command.MustParse[*zset.ZRank]("zrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
