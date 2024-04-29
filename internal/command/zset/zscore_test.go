package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZScoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZScore
		err  error
	}{
		{
			name: "zscore",
			args: command.BuildArgs("zscore"),
			want: zset.ZScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zscore key",
			args: command.BuildArgs("zscore", "key"),
			want: zset.ZScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zscore key member",
			args: command.BuildArgs("zscore", "key", "member"),
			want: zset.ZScore{Key: "key", Member: "member"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZScore)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Member, test.want.Member)
			}
		})
	}
}

func TestZScoreExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := command.MustParse[*zset.ZScore]("zscore key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 22.0)
		testx.AssertEqual(t, conn.Out(), "22")
	})
	t.Run("member not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := command.MustParse[*zset.ZScore]("zscore key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZScore]("zscore key two")
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

		cmd := command.MustParse[*zset.ZScore]("zscore key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
