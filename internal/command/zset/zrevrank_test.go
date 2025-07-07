package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestZRevRankParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRevRank
		err  error
	}{
		{
			cmd:  "zrevrank",
			want: ZRevRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrank key",
			want: ZRevRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrevrank key member",
			want: ZRevRank{key: "key", member: "member"},
			err:  nil,
		},
		{
			cmd:  "zrevrank key member withscore",
			want: ZRevRank{key: "key", member: "member", withScore: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRevRank, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.member, test.want.member)
				be.Equal(t, cmd.withScore, test.want.withScore)
			} else {
				be.Equal(t, cmd, test.want)
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

		cmd := redis.MustParse(ParseZRevRank, "zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("with score", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := redis.MustParse(ParseZRevRank, "zrevrank key two withscore")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "2,0,22")
	})
	t.Run("member not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := redis.MustParse(ParseZRevRank, "zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZRevRank, "zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZRevRank, "zrevrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
}
