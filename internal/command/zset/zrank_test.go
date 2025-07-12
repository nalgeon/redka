package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestZRankParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZRank
		err  error
	}{
		{
			cmd:  "zrank",
			want: ZRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrank key",
			want: ZRank{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zrank key member",
			want: ZRank{key: "key", member: "member"},
			err:  nil,
		},
		{
			cmd:  "zrank key member withscore",
			want: ZRank{key: "key", member: "member", withScore: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZRank, test.cmd)
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

func TestZRankExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)
		_, _ = red.ZSet().Add("key", "two", 22)

		cmd := redis.MustParse(ParseZRank, "zrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")
	})
	t.Run("with score", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)
		_, _ = red.ZSet().Add("key", "two", 22)

		cmd := redis.MustParse(ParseZRank, "zrank key two withscore")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "2,1,22")
	})
	t.Run("member not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)

		cmd := redis.MustParse(ParseZRank, "zrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZRank, "zrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZRank, "zrank key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
}
