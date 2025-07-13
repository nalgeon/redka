package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestZScoreParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZScore
		err  error
	}{
		{
			cmd:  "zscore",
			want: ZScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zscore key",
			want: ZScore{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zscore key member",
			want: ZScore{key: "key", member: "member"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZScore, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.member, test.want.member)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZScoreExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)
		_, _ = red.ZSet().Add("key", "two", 22)

		cmd := redis.MustParse(ParseZScore, "zscore key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 22.0)
		be.Equal(t, conn.Out(), "22")
	})
	t.Run("member not found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 11)

		cmd := redis.MustParse(ParseZScore, "zscore key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZScore, "zscore key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZScore, "zscore key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), "(nil)")
	})
}
