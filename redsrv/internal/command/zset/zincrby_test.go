package zset

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
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
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.member, test.want.member)
				be.Equal(t, cmd.delta, test.want.delta)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestZIncrByExec(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 25.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 25.5)
		be.Equal(t, conn.Out(), "25.5")

		score, _ := red.ZSet().GetScore("key", "one")
		be.Equal(t, score, 25.5)
	})
	t.Run("create field", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 10)

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 25.5 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 25.5)
		be.Equal(t, conn.Out(), "25.5")

		score, _ := red.ZSet().GetScore("key", "two")
		be.Equal(t, score, 25.5)
	})
	t.Run("update field", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 25.5)

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 10.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 36.0)
		be.Equal(t, conn.Out(), "36")

		score, _ := red.ZSet().GetScore("key", "one")
		be.Equal(t, score, 36.0)
	})
	t.Run("decrement", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.ZSet().Add("key", "one", 25.5)

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key -10.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 15.0)
		be.Equal(t, conn.Out(), "15")

		score, _ := red.ZSet().GetScore("key", "one")
		be.Equal(t, score, 15.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "one")

		cmd := redis.MustParse(ParseZIncrBy, "zincrby key 25.5 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), core.ErrKeyType.Error()+" (zincrby)")
	})
}
