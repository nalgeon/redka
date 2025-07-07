package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestSCardParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SCard
		err  error
	}{
		{
			cmd:  "scard",
			want: SCard{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "scard key",
			want: SCard{key: "key"},
			err:  nil,
		},
		{
			cmd:  "scard key one",
			want: SCard{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSCard, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSCardExec(t *testing.T) {
	t.Run("card", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one", "two")

		cmd := redis.MustParse(ParseSCard, "scard key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one")
		_, _ = db.Set().Delete("key", "one")

		cmd := redis.MustParse(ParseSCard, "scard key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSCard, "scard key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseSCard, "scard key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
