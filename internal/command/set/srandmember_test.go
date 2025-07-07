package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestSRandMemberParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SRandMember
		err  error
	}{
		{
			cmd:  "srandmember",
			want: SRandMember{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "srandmember key",
			want: SRandMember{key: "key"},
			err:  nil,
		},
		{
			cmd:  "srandmember key 5",
			want: SRandMember{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSRandMember, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSRandMemberExec(t *testing.T) {
	t.Run("random", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSRandMember, "srandmember key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		s := res.(core.Value).String()
		be.Equal(t, s == "one" || s == "two" || s == "thr", true)
		s = conn.Out()
		be.Equal(t, s == "one" || s == "two" || s == "thr", true)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSRandMember, "srandmember key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseSRandMember, "srandmember key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
}
