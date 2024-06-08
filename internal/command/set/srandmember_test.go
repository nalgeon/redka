package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
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
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
			} else {
				testx.AssertEqual(t, cmd, test.want)
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
		testx.AssertNoErr(t, err)
		s := res.(core.Value).String()
		testx.AssertEqual(t, s == "one" || s == "two" || s == "thr", true)
		s = conn.Out()
		testx.AssertEqual(t, s == "one" || s == "two" || s == "thr", true)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSRandMember, "srandmember key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseSRandMember, "srandmember key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
