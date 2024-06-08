package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSMembersParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SMembers
		err  error
	}{
		{
			cmd:  "smembers",
			want: SMembers{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "smembers key",
			want: SMembers{key: "key"},
			err:  nil,
		},
		{
			cmd:  "smembers key one",
			want: SMembers{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSMembers, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestSMembersExec(t *testing.T) {
	t.Run("items", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSMembers, "smembers key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{
			core.Value("one"), core.Value("thr"), core.Value("two"),
		})
		testx.AssertEqual(t, conn.Out(), "3,one,thr,two")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSMembers, "smembers key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseSMembers, "smembers key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
