package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSIsMemberParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SIsMember
		err  error
	}{
		{
			cmd:  "sismember",
			want: SIsMember{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sismember key",
			want: SIsMember{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sismember key one",
			want: SIsMember{key: "key", member: []byte("one")},
			err:  nil,
		},
		{
			cmd:  "sismember key one two",
			want: SIsMember{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSIsMember, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.member, test.want.member)
			}
		})
	}
}

func TestSIsMemberExec(t *testing.T) {
	t.Run("elem found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one")

		cmd := redis.MustParse(ParseSIsMember, "sismember key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")
	})
	t.Run("elem not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one")

		cmd := redis.MustParse(ParseSIsMember, "sismember key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSIsMember, "sismember key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "one")

		cmd := redis.MustParse(ParseSIsMember, "sismember key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
