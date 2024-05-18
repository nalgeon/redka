package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSRemParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SRem
		err  error
	}{
		{
			cmd:  "srem",
			want: SRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "srem key",
			want: SRem{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "srem key one",
			want: SRem{key: "key", members: []any{"one"}},
			err:  nil,
		},
		{
			cmd:  "srem key one two",
			want: SRem{key: "key", members: []any{"one", "two"}},
			err:  nil,
		},
		{
			cmd:  "srem key one two thr",
			want: SRem{key: "key", members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSRem, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.members, test.want.members)
			}
		})
	}
}

func TestSRemExec(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSRem, "srem key one thr")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		items, _ := db.Set().Items("key")
		testx.AssertEqual(t, items, []core.Value{core.Value("two")})
	})
	t.Run("none", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSRem, "srem key fou fiv")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		slen, _ := db.Set().Len("key")
		testx.AssertEqual(t, slen, 3)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSRem, "srem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseSRem, "srem key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
