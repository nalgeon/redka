package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSPopParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SPop
		err  error
	}{
		{
			cmd:  "spop",
			want: SPop{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "spop key",
			want: SPop{key: "key"},
			err:  nil,
		},
		{
			cmd:  "spop key 5",
			want: SPop{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSPop, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestSPopExec(t *testing.T) {
	t.Run("pop", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one", "two", "thr")

		cmd := redis.MustParse(ParseSPop, "spop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		s := res.(core.Value).String()
		testx.AssertEqual(t, s == "one" || s == "two" || s == "thr", true)
		s = conn.Out()
		testx.AssertEqual(t, s == "one" || s == "two" || s == "thr", true)

		slen, _ := db.Set().Len("key")
		testx.AssertEqual(t, slen, 2)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSPop, "spop key")
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

		cmd := redis.MustParse(ParseSPop, "spop key")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
