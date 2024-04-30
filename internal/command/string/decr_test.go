package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDecrParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want Incr
		err  error
	}{
		{
			cmd:  "decr",
			want: Incr{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "decr age",
			want: Incr{key: "age", delta: -1},
			err:  nil,
		},
		{
			cmd:  "decr age 42",
			want: Incr{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	parse := func(b redis.BaseCmd) (*Incr, error) {
		return ParseIncr(b, -1)
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(parse, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.delta, test.want.delta)
			}
		})
	}
}

func TestDecrExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (*Incr, error) {
		return ParseIncr(b, -1)
	}

	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(parse, "decr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -1)
		testx.AssertEqual(t, conn.Out(), "-1")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), -1)
	})

	t.Run("decr", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("age", "25")

		cmd := redis.MustParse(parse, "decr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 24)
		testx.AssertEqual(t, conn.Out(), "24")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 24)
	})
}
