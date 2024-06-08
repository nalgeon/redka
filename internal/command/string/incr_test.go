package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want Incr
		err  error
	}{
		{
			cmd:  "incr",
			want: Incr{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incr age",
			want: Incr{key: "age", delta: 1},
			err:  nil,
		},
		{
			cmd:  "incr age 42",
			want: Incr{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	parse := func(b redis.BaseCmd) (Incr, error) {
		return ParseIncr(b, 1)
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(parse, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.delta, test.want.delta)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestIncrExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (Incr, error) {
		return ParseIncr(b, 1)
	}

	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(parse, "incr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 1)
	})

	t.Run("incr", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("age", "25")

		cmd := redis.MustParse(parse, "incr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 26)
		testx.AssertEqual(t, conn.Out(), "26")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 26)
	})
}
