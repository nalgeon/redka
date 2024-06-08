package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDecrByParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want IncrBy
		err  error
	}{
		{
			cmd:  "decrby",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "decrby age",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "decrby age 42",
			want: IncrBy{key: "age", delta: -42},
			err:  nil,
		},
	}

	parse := func(b redis.BaseCmd) (IncrBy, error) {
		return ParseIncrBy(b, -1)
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

func TestDecrByExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (IncrBy, error) {
		return ParseIncrBy(b, -1)
	}

	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(parse, "decrby age 12")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -12)
		testx.AssertEqual(t, conn.Out(), "-12")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), -12)
	})

	t.Run("decrby", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("age", "25")

		cmd := redis.MustParse(parse, "decrby age 12")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 13)
		testx.AssertEqual(t, conn.Out(), "13")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 13)
	})
}
