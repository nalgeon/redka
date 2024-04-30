package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrByParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want IncrBy
		err  error
	}{
		{
			cmd:  "incrby",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrby age",
			want: IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrby age 42",
			want: IncrBy{key: "age", delta: 42},
			err:  nil,
		},
	}

	parse := func(b redis.BaseCmd) (*IncrBy, error) {
		return ParseIncrBy(b, 1)
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

func TestIncrByExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (*IncrBy, error) {
		return ParseIncrBy(b, 1)
	}

	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(parse, "incrby age 42")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 42)
		testx.AssertEqual(t, conn.Out(), "42")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 42)
	})

	t.Run("incrby", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("age", "25")

		cmd := redis.MustParse(parse, "incrby age 42")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 67)
		testx.AssertEqual(t, conn.Out(), "67")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 67)
	})
}
