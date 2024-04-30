package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHIncrByParse(t *testing.T) {
	tests := []struct {
		cmd   string
		key   string
		field string
		delta int
		err   error
	}{
		{
			cmd:   "hincrby",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hincrby person",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hincrby person age",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hincrby person age 10",
			key:   "person",
			field: "age",
			delta: 10,
			err:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHIncrBy, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
				testx.AssertEqual(t, cmd.field, test.field)
				testx.AssertEqual(t, cmd.delta, test.delta)
			}
		})
	}
}

func TestHIncrByExec(t *testing.T) {
	t.Run("incr field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age 10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 35)
		testx.AssertEqual(t, conn.Out(), "35")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("35"))
	})
	t.Run("decr field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 15)
		testx.AssertEqual(t, conn.Out(), "15")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("15"))
	})
	t.Run("create field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age 10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10)
		testx.AssertEqual(t, conn.Out(), "10")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10"))
	})
	t.Run("create key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHIncrBy, "hincrby person age 10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10)
		testx.AssertEqual(t, conn.Out(), "10")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10"))
	})
}
