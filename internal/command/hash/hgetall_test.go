package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHGetAllParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "hgetall",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "hgetall person",
			key: "person",
			err: nil,
		},
		{
			cmd: "hgetall person name",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHGetAll, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
			} else {
				testx.AssertEqual(t, cmd, HGetAll{})
			}
		})
	}
}

func TestHGetAllExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHGetAll, "hgetall person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, map[string]core.Value{
			"name": core.Value("alice"), "age": core.Value("25"),
		})
		testx.AssertEqual(t,
			conn.Out() == "4,name,alice,age,25" || conn.Out() == "4,age,25,name,alice",
			true)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHGetAll, "hgetall person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, map[string]core.Value{})
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
