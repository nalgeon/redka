package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHGetAllParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "hgetall",
			args: command.BuildArgs("hgetall"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hgetall person",
			args: command.BuildArgs("hgetall", "person"),
			key:  "person",
			err:  nil,
		},
		{
			name: "hgetall person name",
			args: command.BuildArgs("hgetall", "person", "name"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*hash.HGetAll)
				testx.AssertEqual(t, cm.Key, test.key)
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

		cmd := command.MustParse[*hash.HGetAll]("hgetall person")
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

		cmd := command.MustParse[*hash.HGetAll]("hgetall person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, map[string]core.Value{})
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
