package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHValsParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "hvals",
			args: command.BuildArgs("hvals"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hvals person",
			args: command.BuildArgs("hvals", "person"),
			key:  "person",
			err:  nil,
		},
		{
			name: "hvals person name",
			args: command.BuildArgs("hvals", "person", "name"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*hash.HVals)
				testx.AssertEqual(t, cm.Key, test.key)
			}
		})
	}
}

func TestHValsExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := command.MustParse[*hash.HVals]("hvals person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{core.Value("25"), core.Value("alice")})
		testx.AssertEqual(t, conn.Out(), "2,25,alice")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HVals]("hvals person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []core.Value{})
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
