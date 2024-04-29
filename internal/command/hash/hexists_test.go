package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHExistsParse(t *testing.T) {
	tests := []struct {
		name  string
		args  [][]byte
		key   string
		field string
		err   error
	}{
		{
			name:  "hexists",
			args:  command.BuildArgs("hexists"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			name:  "hexists person",
			args:  command.BuildArgs("hexists", "person"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			name:  "hexists person name",
			args:  command.BuildArgs("hexists", "person", "name"),
			key:   "person",
			field: "name",
			err:   nil,
		},
		{
			name:  "hexists person name age",
			args:  command.BuildArgs("hexists", "person", "name", "age"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*hash.HExists)
				testx.AssertEqual(t, cm.Key, test.key)
				testx.AssertEqual(t, cm.Field, test.field)
			}
		})
	}
}

func TestHExistsExec(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HExists]("hexists person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")
	})
	t.Run("field not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HExists]("hexists person age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HExists]("hexists person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
