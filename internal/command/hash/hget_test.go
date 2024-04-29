package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHGetParse(t *testing.T) {
	tests := []struct {
		name  string
		args  [][]byte
		key   string
		field string
		err   error
	}{
		{
			name:  "hget",
			args:  command.BuildArgs("hget"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			name:  "hget person",
			args:  command.BuildArgs("hget", "person"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			name:  "hget person name",
			args:  command.BuildArgs("hget", "person", "name"),
			key:   "person",
			field: "name",
			err:   nil,
		},
		{
			name:  "hget person name age",
			args:  command.BuildArgs("hget", "person", "name", "age"),
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
				cm := cmd.(*hash.HGet)
				testx.AssertEqual(t, cm.Key, test.key)
				testx.AssertEqual(t, cm.Field, test.field)
			}
		})
	}
}

func TestHGetExec(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HGet]("hget person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("alice"))
		testx.AssertEqual(t, conn.Out(), "alice")
	})
	t.Run("field not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HGet]("hget person age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HGet]("hget person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
