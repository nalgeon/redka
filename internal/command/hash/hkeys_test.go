package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHKeysParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "hkeys",
			args: command.BuildArgs("hkeys"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hkeys person",
			args: command.BuildArgs("hkeys", "person"),
			key:  "person",
			err:  nil,
		},
		{
			name: "hkeys person name",
			args: command.BuildArgs("hkeys", "person", "name"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*hash.HKeys)
				testx.AssertEqual(t, cm.Key, test.key)
			}
		})
	}
}

func TestHKeysExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := command.MustParse[*hash.HKeys]("hkeys person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []string{"age", "name"})
		testx.AssertEqual(t,
			conn.Out() == "2,age,name" || conn.Out() == "2,name,age",
			true)
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HKeys]("hkeys person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []string{})
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
