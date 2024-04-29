package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHSetNXParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want hash.HSetNX
		err  error
	}{
		{
			name: "hsetnx",
			args: command.BuildArgs("hsetnx"),
			want: hash.HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hsetnx person",
			args: command.BuildArgs("hsetnx", "person"),
			want: hash.HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hsetnx person name",
			args: command.BuildArgs("hsetnx", "person", "name"),
			want: hash.HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hsetnx person name alice",
			args: command.BuildArgs("hsetnx", "person", "name", "alice"),
			want: hash.HSetNX{Key: "person", Field: "name", Value: []byte("alice")},
			err:  nil,
		},
		{
			name: "hsetnx person name alice age 25",
			args: command.BuildArgs("hsetnx", "person", "name", "alice", "age", "25"),
			want: hash.HSetNX{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*hash.HSetNX)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Value, test.want.Value)
			}
		})
	}
}

func TestHSetNXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HSetNX]("hsetnx person name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HSetNX]("hsetnx person name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})
}
