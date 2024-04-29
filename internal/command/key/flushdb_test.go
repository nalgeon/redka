package key_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestFlushDBParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		err  error
	}{
		{
			name: "flushdb",
			args: command.BuildArgs("flushdb"),
			err:  nil,
		},
		{
			name: "flushdb name",
			args: command.BuildArgs("flushdb", "name"),
			err:  redis.ErrSyntaxError,
		},
		{
			name: "flushdb 1",
			args: command.BuildArgs("flushdb", "1"),
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
		})
	}
}

func TestFlushDBExec(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		cmd := command.MustParse[*key.FlushDB]("flushdb")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		keys, _ := db.Key().Keys("*")
		testx.AssertEqual(t, len(keys), 0)
	})

	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*key.FlushDB]("flushdb")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		keys, _ := db.Key().Keys("*")
		testx.AssertEqual(t, len(keys), 0)
	})
}
