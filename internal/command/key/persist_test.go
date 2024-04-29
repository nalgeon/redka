package key_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestPersistParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "persist",
			args: command.BuildArgs("persist"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "persist name",
			args: command.BuildArgs("persist", "name"),
			key:  "name",
			err:  nil,
		},
		{
			name: "persist name age",
			args: command.BuildArgs("persist", "name", "age"),
			key:  "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*key.Persist).Key, test.key)
			}
		})
	}
}

func TestPersistExec(t *testing.T) {
	t.Run("persist to persist", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*key.Persist]("persist name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})

	t.Run("volatile to persist", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := command.MustParse[*key.Persist]("persist name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*key.Persist]("persist age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")

		key, _ := db.Key().Get("age")
		testx.AssertEqual(t, key.Exists(), false)
	})
}
