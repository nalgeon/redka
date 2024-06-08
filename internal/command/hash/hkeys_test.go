package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHKeysParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "hkeys",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "hkeys person",
			key: "person",
			err: nil,
		},
		{
			cmd: "hkeys person name",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHKeys, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
			} else {
				testx.AssertEqual(t, cmd, HKeys{})
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

		cmd := redis.MustParse(ParseHKeys, "hkeys person")
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

		cmd := redis.MustParse(ParseHKeys, "hkeys person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, []string{})
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
