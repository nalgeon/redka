package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHExistsParse(t *testing.T) {
	tests := []struct {
		cmd   string
		key   string
		field string
		err   error
	}{
		{
			cmd:   "hexists",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hexists person",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hexists person name",
			key:   "person",
			field: "name",
			err:   nil,
		},
		{
			cmd:   "hexists person name age",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHExists, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
				testx.AssertEqual(t, cmd.field, test.field)
			} else {
				testx.AssertEqual(t, cmd, HExists{})
			}
		})
	}
}

func TestHExistsExec(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHExists, "hexists person name")
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

		cmd := redis.MustParse(ParseHExists, "hexists person age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHExists, "hexists person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
