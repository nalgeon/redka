package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHGetParse(t *testing.T) {
	tests := []struct {
		cmd   string
		key   string
		field string
		err   error
	}{
		{
			cmd:   "hget",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hget person",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			cmd:   "hget person name",
			key:   "person",
			field: "name",
			err:   nil,
		},
		{
			cmd:   "hget person name age",
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHGet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
				testx.AssertEqual(t, cmd.field, test.field)
			} else {
				testx.AssertEqual(t, cmd, HGet{})
			}
		})
	}
}

func TestHGetExec(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHGet, "hget person name")
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

		cmd := redis.MustParse(ParseHGet, "hget person age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHGet, "hget person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
