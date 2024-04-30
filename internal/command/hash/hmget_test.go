package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHMGetParse(t *testing.T) {
	tests := []struct {
		cmd    string
		key    string
		fields []string
		err    error
	}{
		{
			cmd:    "hmget",
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hmget person",
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hmget person name",
			key:    "person",
			fields: []string{"name"},
			err:    nil,
		},
		{
			cmd:    "hmget person name age",
			key:    "person",
			fields: []string{"name", "age"},
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHMGet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
				testx.AssertEqual(t, cmd.fields, test.fields)
			}
		})
	}
}

func TestHMGetExec(t *testing.T) {
	t.Run("one field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHMGet, "hmget person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, conn.Out(), "1,alice")
		items := res.([]core.Value)
		testx.AssertEqual(t, len(items), 1)
		testx.AssertEqual(t, items[0], core.Value("alice"))
	})
	t.Run("some fields", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)
		_, _ = db.Hash().Set("person", "happy", true)

		cmd := redis.MustParse(ParseHMGet, "hmget person name happy city")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, conn.Out(), "3,alice,1,(nil)")
		items := res.([]core.Value)
		testx.AssertEqual(t, len(items), 3)
		testx.AssertEqual(t, items[0], core.Value("alice"))
		testx.AssertEqual(t, items[1], core.Value("1"))
		testx.AssertEqual(t, items[2], core.Value(nil))
	})
	t.Run("all fields", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHMGet, "hmget person name age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, conn.Out(), "2,alice,25")
		items := res.([]core.Value)
		testx.AssertEqual(t, len(items), 2)
		testx.AssertEqual(t, items[0], core.Value("alice"))
		testx.AssertEqual(t, items[1], core.Value("25"))
	})
}
