package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHDelParse(t *testing.T) {
	tests := []struct {
		cmd    string
		key    string
		fields []string
		err    error
	}{
		{
			cmd:    "hdel",
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hdel person",
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hdel person name",
			key:    "person",
			fields: []string{"name"},
			err:    nil,
		},
		{
			cmd:    "hdel person name age",
			key:    "person",
			fields: []string{"name", "age"},
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHDel, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
				testx.AssertEqual(t, cmd.fields, test.fields)
			}
		})
	}
}

func TestHDelExec(t *testing.T) {
	t.Run("one", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHDel, "hdel person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.Exists(), false)
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
	})
	t.Run("some", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)
		_, _ = db.Hash().Set("person", "happy", true)

		cmd := redis.MustParse(ParseHDel, "hdel person name happy city")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.Exists(), false)
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
		happy, _ := db.Hash().Get("person", "happy")
		testx.AssertEqual(t, happy.Exists(), false)
	})
	t.Run("all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHDel, "hdel person name age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.Exists(), false)
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.Exists(), false)
	})
}
