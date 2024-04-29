package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHDelParse(t *testing.T) {
	tests := []struct {
		name   string
		args   [][]byte
		key    string
		fields []string
		err    error
	}{
		{
			name:   "hdel",
			args:   command.BuildArgs("hdel"),
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			name:   "hdel person",
			args:   command.BuildArgs("hdel", "person"),
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			name:   "hdel person name",
			args:   command.BuildArgs("hdel", "person", "name"),
			key:    "person",
			fields: []string{"name"},
			err:    nil,
		},
		{
			name:   "hdel person name age",
			args:   command.BuildArgs("hdel", "person", "name", "age"),
			key:    "person",
			fields: []string{"name", "age"},
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*hash.HDel)
				testx.AssertEqual(t, cm.Key, test.key)
				testx.AssertEqual(t, cm.Fields, test.fields)
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

		cmd := command.MustParse[*hash.HDel]("hdel person name")
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

		cmd := command.MustParse[*hash.HDel]("hdel person name happy city")
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

		cmd := command.MustParse[*hash.HDel]("hdel person name age")
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
