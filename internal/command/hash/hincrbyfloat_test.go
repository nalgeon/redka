package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHIncrByFloatParse(t *testing.T) {
	tests := []struct {
		name  string
		args  [][]byte
		key   string
		field string
		delta float64
		err   error
	}{
		{
			name:  "hincrbyfloat",
			args:  command.BuildArgs("hincrbyfloat"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			name:  "hincrbyfloat person",
			args:  command.BuildArgs("hincrbyfloat", "person"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			name:  "hincrbyfloat person age",
			args:  command.BuildArgs("hincrbyfloat", "person", "age"),
			key:   "",
			field: "",
			err:   redis.ErrInvalidArgNum,
		},
		{
			name:  "hincrbyfloat person age 10.5",
			args:  command.BuildArgs("hincrbyfloat", "person", "age", "10.5"),
			key:   "person",
			field: "age",
			delta: 10.5,
			err:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*hash.HIncrByFloat)
				testx.AssertEqual(t, cm.Key, test.key)
				testx.AssertEqual(t, cm.Field, test.field)
				testx.AssertEqual(t, cm.Delta, test.delta)
			}
		})
	}
}

func TestHIncrByFloatExec(t *testing.T) {
	t.Run("incr field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := command.MustParse[*hash.HIncrByFloat]("hincrbyfloat person age 10.5")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 35.5)
		testx.AssertEqual(t, conn.Out(), "35.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("35.5"))
	})
	t.Run("decr field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := command.MustParse[*hash.HIncrByFloat]("hincrbyfloat person age -10.5")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 14.5)
		testx.AssertEqual(t, conn.Out(), "14.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("14.5"))
	})
	t.Run("create field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HIncrByFloat]("hincrbyfloat person age 10.5")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10.5)
		testx.AssertEqual(t, conn.Out(), "10.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10.5"))
	})
	t.Run("create key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HIncrByFloat]("hincrbyfloat person age 10.5")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10.5)
		testx.AssertEqual(t, conn.Out(), "10.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10.5"))
	})
}
