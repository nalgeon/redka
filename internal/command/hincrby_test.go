package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHIncrByParse(t *testing.T) {
	tests := []struct {
		name  string
		args  [][]byte
		key   string
		field string
		delta int
		err   error
	}{
		{
			name:  "hincrby",
			args:  buildArgs("hincrby"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hincrby person",
			args:  buildArgs("hincrby", "person"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hincrby person age",
			args:  buildArgs("hincrby", "person", "age"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hincrby person age 10",
			args:  buildArgs("hincrby", "person", "age", "10"),
			key:   "person",
			field: "age",
			delta: 10,
			err:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HIncrBy)
				testx.AssertEqual(t, cm.key, test.key)
				testx.AssertEqual(t, cm.field, test.field)
				testx.AssertEqual(t, cm.delta, test.delta)
			}
		})
	}
}

func TestHIncrByExec(t *testing.T) {
	t.Run("incr field", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HIncrBy]("hincrby person age 10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 35)
		testx.AssertEqual(t, conn.out(), "35")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("35"))
	})
	t.Run("decr field", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HIncrBy]("hincrby person age -10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 15)
		testx.AssertEqual(t, conn.out(), "15")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("15"))
	})
	t.Run("create field", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HIncrBy]("hincrby person age 10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10)
		testx.AssertEqual(t, conn.out(), "10")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10"))
	})
	t.Run("create key", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*HIncrBy]("hincrby person age 10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10)
		testx.AssertEqual(t, conn.out(), "10")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10"))
	})
}
