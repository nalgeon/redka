package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
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
			args:  buildArgs("hincrbyfloat"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hincrbyfloat person",
			args:  buildArgs("hincrbyfloat", "person"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hincrbyfloat person age",
			args:  buildArgs("hincrbyfloat", "person", "age"),
			key:   "",
			field: "",
			err:   ErrInvalidArgNum,
		},
		{
			name:  "hincrbyfloat person age 10.5",
			args:  buildArgs("hincrbyfloat", "person", "age", "10.5"),
			key:   "person",
			field: "age",
			delta: 10.5,
			err:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*HIncrByFloat)
				testx.AssertEqual(t, cm.key, test.key)
				testx.AssertEqual(t, cm.field, test.field)
				testx.AssertEqual(t, cm.delta, test.delta)
			}
		})
	}
}

func TestHIncrByFloatExec(t *testing.T) {
	t.Run("incr field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HIncrByFloat]("hincrbyfloat person age 10.5")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 35.5)
		testx.AssertEqual(t, conn.out(), "35.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("35.5"))
	})
	t.Run("decr field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HIncrByFloat]("hincrbyfloat person age -10.5")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 14.5)
		testx.AssertEqual(t, conn.out(), "14.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("14.5"))
	})
	t.Run("create field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HIncrByFloat]("hincrbyfloat person age 10.5")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10.5)
		testx.AssertEqual(t, conn.out(), "10.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10.5"))
	})
	t.Run("create key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*HIncrByFloat]("hincrbyfloat person age 10.5")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)

		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 10.5)
		testx.AssertEqual(t, conn.out(), "10.5")

		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age, core.Value("10.5"))
	})
}
