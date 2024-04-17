package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestDecrByParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want IncrBy
		err  error
	}{
		{
			name: "decrby",
			args: buildArgs("decrby"),
			want: IncrBy{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "decrby age",
			args: buildArgs("decrby", "age"),
			want: IncrBy{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "decrby age 42",
			args: buildArgs("decrby", "age", "42"),
			want: IncrBy{key: "age", delta: -42},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*IncrBy)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.delta, test.want.delta)
			}
		})
	}
}

func TestDecrByExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := mustParse[*IncrBy]("decrby age 12")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -12)
		testx.AssertEqual(t, conn.out(), "-12")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), -12)
	})

	t.Run("decrby", func(t *testing.T) {
		_ = db.Str().Set("age", "25")

		cmd := mustParse[*IncrBy]("decrby age 12")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 13)
		testx.AssertEqual(t, conn.out(), "13")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 13)
	})
}
