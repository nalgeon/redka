package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want Incr
		err  error
	}{
		{
			name: "incr",
			args: buildArgs("incr"),
			want: Incr{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "incr age",
			args: buildArgs("incr", "age"),
			want: Incr{key: "age", delta: 1},
			err:  nil,
		},
		{
			name: "incr age 42",
			args: buildArgs("incr", "age", "42"),
			want: Incr{},
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*Incr)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.delta, test.want.delta)
			}
		})
	}
}

func TestIncrExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := mustParse[*Incr]("incr age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "1")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 1)
	})

	t.Run("incr", func(t *testing.T) {
		_ = db.Str().Set("age", "25")

		cmd := mustParse[*Incr]("incr age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 26)
		testx.AssertEqual(t, conn.out(), "26")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 26)
	})
}
