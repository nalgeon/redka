package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestDecrParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want Incr
		err  error
	}{
		{
			name: "decr",
			args: buildArgs("decr"),
			want: Incr{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "decr age",
			args: buildArgs("decr", "age"),
			want: Incr{key: "age", delta: -1},
			err:  nil,
		},
		{
			name: "decr age 42",
			args: buildArgs("decr", "age", "42"),
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

func TestDecrExec(t *testing.T) {
	db, tx := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := mustParse[*Incr]("decr age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -1)
		testx.AssertEqual(t, conn.out(), "-1")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), -1)
	})

	t.Run("decr", func(t *testing.T) {
		_ = db.Str().Set("age", "25")

		cmd := mustParse[*Incr]("decr age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, tx)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 24)
		testx.AssertEqual(t, conn.out(), "24")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 24)
	})
}
