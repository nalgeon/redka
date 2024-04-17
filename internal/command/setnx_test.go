package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestSetNXParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want SetNX
		err  error
	}{
		{
			name: "setnx",
			args: buildArgs("setnx"),
			want: SetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "setnx name",
			args: buildArgs("setnx", "name"),
			want: SetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "setnx name alice",
			args: buildArgs("setnx", "name", "alice"),
			want: SetNX{key: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			name: "setnx name alice 60",
			args: buildArgs("setnx", "name", "alice", "60"),
			want: SetNX{},
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*SetNX)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.value, test.want.value)
			}
		})
	}
}

func TestSetNXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*SetNX]("setnx name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*SetNX]("setnx name bob")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})
}
