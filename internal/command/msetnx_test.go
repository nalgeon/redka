package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestMSetNXParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want MSetNX
		err  error
	}{
		{
			name: "msetnx",
			args: buildArgs("msetnx"),
			want: MSetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "msetnx name",
			args: buildArgs("msetnx", "name"),
			want: MSetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "msetnx name alice",
			args: buildArgs("msetnx", "name", "alice"),
			want: MSetNX{items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			name: "msetnx name alice age",
			args: buildArgs("msetnx", "name", "alice", "age"),
			want: MSetNX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "msetnx name alice age 25",
			args: buildArgs("msetnx", "name", "alice", "age", "25"),
			want: MSetNX{items: map[string]any{
				"name": []byte("alice"),
				"age":  []byte("25"),
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*MSetNX)
				testx.AssertEqual(t, cm.items, test.want.items)
			}
		})
	}
}

func TestMSetNXExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*MSetNX]("msetnx name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*MSetNX]("msetnx name alice age 25")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*MSetNX]("msetnx name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.Exists(), false)
	})

	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		cmd := mustParse[*MSetNX]("msetnx name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "25")
	})
}
