package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestMSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want MSet
		err  error
	}{
		{
			name: "mset",
			args: buildArgs("mset"),
			want: MSet{},
			err:  ErrInvalidArgNum("mset"),
		},
		{
			name: "mset name",
			args: buildArgs("mset", "name"),
			want: MSet{},
			err:  ErrInvalidArgNum("mset"),
		},
		{
			name: "mset name alice",
			args: buildArgs("mset", "name", "alice"),
			want: MSet{items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			name: "mset name alice age",
			args: buildArgs("mset", "name", "alice", "age"),
			want: MSet{},
			err:  ErrInvalidArgNum("mset"),
		},
		{
			name: "mset name alice age 25",
			args: buildArgs("mset", "name", "alice", "age", "25"),
			want: MSet{items: map[string]any{
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
				cm := cmd.(*MSet)
				testx.AssertEqual(t, cm.items, test.want.items)
			}
		})
	}
}

func TestMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*MSet]("mset name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*MSet]("mset name alice age 25")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*MSet]("mset name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		cmd := mustParse[*MSet]("mset name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "50")
	})
}
