package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestHSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want HSet
		err  error
	}{
		{
			name: "hset",
			args: buildArgs("hset"),
			want: HSet{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "hset person",
			args: buildArgs("hset", "person"),
			want: HSet{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "hset person name",
			args: buildArgs("hset", "person", "name"),
			want: HSet{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "hset person name alice",
			args: buildArgs("hset", "person", "name", "alice"),
			want: HSet{key: "person", items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			name: "hset person name alice age",
			args: buildArgs("hset", "person", "name", "alice", "age"),
			want: HSet{},
			err:  ErrSyntaxError,
		},
		{
			name: "hset person name alice age 25",
			args: buildArgs("hset", "person", "name", "alice", "age", "25"),
			want: HSet{key: "person", items: map[string]any{
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
				cm := cmd.(*HSet)
				testx.AssertEqual(t, cm.items, test.want.items)
			}
		})
	}
}

func TestHSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*HSet]("hset person name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*HSet]("hset person name alice age 25")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HSet]("hset person name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HSet]("hset person name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})
}
