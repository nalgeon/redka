package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestHMSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want HMSet
		err  error
	}{
		{
			name: "hmset",
			args: buildArgs("hmset"),
			want: HMSet{},
			err:  ErrInvalidArgNum("hmset"),
		},
		{
			name: "hmset person",
			args: buildArgs("hmset", "person"),
			want: HMSet{},
			err:  ErrInvalidArgNum("hmset"),
		},
		{
			name: "hmset person name",
			args: buildArgs("hmset", "person", "name"),
			want: HMSet{},
			err:  ErrInvalidArgNum("hmset"),
		},
		{
			name: "hmset person name alice",
			args: buildArgs("hmset", "person", "name", "alice"),
			want: HMSet{key: "person", items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			name: "hmset person name alice age",
			args: buildArgs("hmset", "person", "name", "alice", "age"),
			want: HMSet{},
			err:  ErrInvalidArgNum("hmset"),
		},
		{
			name: "hmset person name alice age 25",
			args: buildArgs("hmset", "person", "name", "alice", "age", "25"),
			want: HMSet{key: "person", items: map[string]any{
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
				cm := cmd.(*HMSet)
				testx.AssertEqual(t, cm.items, test.want.items)
			}
		})
	}
}

func TestHMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*HMSet]("hmset person name alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		cmd := mustParse[*HMSet]("hmset person name alice age 25")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := mustParse[*HMSet]("hmset person name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := mustParse[*HMSet]("hmset person name bob age 50")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})
}
