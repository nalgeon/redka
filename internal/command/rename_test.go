package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestRenameParse(t *testing.T) {
	tests := []struct {
		name   string
		args   [][]byte
		key    string
		newKey string
		err    error
	}{
		{
			name:   "rename",
			args:   buildArgs("rename"),
			key:    "",
			newKey: "",
			err:    ErrInvalidArgNum("rename"),
		},
		{
			name:   "rename name",
			args:   buildArgs("rename", "name"),
			key:    "",
			newKey: "",
			err:    ErrInvalidArgNum("rename"),
		},
		{
			name:   "rename name title",
			args:   buildArgs("rename", "name", "title"),
			key:    "name",
			newKey: "title",
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Rename).key, test.key)
			}
		})
	}
}

func TestRenameExec(t *testing.T) {
	t.Run("create new", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Rename]("rename name title")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
	})

	t.Run("replace existing", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("title", "bob")

		cmd := mustParse[*Rename]("rename name title")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("title")
		testx.AssertEqual(t, val.String(), "alice")
	})

	t.Run("rename to self", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Rename]("rename name name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("name")
		testx.AssertEqual(t, val.String(), "alice")
	})

	t.Run("not found", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("title", "bob")

		cmd := mustParse[*Rename]("rename name title")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertEqual(t, err, core.ErrKeyNotFound)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), ErrKeyNotFound.Error())

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("title")
		testx.AssertEqual(t, val.String(), "bob")
	})
}
