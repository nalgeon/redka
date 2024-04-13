package command

import (
	"strings"
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestRenameNXParse(t *testing.T) {
	tests := []struct {
		name   string
		args   [][]byte
		key    string
		newKey string
		err    error
	}{
		{
			name:   "renamenx",
			args:   buildArgs("renamenx"),
			key:    "",
			newKey: "",
			err:    ErrInvalidArgNum,
		},
		{
			name:   "renamenx name",
			args:   buildArgs("renamenx", "name"),
			key:    "",
			newKey: "",
			err:    ErrInvalidArgNum,
		},
		{
			name:   "renamenx name title",
			args:   buildArgs("renamenx", "name", "title"),
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
				testx.AssertEqual(t, cmd.(*RenameNX).key, test.key)
			}
		})
	}
}

func TestRenameNXExec(t *testing.T) {
	t.Run("create new", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*RenameNX]("renamenx name title")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

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

		cmd := mustParse[*RenameNX]("renamenx name title")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("name")
		testx.AssertEqual(t, val.String(), "alice")
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ = db.Str().Get("title")
		testx.AssertEqual(t, val.String(), "bob")
	})

	t.Run("rename to self", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*RenameNX]("renamenx name name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("name")
		testx.AssertEqual(t, val.String(), "alice")
	})

	t.Run("not found", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()

		_ = db.Str().Set("title", "bob")

		cmd := mustParse[*RenameNX]("renamenx name title")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, db)
		testx.AssertEqual(t, err, core.ErrNotFound)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, strings.HasPrefix(conn.out(), ErrNotFound.Error()), true)

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("title")
		testx.AssertEqual(t, val.String(), "bob")
	})
}
