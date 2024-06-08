package key

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestRenameParse(t *testing.T) {
	tests := []struct {
		cmd    string
		key    string
		newKey string
		err    error
	}{
		{
			cmd:    "rename",
			key:    "",
			newKey: "",
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "rename name",
			key:    "",
			newKey: "",
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "rename name title",
			key:    "name",
			newKey: "title",
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseRename, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
				testx.AssertEqual(t, cmd.newKey, test.newKey)
			} else {
				testx.AssertEqual(t, cmd, Rename{})
			}
		})
	}
}

func TestRenameExec(t *testing.T) {
	t.Run("create new", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseRename, "rename name title")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
	})

	t.Run("replace existing", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("title", "bob")

		cmd := redis.MustParse(ParseRename, "rename name title")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("title")
		testx.AssertEqual(t, val.String(), "alice")
	})

	t.Run("rename to self", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseRename, "rename name name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("name")
		testx.AssertEqual(t, val.String(), "alice")
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("title", "bob")

		cmd := redis.MustParse(ParseRename, "rename name title")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertEqual(t, err, core.ErrNotFound)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), redis.ErrNotFound.Error()+" (rename)")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
		key, _ = db.Key().Get("title")
		testx.AssertEqual(t, key.Exists(), true)
		val, _ := db.Str().Get("title")
		testx.AssertEqual(t, val.String(), "bob")
	})
}
