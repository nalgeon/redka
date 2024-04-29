package key_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
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
			args:   command.BuildArgs("rename"),
			key:    "",
			newKey: "",
			err:    redis.ErrInvalidArgNum,
		},
		{
			name:   "rename name",
			args:   command.BuildArgs("rename", "name"),
			key:    "",
			newKey: "",
			err:    redis.ErrInvalidArgNum,
		},
		{
			name:   "rename name title",
			args:   command.BuildArgs("rename", "name", "title"),
			key:    "name",
			newKey: "title",
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*key.Rename).Key, test.key)
				testx.AssertEqual(t, cmd.(*key.Rename).NewKey, test.newKey)
			}
		})
	}
}

func TestRenameExec(t *testing.T) {
	t.Run("create new", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*key.Rename]("rename name title")
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

		cmd := command.MustParse[*key.Rename]("rename name title")
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

		cmd := command.MustParse[*key.Rename]("rename name name")
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

		cmd := command.MustParse[*key.Rename]("rename name title")
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
