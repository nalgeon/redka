package key

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
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
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.newKey, test.newKey)
			} else {
				be.Equal(t, cmd, Rename{})
			}
		})
	}
}

func TestRenameExec(t *testing.T) {
	t.Run("create new", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseRename, "rename name title")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		key, _ := red.Key().Get("name")
		be.Equal(t, key.Exists(), false)
		key, _ = red.Key().Get("title")
		be.Equal(t, key.Exists(), true)
	})

	t.Run("replace existing", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("title", "bob")

		cmd := redis.MustParse(ParseRename, "rename name title")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		key, _ := red.Key().Get("name")
		be.Equal(t, key.Exists(), false)
		key, _ = red.Key().Get("title")
		be.Equal(t, key.Exists(), true)
		val, _ := red.Str().Get("title")
		be.Equal(t, val.String(), "alice")
	})

	t.Run("rename to self", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseRename, "rename name name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		key, _ := red.Key().Get("name")
		be.Equal(t, key.Exists(), true)
		val, _ := red.Str().Get("name")
		be.Equal(t, val.String(), "alice")
	})

	t.Run("not found", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("title", "bob")

		cmd := redis.MustParse(ParseRename, "rename name title")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Equal(t, err, core.ErrNotFound)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), redis.ErrNotFound.Error()+" (rename)")

		key, _ := red.Key().Get("name")
		be.Equal(t, key.Exists(), false)
		key, _ = red.Key().Get("title")
		be.Equal(t, key.Exists(), true)
		val, _ := red.Str().Get("title")
		be.Equal(t, val.String(), "bob")
	})
}
