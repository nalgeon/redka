package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestGetSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want GetSet
		err  error
	}{
		{
			cmd:  "getset",
			want: GetSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "getset name",
			want: GetSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "getset name alice",
			want: GetSet{key: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			cmd:  "getset name alice 60",
			want: GetSet{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseGetSet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.value, test.want.value)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestGetSetExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseGetSet, "getset name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")

		name, _ := db.Str().Get("name")
		be.Equal(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseGetSet, "getset name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value("alice"))
		be.Equal(t, conn.Out(), "alice")

		name, _ := db.Str().Get("name")
		be.Equal(t, name.String(), "bob")
	})
}
