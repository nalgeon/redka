package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
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
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.value, test.want.value)
			} else {
				testx.AssertEqual(t, cmd, test.want)
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
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseGetSet, "getset name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("alice"))
		testx.AssertEqual(t, conn.Out(), "alice")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
	})
}
