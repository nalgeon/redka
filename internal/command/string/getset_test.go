package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestGetSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.GetSet
		err  error
	}{
		{
			name: "getset",
			args: command.BuildArgs("getset"),
			want: str.GetSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "getset name",
			args: command.BuildArgs("getset", "name"),
			want: str.GetSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "getset name alice",
			args: command.BuildArgs("getset", "name", "alice"),
			want: str.GetSet{Key: "name", Value: []byte("alice")},
			err:  nil,
		},
		{
			name: "getset name alice 60",
			args: command.BuildArgs("getset", "name", "alice", "60"),
			want: str.GetSet{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.GetSet)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Value, test.want.Value)
			}
		})
	}
}

func TestGetSetExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*str.GetSet]("getset name alice")
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

		cmd := command.MustParse[*str.GetSet]("getset name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("alice"))
		testx.AssertEqual(t, conn.Out(), "alice")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
	})
}
