package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSetNXParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.SetNX
		err  error
	}{
		{
			name: "setnx",
			args: command.BuildArgs("setnx"),
			want: str.SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "setnx name",
			args: command.BuildArgs("setnx", "name"),
			want: str.SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "setnx name alice",
			args: command.BuildArgs("setnx", "name", "alice"),
			want: str.SetNX{Key: "name", Value: []byte("alice")},
			err:  nil,
		},
		{
			name: "setnx name alice 60",
			args: command.BuildArgs("setnx", "name", "alice", "60"),
			want: str.SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.SetNX)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Value, test.want.Value)
			}
		})
	}
}

func TestSetNXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*str.SetNX]("setnx name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*str.SetNX]("setnx name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})
}
