package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestGetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want string
		err  error
	}{
		{
			name: "get",
			args: command.BuildArgs("get"),
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "get name",
			args: command.BuildArgs("get", "name"),
			want: "name",
			err:  nil,
		},
		{
			name: "get name age",
			args: command.BuildArgs("get", "name", "age"),
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*str.Get).Key, test.want)
			}
		})
	}
}

func TestGetExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")

	tests := []struct {
		name string
		cmd  *str.Get
		res  any
		out  string
	}{
		{
			name: "get found",
			cmd:  command.MustParse[*str.Get]("get name"),
			res:  core.Value("alice"),
			out:  "alice",
		},
		{
			name: "get not found",
			cmd:  command.MustParse[*str.Get]("get age"),
			res:  core.Value(nil),
			out:  "(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := redis.NewFakeConn()
			res, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
