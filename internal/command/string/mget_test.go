package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestMGetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "mget",
			args: command.BuildArgs("mget"),
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "mget name",
			args: command.BuildArgs("mget", "name"),
			want: []string{"name"},
			err:  nil,
		},
		{
			name: "mget name age",
			args: command.BuildArgs("mget", "name", "age"),
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.MGet)
				testx.AssertEqual(t, cm.Keys, test.want)
			}
		})
	}
}

func TestMGetExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	tests := []struct {
		name string
		cmd  *str.MGet
		res  any
		out  string
	}{
		{
			name: "single key",
			cmd:  command.MustParse[*str.MGet]("mget name"),
			res:  []core.Value{core.Value("alice")},
			out:  "1,alice",
		},
		{
			name: "multiple keys",
			cmd:  command.MustParse[*str.MGet]("mget name age"),
			res:  []core.Value{core.Value("alice"), core.Value("25")},
			out:  "2,alice,25",
		},
		{
			name: "some not found",
			cmd:  command.MustParse[*str.MGet]("mget name city age"),
			res:  []core.Value{core.Value("alice"), core.Value(nil), core.Value("25")},
			out:  "3,alice,(nil),25",
		},
		{
			name: "all not found",
			cmd:  command.MustParse[*str.MGet]("mget one two"),
			res:  []core.Value{core.Value(nil), core.Value(nil)},
			out:  "2,(nil),(nil)",
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
