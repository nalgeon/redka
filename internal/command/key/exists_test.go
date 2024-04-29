package key_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestExistsParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "exists",
			args: command.BuildArgs("exists"),
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "exists name",
			args: command.BuildArgs("exists", "name"),
			want: []string{"name"},
			err:  nil,
		},
		{
			name: "exists name age",
			args: command.BuildArgs("exists", "name", "age"),
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*key.Exists).Keys, test.want)
			}
		})
	}
}

func TestExistsExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 50)
	_ = db.Str().Set("city", "paris")

	tests := []struct {
		name string
		cmd  *key.Exists
		res  any
		out  string
	}{
		{
			name: "exists one",
			cmd:  command.MustParse[*key.Exists]("exists name"),
			res:  1,
			out:  "1",
		},
		{
			name: "exists all",
			cmd:  command.MustParse[*key.Exists]("exists name age"),
			res:  2,
			out:  "2",
		},
		{
			name: "exists some",
			cmd:  command.MustParse[*key.Exists]("exists name age street"),
			res:  2,
			out:  "2",
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
