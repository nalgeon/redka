package conn

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestEchoParse(t *testing.T) {
	tests := []struct {
		cmd  string
		args [][]byte
		want []string
		err  error
	}{
		{
			cmd:  "echo",
			want: []string{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "echo hello",
			want: []string{"hello"},
			err:  nil,
		},
		{
			cmd:  "echo one two",
			want: []string{"one", "two"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseEcho, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.parts, test.want)
			}
		})
	}
}

func TestEchoExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "echo hello",
			res: "hello",
			out: "hello",
		},
		{
			cmd: "echo one two",
			res: "one two",
			out: "one two",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseEcho, test.cmd)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
