package key

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestExistsParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want []string
		err  error
	}{
		{
			cmd:  "exists",
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "exists name",
			want: []string{"name"},
			err:  nil,
		},
		{
			cmd:  "exists name age",
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseExists, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.keys, test.want)
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
		cmd string
		res any
		out string
	}{
		{
			cmd: "exists name",
			res: 1,
			out: "1",
		},
		{
			cmd: "exists name age",
			res: 2,
			out: "2",
		},
		{
			cmd: "exists name age street",
			res: 2,
			out: "2",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseExists, test.cmd)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
