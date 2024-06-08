package key

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDelParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want []string
		err  error
	}{
		{
			cmd:  "del",
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "del name",
			want: []string{"name"},
			err:  nil,
		},
		{
			cmd:  "del name age",
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseDel, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.keys, test.want)
			} else {
				testx.AssertEqual(t, cmd, Del{})
			}
		})
	}
}

func TestDelExec(t *testing.T) {
	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "del name",
			res: 1,
			out: "1",
		},
		{
			cmd: "del name age",
			res: 2,
			out: "2",
		},
		{
			cmd: "del name age street",
			res: 2,
			out: "2",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			db, red := getDB(t)
			defer db.Close()

			_ = db.Str().Set("name", "alice")
			_ = db.Str().Set("age", 50)
			_ = db.Str().Set("city", "paris")

			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseDel, test.cmd)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)

			_, err = db.Str().Get("name")
			testx.AssertErr(t, err, core.ErrNotFound)
			city, _ := db.Str().Get("city")
			testx.AssertEqual(t, city.String(), "paris")
		})
	}
}
