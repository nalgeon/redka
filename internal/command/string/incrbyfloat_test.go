package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrByFloatParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want IncrByFloat
		err  error
	}{
		{
			cmd:  "incrbyfloat",
			want: IncrByFloat{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrbyfloat age",
			want: IncrByFloat{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "incrbyfloat age 4.2",
			want: IncrByFloat{key: "age", delta: 4.2},
			err:  nil,
		},
		{
			cmd:  "incrbyfloat age -4.2",
			want: IncrByFloat{key: "age", delta: -4.2},
			err:  nil,
		},
		{
			cmd:  "incrbyfloat age 2.0e2",
			want: IncrByFloat{key: "age", delta: 2.0e2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseIncrByFloat, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.delta, test.want.delta)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestIncrByFloatExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "incrbyfloat age 4.2",
			res: 29.2,
			out: "29.2",
		},
		{
			cmd: "incrbyfloat age -4.2",
			res: 20.8,
			out: "20.8",
		},
		{
			cmd: "incrbyfloat age 0",
			res: 25.0,
			out: "25",
		},
		{
			cmd: "incrbyfloat age 2.0e2",
			res: 225.0,
			out: "225",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			_ = db.Str().Set("age", 25)

			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseIncrByFloat, test.cmd)
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}

}
