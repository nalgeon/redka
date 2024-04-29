package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrByFloatParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.IncrByFloat
		err  error
	}{
		{
			name: "incrbyfloat",
			args: command.BuildArgs("incrbyfloat"),
			want: str.IncrByFloat{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "incrbyfloat age",
			args: command.BuildArgs("incrbyfloat", "age"),
			want: str.IncrByFloat{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "incrbyfloat age 4.2",
			args: command.BuildArgs("incrbyfloat", "age", "4.2"),
			want: str.IncrByFloat{Key: "age", Delta: 4.2},
			err:  nil,
		},
		{
			name: "incrbyfloat age -4.2",
			args: command.BuildArgs("incrbyfloat", "age", "4.2"),
			want: str.IncrByFloat{Key: "age", Delta: 4.2},
			err:  nil,
		},
		{
			name: "incrbyfloat age 2.0e2",
			args: command.BuildArgs("incrbyfloat", "age", "2.0e2"),
			want: str.IncrByFloat{Key: "age", Delta: 2.0e2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.IncrByFloat)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Delta, test.want.Delta)
			}
		})
	}
}

func TestIncrByFloatExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *str.IncrByFloat
		res  any
		out  string
	}{
		{
			name: "positive",
			cmd:  command.MustParse[*str.IncrByFloat]("incrbyfloat age 4.2"),
			res:  29.2,
			out:  "29.2",
		},
		{
			name: "negative",
			cmd:  command.MustParse[*str.IncrByFloat]("incrbyfloat age -4.2"),
			res:  20.8,
			out:  "20.8",
		},
		{
			name: "zero",
			cmd:  command.MustParse[*str.IncrByFloat]("incrbyfloat age 0"),
			res:  25.0,
			out:  "25",
		},
		{
			name: "exponential",
			cmd:  command.MustParse[*str.IncrByFloat]("incrbyfloat age 2.0e2"),
			res:  225.0,
			out:  "225",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_ = db.Str().Set("age", 25)

			conn := redis.NewFakeConn()
			res, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}

}
