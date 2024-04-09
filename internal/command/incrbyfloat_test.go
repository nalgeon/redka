package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrByFloatParse(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		args [][]byte
		want IncrByFloat
		err  error
	}{
		{
			name: "incrbyfloat",
			args: buildArgs("incrbyfloat"),
			want: IncrByFloat{},
			err:  ErrInvalidArgNum("incrbyfloat"),
		},
		{
			name: "incrbyfloat age",
			args: buildArgs("incrbyfloat", "age"),
			want: IncrByFloat{},
			err:  ErrInvalidArgNum("incrbyfloat"),
		},
		{
			name: "incrbyfloat age 4.2",
			args: buildArgs("incrbyfloat", "age", "4.2"),
			want: IncrByFloat{key: "age", delta: 4.2},
			err:  nil,
		},
		{
			name: "incrbyfloat age -4.2",
			args: buildArgs("incrbyfloat", "age", "4.2"),
			want: IncrByFloat{key: "age", delta: 4.2},
			err:  nil,
		},
		{
			name: "incrbyfloat age 2.0e2",
			args: buildArgs("incrbyfloat", "age", "2.0e2"),
			want: IncrByFloat{key: "age", delta: 2.0e2},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*IncrByFloat)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.delta, test.want.delta)
			}
		})
	}
}

func TestIncrByFloatExec(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *IncrByFloat
		res  any
		out  string
	}{
		{
			name: "positive",
			cmd:  mustParse[*IncrByFloat]("incrbyfloat age 4.2"),
			res:  29.2,
			out:  "29.2",
		},
		{
			name: "negative",
			cmd:  mustParse[*IncrByFloat]("incrbyfloat age -4.2"),
			res:  20.8,
			out:  "20.8",
		},
		{
			name: "zero",
			cmd:  mustParse[*IncrByFloat]("incrbyfloat age 0"),
			res:  25.0,
			out:  "25",
		},
		{
			name: "exponential",
			cmd:  mustParse[*IncrByFloat]("incrbyfloat age 2.0e2"),
			res:  225.0,
			out:  "225",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_ = db.Str().Set("age", 25)

			conn := new(fakeConn)
			res, err := test.cmd.Run(conn, db)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.out(), test.out)
		})
	}

}
