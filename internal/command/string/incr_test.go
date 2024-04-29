package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.Incr
		err  error
	}{
		{
			name: "incr",
			args: command.BuildArgs("incr"),
			want: str.Incr{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "incr age",
			args: command.BuildArgs("incr", "age"),
			want: str.Incr{Key: "age", Delta: 1},
			err:  nil,
		},
		{
			name: "incr age 42",
			args: command.BuildArgs("incr", "age", "42"),
			want: str.Incr{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.Incr)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Delta, test.want.Delta)
			}
		})
	}
}

func TestIncrExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := command.MustParse[*str.Incr]("incr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 1)
	})

	t.Run("incr", func(t *testing.T) {
		_ = db.Str().Set("age", "25")

		cmd := command.MustParse[*str.Incr]("incr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 26)
		testx.AssertEqual(t, conn.Out(), "26")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 26)
	})
}
