package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDecrParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.Incr
		err  error
	}{
		{
			name: "decr",
			args: command.BuildArgs("decr"),
			want: str.Incr{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "decr age",
			args: command.BuildArgs("decr", "age"),
			want: str.Incr{Key: "age", Delta: -1},
			err:  nil,
		},
		{
			name: "decr age 42",
			args: command.BuildArgs("decr", "age", "42"),
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

func TestDecrExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := command.MustParse[*str.Incr]("decr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -1)
		testx.AssertEqual(t, conn.Out(), "-1")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), -1)
	})

	t.Run("decr", func(t *testing.T) {
		_ = db.Str().Set("age", "25")

		cmd := command.MustParse[*str.Incr]("decr age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 24)
		testx.AssertEqual(t, conn.Out(), "24")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 24)
	})
}
