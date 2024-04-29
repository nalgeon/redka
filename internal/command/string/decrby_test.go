package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDecrByParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.IncrBy
		err  error
	}{
		{
			name: "decrby",
			args: command.BuildArgs("decrby"),
			want: str.IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "decrby age",
			args: command.BuildArgs("decrby", "age"),
			want: str.IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "decrby age 42",
			args: command.BuildArgs("decrby", "age", "42"),
			want: str.IncrBy{Key: "age", Delta: -42},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.IncrBy)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Delta, test.want.Delta)
			}
		})
	}
}

func TestDecrByExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := command.MustParse[*str.IncrBy]("decrby age 12")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -12)
		testx.AssertEqual(t, conn.Out(), "-12")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), -12)
	})

	t.Run("decrby", func(t *testing.T) {
		_ = db.Str().Set("age", "25")

		cmd := command.MustParse[*str.IncrBy]("decrby age 12")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 13)
		testx.AssertEqual(t, conn.Out(), "13")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 13)
	})
}
