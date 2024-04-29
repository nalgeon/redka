package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestIncrByParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.IncrBy
		err  error
	}{
		{
			name: "incrby",
			args: command.BuildArgs("incrby"),
			want: str.IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "incrby age",
			args: command.BuildArgs("incrby", "age"),
			want: str.IncrBy{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "incrby age 42",
			args: command.BuildArgs("incrby", "age", "42"),
			want: str.IncrBy{Key: "age", Delta: 42},
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

func TestIncrByExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	t.Run("create", func(t *testing.T) {
		cmd := command.MustParse[*str.IncrBy]("incrby age 42")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 42)
		testx.AssertEqual(t, conn.Out(), "42")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 42)
	})

	t.Run("incrby", func(t *testing.T) {
		_ = db.Str().Set("age", "25")

		cmd := command.MustParse[*str.IncrBy]("incrby age 42")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 67)
		testx.AssertEqual(t, conn.Out(), "67")

		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 67)
	})
}
