package string_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestMSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.MSet
		err  error
	}{
		{
			name: "mset",
			args: command.BuildArgs("mset"),
			want: str.MSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "mset name",
			args: command.BuildArgs("mset", "name"),
			want: str.MSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "mset name alice",
			args: command.BuildArgs("mset", "name", "alice"),
			want: str.MSet{Items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			name: "mset name alice age",
			args: command.BuildArgs("mset", "name", "alice", "age"),
			want: str.MSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "mset name alice age 25",
			args: command.BuildArgs("mset", "name", "alice", "age", "25"),
			want: str.MSet{Items: map[string]any{
				"name": []byte("alice"),
				"age":  []byte("25"),
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.MSet)
				testx.AssertEqual(t, cm.Items, test.want.Items)
			}
		})
	}
}

func TestMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*str.MSet]("mset name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*str.MSet]("mset name alice age 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*str.MSet]("mset name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		cmd := command.MustParse[*str.MSet]("mset name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Str().Get("age")
		testx.AssertEqual(t, age.String(), "50")
	})
}
