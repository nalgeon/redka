package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want hash.HSet
		err  error
	}{
		{
			name: "hset",
			args: command.BuildArgs("hset"),
			want: hash.HSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hset person",
			args: command.BuildArgs("hset", "person"),
			want: hash.HSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hset person name",
			args: command.BuildArgs("hset", "person", "name"),
			want: hash.HSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hset person name alice",
			args: command.BuildArgs("hset", "person", "name", "alice"),
			want: hash.HSet{Key: "person", Items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			name: "hset person name alice age",
			args: command.BuildArgs("hset", "person", "name", "alice", "age"),
			want: hash.HSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "hset person name alice age 25",
			args: command.BuildArgs("hset", "person", "name", "alice", "age", "25"),
			want: hash.HSet{Key: "person", Items: map[string]any{
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
				cm := cmd.(*hash.HSet)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Items, test.want.Items)
			}
		})
	}
}

func TestHSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HSet]("hset person name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HSet]("hset person name alice age 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HSet]("hset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")
		_, _ = db.Hash().Set("person", "age", 25)

		cmd := command.MustParse[*hash.HSet]("hset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})
}
