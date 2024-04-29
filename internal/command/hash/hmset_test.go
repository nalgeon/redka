package hash_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHMSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want hash.HMSet
		err  error
	}{
		{
			name: "hmset",
			args: command.BuildArgs("hmset"),
			want: hash.HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hmset person",
			args: command.BuildArgs("hmset", "person"),
			want: hash.HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hmset person name",
			args: command.BuildArgs("hmset", "person", "name"),
			want: hash.HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "hmset person name alice",
			args: command.BuildArgs("hmset", "person", "name", "alice"),
			want: hash.HMSet{Key: "person", Items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			name: "hmset person name alice age",
			args: command.BuildArgs("hmset", "person", "name", "alice", "age"),
			want: hash.HMSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "hmset person name alice age 25",
			args: command.BuildArgs("hmset", "person", "name", "alice", "age", "25"),
			want: hash.HMSet{Key: "person", Items: map[string]any{
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
				cm := cmd.(*hash.HMSet)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Items, test.want.Items)
			}
		})
	}
}

func TestHMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HMSet]("hmset person name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*hash.HMSet]("hmset person name alice age 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.Hash().Set("person", "name", "alice")

		cmd := command.MustParse[*hash.HMSet]("hmset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "OK")

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

		cmd := command.MustParse[*hash.HMSet]("hmset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "OK")

		name, _ := db.Hash().Get("person", "name")
		testx.AssertEqual(t, name.String(), "bob")
		age, _ := db.Hash().Get("person", "age")
		testx.AssertEqual(t, age.String(), "50")
	})
}
