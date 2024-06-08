package hash

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want HSet
		err  error
	}{
		{
			cmd:  "hset",
			want: HSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hset person",
			want: HSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hset person name",
			want: HSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hset person name alice",
			want: HSet{key: "person", items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			cmd:  "hset person name alice age",
			want: HSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd: "hset person name alice age 25",
			want: HSet{key: "person", items: map[string]any{
				"name": []byte("alice"),
				"age":  []byte("25"),
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHSet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.items, test.want.items)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestHSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseHSet, "hset person name alice")
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

		cmd := redis.MustParse(ParseHSet, "hset person name alice age 25")
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

		cmd := redis.MustParse(ParseHSet, "hset person name bob age 50")
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

		cmd := redis.MustParse(ParseHSet, "hset person name bob age 50")
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
