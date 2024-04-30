package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestMSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want MSet
		err  error
	}{
		{
			cmd:  "mset",
			want: MSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "mset name",
			want: MSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "mset name alice",
			want: MSet{items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			cmd:  "mset name alice age",
			want: MSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd: "mset name alice age 25",
			want: MSet{items: map[string]any{
				"name": []byte("alice"),
				"age":  []byte("25"),
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseMSet, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.items, test.want.items)
			}
		})
	}
}

func TestMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseMSet, "mset name alice")
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

		cmd := redis.MustParse(ParseMSet, "mset name alice age 25")
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

		cmd := redis.MustParse(ParseMSet, "mset name bob age 50")
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

		cmd := redis.MustParse(ParseMSet, "mset name bob age 50")
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
