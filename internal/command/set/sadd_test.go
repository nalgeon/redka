package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSAddParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SAdd
		err  error
	}{
		{
			cmd:  "sadd",
			want: SAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sadd key",
			want: SAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sadd key one",
			want: SAdd{key: "key", members: []any{"one"}},
			err:  nil,
		},
		{
			cmd:  "sadd key one two",
			want: SAdd{key: "key", members: []any{"one", "two"}},
			err:  nil,
		},
		{
			cmd:  "sadd key one two thr",
			want: SAdd{key: "key", members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSAdd, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.members, test.want.members)
			}
		})
	}
}

func TestSAddExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSAdd, "sadd key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		items, _ := db.Set().Items("key")
		testx.AssertEqual(t, items, []core.Value{core.Value("one")})
	})
	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSAdd, "sadd key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		items, _ := db.Set().Items("key")
		sortValues(items)
		testx.AssertEqual(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one")

		cmd := redis.MustParse(ParseSAdd, "sadd key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		items, _ := db.Set().Items("key")
		sortValues(items)
		testx.AssertEqual(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key", "one", "two")

		cmd := redis.MustParse(ParseSAdd, "sadd key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		items, _ := db.Set().Items("key")
		sortValues(items)
		testx.AssertEqual(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseSAdd, "sadd key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), core.ErrKeyType.Error()+" (sadd)")
	})
}
