package list

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestLInsertParse(t *testing.T) {
	tests := []struct {
		cmd   string
		want  LInsert
		index int
		err   error
	}{
		{
			cmd:  "linsert",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key before",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key before pivot",
			want: LInsert{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "linsert key before pivot elem",
			want: LInsert{key: "key", where: Before, pivot: []byte("pivot"), elem: []byte("elem")},
			err:  nil,
		},
		{
			cmd:  "linsert key after pivot elem",
			want: LInsert{key: "key", where: After, pivot: []byte("pivot"), elem: []byte("elem")},
			err:  nil,
		},
		{
			cmd:  "linsert key inplace pivot elem",
			want: LInsert{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLInsert, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.where, test.want.where)
				testx.AssertEqual(t, cmd.pivot, test.want.pivot)
				testx.AssertEqual(t, cmd.elem, test.want.elem)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestLInsertExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLInsert, "linsert key before pivot elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		_, err = db.List().Get("key", 0)
		testx.AssertEqual(t, err, core.ErrNotFound)
	})
	t.Run("insert before first", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, err := db.List().PushBack("key", "pivot")
		testx.AssertNoErr(t, err)

		cmd := redis.MustParse(ParseLInsert, "linsert key before pivot elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		elem, _ := db.List().Get("key", 0)
		testx.AssertEqual(t, elem, core.Value("elem"))
	})
	t.Run("insert before middle", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLInsert, "linsert key before thr two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")

		elem, _ := db.List().Get("key", 1)
		testx.AssertEqual(t, elem, core.Value("two"))
	})
	t.Run("insert after middle", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "thr")

		cmd := redis.MustParse(ParseLInsert, "linsert key after thr two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")

		elem, _ := db.List().Get("key", 2)
		testx.AssertEqual(t, elem, core.Value("two"))
	})
	t.Run("elem not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")
		_, _ = db.List().PushBack("key", "two")

		cmd := redis.MustParse(ParseLInsert, "linsert key before thr two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -1)
		testx.AssertEqual(t, conn.Out(), "-1")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseLInsert, "linsert key before pivot elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
