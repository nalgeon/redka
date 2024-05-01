package list

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestRPushParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want RPush
		err  error
	}{
		{
			cmd:  "rpush",
			want: RPush{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "rpush key",
			want: RPush{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "rpush key elem",
			want: RPush{key: "key", elem: []byte("elem")},
			err:  nil,
		},
		{
			cmd:  "rpush key elem other",
			want: RPush{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseRPush, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.elem, test.want.elem)
			}
		})
	}
}

func TestRPushExec(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseRPush, "rpush key elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		elem, _ := db.List().Get("key", 0)
		testx.AssertEqual(t, elem.String(), "elem")
	})
	t.Run("add elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")

		cmd := redis.MustParse(ParseRPush, "rpush key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		elem, _ := db.List().Get("key", 1)
		testx.AssertEqual(t, elem.String(), "two")
	})
	t.Run("add miltiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		{
			cmd := redis.MustParse(ParseRPush, "rpush key one")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, 1)
			testx.AssertEqual(t, conn.Out(), "1")
		}
		{
			cmd := redis.MustParse(ParseRPush, "rpush key two")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, 2)
			testx.AssertEqual(t, conn.Out(), "2")
		}

		el0, _ := db.List().Get("key", 0)
		testx.AssertEqual(t, el0.String(), "one")
		el1, _ := db.List().Get("key", 1)
		testx.AssertEqual(t, el1.String(), "two")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseRPush, "rpush key elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		elem, _ := db.List().Get("key", 0)
		testx.AssertEqual(t, elem.String(), "elem")
		str, _ := db.Str().Get("key")
		testx.AssertEqual(t, str.String(), "str")
	})
}
