package list

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestRPopLPushParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want RPopLPush
		err  error
	}{
		{
			cmd:  "rpoplpush",
			want: RPopLPush{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "rpoplpush key",
			want: RPopLPush{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "rpoplpush src dst",
			want: RPopLPush{src: "src", dst: "dst"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseRPopLPush, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.src, test.want.src)
				testx.AssertEqual(t, cmd.dst, test.want.dst)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestRPopLPushExec(t *testing.T) {
	t.Run("src not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
	t.Run("pop elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("src", "elem")

		cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value("elem"))
		testx.AssertEqual(t, conn.Out(), "elem")

		count, _ := db.List().Len("src")
		testx.AssertEqual(t, count, 0)
		count, _ = db.List().Len("dst")
		testx.AssertEqual(t, count, 1)
	})
	t.Run("pop multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("src", "one")
		_, _ = db.List().PushBack("src", "two")
		_, _ = db.List().PushBack("src", "thr")

		{
			cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, core.Value("thr"))
			testx.AssertEqual(t, conn.Out(), "thr")
		}
		{
			cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, core.Value("two"))
			testx.AssertEqual(t, conn.Out(), "two")
		}

		count, _ := db.List().Len("src")
		testx.AssertEqual(t, count, 1)
		count, _ = db.List().Len("dst")
		testx.AssertEqual(t, count, 2)
	})
	t.Run("push to self", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("src", "one")
		_, _ = db.List().PushBack("src", "two")
		_, _ = db.List().PushBack("src", "thr")

		{
			cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src src")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, core.Value("thr"))
			testx.AssertEqual(t, conn.Out(), "thr")
		}
		{
			cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src src")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, core.Value("two"))
			testx.AssertEqual(t, conn.Out(), "two")
		}

		elems, _ := db.List().Range("src", 0, -1)
		testx.AssertEqual(t, elems[0].String(), "two")
		testx.AssertEqual(t, elems[1].String(), "thr")
		testx.AssertEqual(t, elems[2].String(), "one")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("src", "str")

		cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, core.Value(nil))
		testx.AssertEqual(t, conn.Out(), "(nil)")
	})
}
