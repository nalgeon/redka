package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
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
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.src, test.want.src)
				be.Equal(t, cmd.dst, test.want.dst)
			} else {
				be.Equal(t, cmd, test.want)
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
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
	t.Run("pop elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("src", "elem")

		cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value("elem"))
		be.Equal(t, conn.Out(), "elem")

		count, _ := db.List().Len("src")
		be.Equal(t, count, 0)
		count, _ = db.List().Len("dst")
		be.Equal(t, count, 1)
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
			be.Err(t, err, nil)
			be.Equal(t, res.(core.Value), core.Value("thr"))
			be.Equal(t, conn.Out(), "thr")
		}
		{
			cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res.(core.Value), core.Value("two"))
			be.Equal(t, conn.Out(), "two")
		}

		count, _ := db.List().Len("src")
		be.Equal(t, count, 1)
		count, _ = db.List().Len("dst")
		be.Equal(t, count, 2)
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
			be.Err(t, err, nil)
			be.Equal(t, res.(core.Value), core.Value("thr"))
			be.Equal(t, conn.Out(), "thr")
		}
		{
			cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src src")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res.(core.Value), core.Value("two"))
			be.Equal(t, conn.Out(), "two")
		}

		elems, _ := db.List().Range("src", 0, -1)
		be.Equal(t, elems[0].String(), "two")
		be.Equal(t, elems[1].String(), "thr")
		be.Equal(t, elems[2].String(), "one")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("src", "str")

		cmd := redis.MustParse(ParseRPopLPush, "rpoplpush src dst")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res.(core.Value), core.Value(nil))
		be.Equal(t, conn.Out(), "(nil)")
	})
}
