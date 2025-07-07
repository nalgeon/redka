package list

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
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
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.elem, test.want.elem)
			} else {
				be.Equal(t, cmd, test.want)
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
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		elem, _ := db.List().Get("key", 0)
		be.Equal(t, elem.String(), "elem")
	})
	t.Run("add elem", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.List().PushBack("key", "one")

		cmd := redis.MustParse(ParseRPush, "rpush key two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		elem, _ := db.List().Get("key", 1)
		be.Equal(t, elem.String(), "two")
	})
	t.Run("add miltiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		{
			cmd := redis.MustParse(ParseRPush, "rpush key one")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, 1)
			be.Equal(t, conn.Out(), "1")
		}
		{
			cmd := redis.MustParse(ParseRPush, "rpush key two")
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, 2)
			be.Equal(t, conn.Out(), "2")
		}

		el0, _ := db.List().Get("key", 0)
		be.Equal(t, el0.String(), "one")
		el1, _ := db.List().Get("key", 1)
		be.Equal(t, el1.String(), "two")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		cmd := redis.MustParse(ParseRPush, "rpush key elem")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), core.ErrKeyType.Error()+" (rpush)")
	})
}
