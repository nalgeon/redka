package zset

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZCountParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want ZCount
		err  error
	}{
		{
			cmd:  "zcount",
			want: ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zcount key",
			want: ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zcount key 1.1",
			want: ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "zcount key 1.1 2.2",
			want: ZCount{key: "key", min: 1.1, max: 2.2},
			err:  nil,
		},
		{
			cmd:  "zcount key 1.1 2.2 3.3",
			want: ZCount{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZCount, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.min, test.want.min)
				testx.AssertEqual(t, cmd.max, test.want.max)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestZCountExec(t *testing.T) {
	t.Run("zcount", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)
		_, _ = db.ZSet().Add("key", "thr", 33)

		cmd := redis.MustParse(ParseZCount, "zcount key 15 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")
	})
	t.Run("inclusive", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)
		_, _ = db.ZSet().Add("key", "thr", 33)

		cmd := redis.MustParse(ParseZCount, "zcount key 11 33")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.Out(), "3")
	})
	t.Run("zero", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)
		_, _ = db.ZSet().Add("key", "thr", 33)

		cmd := redis.MustParse(ParseZCount, "zcount key 44 55")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseZCount, "zcount key 11 33")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := redis.MustParse(ParseZCount, "zcount key 11 33")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
