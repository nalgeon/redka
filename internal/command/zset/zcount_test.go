package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZCountParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZCount
		err  error
	}{
		{
			name: "zcount",
			args: command.BuildArgs("zcount"),
			want: zset.ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zcount key",
			args: command.BuildArgs("zcount", "key"),
			want: zset.ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zcount key 11",
			args: command.BuildArgs("zcount", "key", "11"),
			want: zset.ZCount{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zcount key 11 22",
			args: command.BuildArgs("zcount", "key", "1.1", "2.2"),
			want: zset.ZCount{Key: "key", Min: 1.1, Max: 2.2},
			err:  nil,
		},
		{
			name: "zcount key 11 22 33",
			args: command.BuildArgs("zcount", "key", "1.1", "2.2", "3.3"),
			want: zset.ZCount{},
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZCount)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Min, test.want.Min)
				testx.AssertEqual(t, cm.Max, test.want.Max)
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

		cmd := command.MustParse[*zset.ZCount]("zcount key 15 25")
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

		cmd := command.MustParse[*zset.ZCount]("zcount key 11 33")
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

		cmd := command.MustParse[*zset.ZCount]("zcount key 44 55")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZCount]("zcount key 11 33")
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

		cmd := command.MustParse[*zset.ZCount]("zcount key 11 33")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
