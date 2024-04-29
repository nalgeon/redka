package zset_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZAddParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want zset.ZAdd
		err  error
	}{
		{
			name: "zadd",
			args: command.BuildArgs("zadd"),
			want: zset.ZAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zadd key",
			args: command.BuildArgs("zadd", "key"),
			want: zset.ZAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zadd key one",
			args: command.BuildArgs("zadd", "key", "one"),
			want: zset.ZAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "zadd key 1 one",
			args: command.BuildArgs("zadd", "key", "1.1", "one"),
			want: zset.ZAdd{Key: "key", Items: map[any]float64{"one": 1.1}},
			err:  nil,
		},
		{
			name: "zadd key 1 one 2",
			args: command.BuildArgs("zadd", "key", "1.1", "one", "2.2"),
			want: zset.ZAdd{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "zadd key one 1.1 two 2.2",
			args: command.BuildArgs("zadd", "key", "1.1", "one", "2.2", "two"),
			want: zset.ZAdd{Key: "key", Items: map[any]float64{
				"one": 1.1,
				"two": 2.2,
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*zset.ZAdd)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Items, test.want.Items)
			}
		})
	}
}

func TestZAddExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZAdd]("zadd key 11 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 11.0)
	})
	t.Run("create multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*zset.ZAdd]("zadd key 11 one 22 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")

		one, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, one, 11.0)
		two, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, two, 22.0)
	})

	t.Run("create/update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := command.MustParse[*zset.ZAdd]("zadd key 12 one 22 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		one, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, one, 12.0)
		two, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, two, 22.0)
	})
	t.Run("update multiple", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := command.MustParse[*zset.ZAdd]("zadd key 12 one 23 two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		one, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, one, 12.0)
		two, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, two, 23.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := command.MustParse[*zset.ZAdd]("zadd key 11 one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")
	})
}
