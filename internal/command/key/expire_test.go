package key_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestExpireParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		ttl  time.Duration
		err  error
	}{
		{
			name: "expire",
			args: command.BuildArgs("expire"),
			key:  "",
			ttl:  0,
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "expire name",
			args: command.BuildArgs("expire", "name"),
			key:  "",
			ttl:  0,
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "expire name 60",
			args: command.BuildArgs("expire", "name", "60"),
			key:  "name",
			ttl:  60 * 1000 * time.Millisecond,
			err:  nil,
		},
		{
			name: "expire name age",
			args: command.BuildArgs("expire", "name", "age"),
			key:  "",
			ttl:  0,
			err:  redis.ErrInvalidInt,
		},
		{
			name: "expire name 60 age 60",
			args: command.BuildArgs("expire", "name", "60", "age", "60"),
			key:  "",
			ttl:  0,
			err:  redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*key.Expire).Key, test.key)
				testx.AssertEqual(t, cmd.(*key.Expire).TTL, test.ttl)
			}
		})
	}
}

func TestExpireExec(t *testing.T) {
	t.Run("create expire", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*key.Expire]("expire name 60")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("update expire", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := command.MustParse[*key.Expire]("expire name 30")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		expireAt := time.Now().Add(30 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("set to zero", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*key.Expire]("expire name 0")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
	})

	t.Run("negative", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*key.Expire]("expire name -10")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := command.MustParse[*key.Expire]("expire age 60")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")

		key, _ := db.Key().Get("age")
		testx.AssertEqual(t, key.Exists(), false)
	})
}
