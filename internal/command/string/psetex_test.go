package string

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestPSetEXParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SetEX
		err  error
	}{
		{
			cmd:  "psetex",
			want: SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "psetex name",
			want: SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "psetex name alice",
			want: SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "psetex name alice 60",
			want: SetEX{},
			err:  redis.ErrInvalidInt,
		},
		{
			cmd:  "psetex name 60 alice",
			want: SetEX{key: "name", value: []byte("alice"), ttl: 60 * time.Millisecond},
			err:  nil,
		},
	}

	parse := func(b redis.BaseCmd) (*SetEX, error) {
		return ParseSetEX(b, 1)
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(parse, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.value, test.want.value)
				testx.AssertEqual(t, cmd.ttl, test.want.ttl)
			}
		})
	}
}

func TestPSetEXExec(t *testing.T) {
	parse := func(b redis.BaseCmd) (*SetEX, error) {
		return ParseSetEX(b, 1)
	}

	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(parse, "psetex name 60000 alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(parse, "psetex name 60000 bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
	})

	t.Run("change ttl", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := redis.MustParse(parse, "psetex name 10000 bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "OK")

		expireAt := time.Now().Add(10 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
	})

}
