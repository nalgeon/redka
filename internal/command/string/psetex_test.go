package string_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestPSetEXParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.SetEX
		err  error
	}{
		{
			name: "psetex",
			args: command.BuildArgs("psetex"),
			want: str.SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "psetex name",
			args: command.BuildArgs("psetex", "name"),
			want: str.SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "psetex name alice",
			args: command.BuildArgs("psetex", "name", "alice"),
			want: str.SetEX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "psetex name alice 60",
			args: command.BuildArgs("psetex", "name", "alice", "60"),
			want: str.SetEX{},
			err:  redis.ErrInvalidInt,
		},
		{
			name: "psetex name 60 alice",
			args: command.BuildArgs("psetex", "name", "60", "alice"),
			want: str.SetEX{Key: "name", Value: []byte("alice"), TTL: 60 * time.Millisecond},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*str.SetEX)
				testx.AssertEqual(t, cm.Key, test.want.Key)
				testx.AssertEqual(t, cm.Value, test.want.Value)
				testx.AssertEqual(t, cm.TTL, test.want.TTL)
			}
		})
	}
}

func TestPSetEXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := command.MustParse[*str.SetEX]("psetex name 60000 alice")
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

		cmd := command.MustParse[*str.SetEX]("psetex name 60000 bob")
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

		cmd := command.MustParse[*str.SetEX]("psetex name 10000 bob")
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
