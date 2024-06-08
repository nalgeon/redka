package key

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestTTLParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "ttl",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "ttl name",
			key: "name",
			err: nil,
		},
		{
			cmd: "ttl name age",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseTTL, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
			} else {
				testx.AssertEqual(t, cmd, TTL{})
			}
		})
	}
}

func TestTTLExec(t *testing.T) {
	t.Run("has ttl", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := redis.MustParse(ParseTTL, "ttl name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 60)
		testx.AssertEqual(t, conn.Out(), "60")
	})

	t.Run("no ttl", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseTTL, "ttl name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -1)
		testx.AssertEqual(t, conn.Out(), "-1")
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseTTL, "ttl name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, -2)
		testx.AssertEqual(t, conn.Out(), "-2")
	})
}
