package server

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDBSizeParse(t *testing.T) {
	tests := []struct {
		cmd string
		err error
	}{
		{
			cmd: "dbsize",
			err: nil,
		},
		{
			cmd: "dbsize name",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			_, err := redis.Parse(ParseDBSize, test.cmd)
			testx.AssertEqual(t, err, test.err)
		})
	}
}

func TestDBSizeExec(t *testing.T) {
	t.Run("dbsize", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		cmd := redis.MustParse(ParseDBSize, "dbsize")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.Out(), "2")
	})

	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseDBSize, "dbsize")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
