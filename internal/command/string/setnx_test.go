package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSetNXParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SetNX
		err  error
	}{
		{
			cmd:  "setnx",
			want: SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "setnx name",
			want: SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "setnx name alice",
			want: SetNX{key: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			cmd:  "setnx name alice 60",
			want: SetNX{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSetNX, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
				testx.AssertEqual(t, cmd.value, test.want.value)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestSetNXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSetNX, "setnx name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "1")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseSetNX, "setnx name bob")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.Out(), "0")

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})
}
