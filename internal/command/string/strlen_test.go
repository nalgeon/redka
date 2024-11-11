package string

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestStrlenParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want Strlen
		err  error
	}{
		{
			cmd:  "strlen",
			want: Strlen{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "strlen name",
			want: Strlen{key: "name"},
			err:  nil,
		},
		{
			cmd:  "strlen name age",
			want: Strlen{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseStrlen, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.want.key)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestStrlenExec(t *testing.T) {
	t.Run("strlen", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseStrlen, "strlen name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 5)
		testx.AssertEqual(t, conn.Out(), "5")
	})

	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseStrlen, "strlen name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
