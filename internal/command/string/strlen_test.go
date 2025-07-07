package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
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
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
			} else {
				be.Equal(t, cmd, test.want)
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
		be.Err(t, err, nil)
		be.Equal(t, res, 5)
		be.Equal(t, conn.Out(), "5")
	})

	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseStrlen, "strlen name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")
	})
}
