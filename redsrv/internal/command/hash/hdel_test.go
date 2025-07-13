package hash

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestHDelParse(t *testing.T) {
	tests := []struct {
		cmd    string
		key    string
		fields []string
		err    error
	}{
		{
			cmd:    "hdel",
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hdel person",
			key:    "",
			fields: nil,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hdel person name",
			key:    "person",
			fields: []string{"name"},
			err:    nil,
		},
		{
			cmd:    "hdel person name age",
			key:    "person",
			fields: []string{"name", "age"},
			err:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHDel, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.fields, test.fields)
			} else {
				be.Equal(t, cmd, HDel{})
			}
		})
	}
}

func TestHDelExec(t *testing.T) {
	t.Run("one", func(t *testing.T) {
		red := getRedka(t)

		_, _ = red.Hash().Set("person", "name", "alice")
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHDel, "hdel person name")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		_, err = red.Hash().Get("person", "name")
		be.Err(t, err, core.ErrNotFound)
		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age.String(), "25")
	})
	t.Run("some", func(t *testing.T) {
		red := getRedka(t)

		_, _ = red.Hash().Set("person", "name", "alice")
		_, _ = red.Hash().Set("person", "age", 25)
		_, _ = red.Hash().Set("person", "happy", true)

		cmd := redis.MustParse(ParseHDel, "hdel person name happy city")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		_, err = red.Hash().Get("person", "name")
		be.Err(t, err, core.ErrNotFound)
		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age.String(), "25")
		_, err = red.Hash().Get("person", "happy")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("all", func(t *testing.T) {
		red := getRedka(t)

		_, _ = red.Hash().Set("person", "name", "alice")
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHDel, "hdel person name age")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		_, err = red.Hash().Get("person", "name")
		be.Err(t, err, core.ErrNotFound)
		_, err = red.Hash().Get("person", "age")
		be.Err(t, err, core.ErrNotFound)
	})
}
