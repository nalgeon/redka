package hash

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestHMSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want HMSet
		err  error
	}{
		{
			cmd:  "hmset",
			want: HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hmset person",
			want: HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hmset person name",
			want: HMSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "hmset person name alice",
			want: HMSet{key: "person", items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			cmd:  "hmset person name alice age",
			want: HMSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd: "hmset person name alice age 25",
			want: HMSet{key: "person", items: map[string]any{
				"name": []byte("alice"),
				"age":  []byte("25"),
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHMSet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.items, test.want.items)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestHMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseHMSet, "hmset person name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Hash().Get("person", "name")
		be.Equal(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseHMSet, "hmset person name alice age 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Hash().Get("person", "name")
		be.Equal(t, name.String(), "alice")
		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")

		cmd := redis.MustParse(ParseHMSet, "hmset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Hash().Get("person", "name")
		be.Equal(t, name.String(), "bob")
		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHMSet, "hmset person name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Hash().Get("person", "name")
		be.Equal(t, name.String(), "bob")
		age, _ := red.Hash().Get("person", "age")
		be.Equal(t, age.String(), "50")
	})
}
