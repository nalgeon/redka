package string

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestMSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want MSet
		err  error
	}{
		{
			cmd:  "mset",
			want: MSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "mset name",
			want: MSet{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "mset name alice",
			want: MSet{items: map[string]any{"name": []byte("alice")}},
			err:  nil,
		},
		{
			cmd:  "mset name alice age",
			want: MSet{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd: "mset name alice age 25",
			want: MSet{items: map[string]any{
				"name": []byte("alice"),
				"age":  []byte("25"),
			}},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseMSet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.items, test.want.items)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestMSetExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseMSet, "mset name alice")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "alice")
	})

	t.Run("create multiple", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseMSet, "mset name alice age 25")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "alice")
		age, _ := red.Str().Get("age")
		be.Equal(t, age.String(), "25")
	})

	t.Run("create/update", func(t *testing.T) {
		red := getRedka(t)

		_ = red.Str().Set("name", "alice")

		cmd := redis.MustParse(ParseMSet, "mset name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "bob")
		age, _ := red.Str().Get("age")
		be.Equal(t, age.String(), "50")
	})

	t.Run("update multiple", func(t *testing.T) {
		red := getRedka(t)

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		cmd := redis.MustParse(ParseMSet, "mset name bob age 50")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, true)
		be.Equal(t, conn.Out(), "OK")

		name, _ := red.Str().Get("name")
		be.Equal(t, name.String(), "bob")
		age, _ := red.Str().Get("age")
		be.Equal(t, age.String(), "50")
	})
}
