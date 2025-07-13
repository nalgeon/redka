package set

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestSAddParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SAdd
		err  error
	}{
		{
			cmd:  "sadd",
			want: SAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sadd key",
			want: SAdd{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sadd key one",
			want: SAdd{key: "key", members: []any{"one"}},
			err:  nil,
		},
		{
			cmd:  "sadd key one two",
			want: SAdd{key: "key", members: []any{"one", "two"}},
			err:  nil,
		},
		{
			cmd:  "sadd key one two thr",
			want: SAdd{key: "key", members: []any{"one", "two", "thr"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSAdd, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.members, test.want.members)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSAddExec(t *testing.T) {
	t.Run("create single", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSAdd, "sadd key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		items, _ := red.Set().Items("key")
		be.Equal(t, items, []core.Value{core.Value("one")})
	})
	t.Run("create multiple", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseSAdd, "sadd key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 2)
		be.Equal(t, conn.Out(), "2")

		items, _ := red.Set().Items("key")
		sortValues(items)
		be.Equal(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("create/update", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key", "one")

		cmd := redis.MustParse(ParseSAdd, "sadd key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 1)
		be.Equal(t, conn.Out(), "1")

		items, _ := red.Set().Items("key")
		sortValues(items)
		be.Equal(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("update multiple", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Set().Add("key", "one", "two")

		cmd := redis.MustParse(ParseSAdd, "sadd key one two")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, res, 0)
		be.Equal(t, conn.Out(), "0")

		items, _ := red.Set().Items("key")
		sortValues(items)
		be.Equal(t, items, []core.Value{core.Value("one"), core.Value("two")})
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red := getRedka(t)
		_ = red.Str().Set("key", "value")

		cmd := redis.MustParse(ParseSAdd, "sadd key one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, res, nil)
		be.Equal(t, conn.Out(), core.ErrKeyType.Error()+" (sadd)")
	})
}
