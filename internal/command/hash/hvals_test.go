package hash

import (
	"slices"
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestHValsParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "hvals",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "hvals person",
			key: "person",
			err: nil,
		},
		{
			cmd: "hvals person name",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHVals, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
			} else {
				be.Equal(t, cmd, HVals{})
			}
		})
	}
}

func TestHValsExec(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		red := getRedka(t)
		_, _ = red.Hash().Set("person", "name", "alice")
		_, _ = red.Hash().Set("person", "age", 25)

		cmd := redis.MustParse(ParseHVals, "hvals person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		var got []string
		for _, val := range res.([]core.Value) {
			got = append(got, val.String())
		}
		slices.Sort(got)
		be.Equal(t, got, []string{"25", "alice"})
		be.True(t, conn.Out() == "2,25,alice" || conn.Out() == "2,alice,25")
	})
	t.Run("key not found", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseHVals, "hvals person")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)

		be.Err(t, err, nil)
		be.Equal(t, res.([]core.Value), []core.Value{})
		be.Equal(t, conn.Out(), "0")
	})
}
