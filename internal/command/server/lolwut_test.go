package server

import (
	"strings"
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/redis"
)

func TestLolwutParse(t *testing.T) {
	tests := []struct {
		cmd string
		err error
	}{
		{
			cmd: "lolwut",
			err: nil,
		},
		{
			cmd: "lolwut you ok?",
			err: nil,
		},
		{
			cmd: "lolwut is redis cool?",
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseLolwut, test.cmd)
			be.Equal(t, err, test.err)
			if err != nil {
				be.Equal(t, cmd, Lolwut{})
			}
		})
	}
}

func TestLolwutExec(t *testing.T) {
	t.Run("lolwut", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseLolwut, "lolwut you ok?")
		conn := redis.NewFakeConn()
		_, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, len(conn.Out()) >= 3, true)
	})

	t.Run("empty", func(t *testing.T) {
		red := getRedka(t)

		cmd := redis.MustParse(ParseLolwut, "lolwut")
		conn := redis.NewFakeConn()
		_, err := cmd.Run(conn, red)
		be.Err(t, err, nil)
		be.Equal(t, strings.HasPrefix(conn.Out(), "Ask me a question"), true)
	})
}
