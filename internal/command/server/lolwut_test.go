package server

import (
	"strings"
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
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
			testx.AssertEqual(t, err, test.err)
			if err != nil {
				testx.AssertEqual(t, cmd, Lolwut{})
			}
		})
	}
}

func TestLolwutExec(t *testing.T) {
	t.Run("lolwut", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLolwut, "lolwut you ok?")
		conn := redis.NewFakeConn()
		_, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(conn.Out()) >= 3, true)
	})

	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseLolwut, "lolwut")
		conn := redis.NewFakeConn()
		_, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, strings.HasPrefix(conn.Out(), "Ask me a question"), true)
	})
}
