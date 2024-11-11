package server

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestConfigParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want Config
		err  error
	}{
		{
			cmd:  "config",
			want: Config{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "config get",
			want: Config{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "config get *",
			want: Config{subcmd: "get"},
			err:  nil,
		},
		{
			cmd:  "config set parameter value",
			want: Config{},
			err:  redis.ErrUnknownSubcmd,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseConfig, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.subcmd, test.want.subcmd)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}

func TestConfigExec(t *testing.T) {
	t.Run("config get", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseConfig, "config get *")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.Out(), "2,databases,1")
	})
}
