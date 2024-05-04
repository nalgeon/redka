package key

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestTypeParse(t *testing.T) {
	tests := []struct {
		cmd string
		key string
		err error
	}{
		{
			cmd: "type",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
		{
			cmd: "type name",
			key: "name",
			err: nil,
		},
		{
			cmd: "type name age",
			key: "",
			err: redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseType, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
			}
		})
	}
}

func TestTypeExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("kstr", "string")
	_, _ = db.List().PushBack("klist", "list")
	_, _ = db.Hash().Set("khash", "field", "hash")
	_, _ = db.ZSet().Add("kzset", "zset", 1)

	tests := []struct {
		key  string
		want string
	}{
		{key: "kstr", want: "string"},
		{key: "klist", want: "list"},
		{key: "khash", want: "hash"},
		{key: "kzset", want: "zset"},
		{key: "knone", want: "none"},
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			cmd := redis.MustParse(ParseType, "type "+test.key)
			conn := redis.NewFakeConn()
			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.want)
			testx.AssertEqual(t, conn.Out(), test.want)
		})
	}
}
