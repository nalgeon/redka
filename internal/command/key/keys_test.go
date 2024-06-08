package key

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestKeysParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want string
		err  error
	}{
		{
			cmd:  "keys",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "keys *",
			want: "*",
			err:  nil,
		},
		{
			cmd:  "keys k2*",
			want: "k2*",
			err:  nil,
		},
		{
			cmd:  "keys * k2*",
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseKeys, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.pattern, test.want)
			} else {
				testx.AssertEqual(t, cmd, Keys{})
			}
		})
	}
}

func TestKeysExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("k11", "11")
	_ = db.Str().Set("k12", "12")
	_ = db.Str().Set("k21", "21")
	_ = db.Str().Set("k22", "22")
	_ = db.Str().Set("k31", "31")

	tests := []struct {
		cmd string
		res []string
		out string
	}{
		{
			cmd: "keys *",
			res: []string{"k11", "k12", "k21", "k22", "k31"},
			out: "5,k11,k12,k21,k22,k31",
		},
		{
			cmd: "keys k2*",
			res: []string{"k21", "k22"},
			out: "2,k21,k22",
		},
		{
			cmd: "keys k12",
			res: []string{"k12"},
			out: "1,k12",
		},
		{
			cmd: "keys name",
			res: []string{},
			out: "0",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseKeys, test.cmd)
			keys, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			for i, key := range keys.([]core.Key) {
				testx.AssertEqual(t, key.Key, test.res[i])
			}
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
