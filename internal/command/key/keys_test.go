package key_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestKeysParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want string
		err  error
	}{
		{
			name: "keys",
			args: command.BuildArgs("keys"),
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "keys *",
			args: command.BuildArgs("keys", "*"),
			want: "*",
			err:  nil,
		},
		{
			name: "keys 2*",
			args: command.BuildArgs("keys", "k2*"),
			want: "k2*",
			err:  nil,
		},
		{
			name: "keys * k2*",
			args: command.BuildArgs("keys", "*", "k2*"),
			want: "",
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*key.Keys).Pattern, test.want)
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
		name string
		cmd  *key.Keys
		res  []string
		out  string
	}{
		{
			name: "all keys",
			cmd:  command.MustParse[*key.Keys]("keys *"),
			res:  []string{"k11", "k12", "k21", "k22", "k31"},
			out:  "5,k11,k12,k21,k22,k31",
		},
		{
			name: "some keys",
			cmd:  command.MustParse[*key.Keys]("keys k2*"),
			res:  []string{"k21", "k22"},
			out:  "2,k21,k22",
		},
		{
			name: "one key",
			cmd:  command.MustParse[*key.Keys]("keys k12"),
			res:  []string{"k12"},
			out:  "1,k12",
		},
		{
			name: "not found",
			cmd:  command.MustParse[*key.Keys]("keys name"),
			res:  []string{},
			out:  "0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := redis.NewFakeConn()
			keys, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			for i, key := range keys.([]core.Key) {
				testx.AssertEqual(t, key.Key, test.res[i])
			}
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
