package string_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/command"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want str.Set
		err  error
	}{
		{
			name: "set",
			args: command.BuildArgs("set"),
			want: str.Set{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "set name",
			args: command.BuildArgs("set", "name"),
			want: str.Set{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "set name alice",
			args: command.BuildArgs("set", "name", "alice"),
			want: str.Set{Key: "name", Value: []byte("alice")},
			err:  nil,
		},
		{
			name: "set name alice nx",
			args: command.BuildArgs("set", "name", "alice", "nx"),
			want: str.Set{Key: "name", Value: []byte("alice"), IfNX: true},
			err:  nil,
		},
		{
			name: "set name alice xx",
			args: command.BuildArgs("set", "name", "alice", "xx"),
			want: str.Set{Key: "name", Value: []byte("alice"), IfXX: true},
			err:  nil,
		},
		{
			name: "set name alice nx xx",
			args: command.BuildArgs("set", "name", "alice", "nx", "xx"),
			want: str.Set{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "set name alice ex 10",
			args: command.BuildArgs("set", "name", "alice", "ex", "10"),
			want: str.Set{Key: "name", Value: []byte("alice"), TTL: 10 * time.Second},
			err:  nil,
		},
		{
			name: "set name alice ex 0",
			args: command.BuildArgs("set", "name", "alice", "ex", "0"),
			want: str.Set{Key: "name", Value: []byte("alice"), TTL: 0},
			err:  nil,
		},
		{
			name: "set name alice px 10",
			args: command.BuildArgs("set", "name", "alice", "px", "10"),
			want: str.Set{Key: "name", Value: []byte("alice"), TTL: 10 * time.Millisecond},
			err:  nil,
		},
		{
			name: "set name alice exat 1577882096",
			args: command.BuildArgs("set", "name", "alice", "exat", "1577882096"),
			want: str.Set{Key: "name", Value: []byte("alice"),
				At: time.Date(2020, 1, 1, 12, 34, 56, 0, time.UTC)},
			err: nil,
		},
		{
			name: "set name alice pxat 1577882096000",
			args: command.BuildArgs("set", "name", "alice", "exat", "1577882096000"),
			want: str.Set{Key: "name", Value: []byte("alice"),
				At: time.Date(2020, 1, 1, 12, 34, 56, 0, time.UTC)},
			err: nil,
		},
		{
			name: "set name alice keepttl",
			args: command.BuildArgs("set", "name", "alice", "keepttl"),
			want: str.Set{Key: "name", Value: []byte("alice"), KeepTTL: true},
			err:  nil,
		},
		{
			name: "set name alice ex 10 keepttl",
			args: command.BuildArgs("set", "name", "alice", "ex", "10", "keepttl"),
			want: str.Set{},
			err:  redis.ErrSyntaxError,
		},
		{
			name: "set name alice nx ex 10",
			args: command.BuildArgs("set", "name", "alice", "nx", "ex", "10"),
			want: str.Set{Key: "name", Value: []byte("alice"), IfNX: true, TTL: 10 * time.Second},
			err:  nil,
		},
		{
			name: "set name alice xx px 10",
			args: command.BuildArgs("set", "name", "alice", "xx", "px", "10"),
			want: str.Set{Key: "name", Value: []byte("alice"), IfXX: true, TTL: 10 * time.Millisecond},
			err:  nil,
		},
		{
			name: "set name alice ex 10 nx",
			args: command.BuildArgs("set", "name", "alice", "ex", "10", "nx"),
			want: str.Set{Key: "name", Value: []byte("alice"), IfNX: true, TTL: 10 * time.Second},
			err:  nil,
		},
		{
			name: "set name alice nx get ex 10",
			args: command.BuildArgs("set", "name", "alice", "nx", "ex", "10"),
			want: str.Set{Key: "name", Value: []byte("alice"), IfNX: true, Get: true, TTL: 10 * time.Second},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				setCmd := cmd.(*str.Set)
				testx.AssertEqual(t, setCmd.Key, test.want.Key)
				testx.AssertEqual(t, setCmd.Value, test.want.Value)
				testx.AssertEqual(t, setCmd.IfNX, test.want.IfNX)
				testx.AssertEqual(t, setCmd.IfXX, test.want.IfXX)
				testx.AssertEqual(t, setCmd.TTL, test.want.TTL)
			}
		})
	}
}

func TestSetExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *str.Set
		res  any
		out  string
	}{
		{
			name: "set",
			cmd:  command.MustParse[*str.Set]("set name alice"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set nx conflict",
			cmd:  command.MustParse[*str.Set]("set name alice nx"),
			res:  false,
			out:  "(nil)",
		},
		{
			name: "set nx",
			cmd:  command.MustParse[*str.Set]("set age alice nx"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set xx",
			cmd:  command.MustParse[*str.Set]("set name bob xx"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set xx conflict",
			cmd:  command.MustParse[*str.Set]("set city paris xx"),
			res:  false,
			out:  "(nil)",
		},
		{
			name: "set ex",
			cmd:  command.MustParse[*str.Set]("set name alice ex 10"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set keepttl",
			cmd:  command.MustParse[*str.Set]("set name alice keepttl"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set nx ex",
			cmd:  command.MustParse[*str.Set]("set color blue nx ex 10"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set get",
			cmd:  command.MustParse[*str.Set]("set name bob get"),
			res:  core.Value("alice"),
			out:  "alice",
		},
		{
			name: "set get nil",
			cmd:  command.MustParse[*str.Set]("set country france get"),
			res:  core.Value(nil),
			out:  "(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := redis.NewFakeConn()
			res, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)
		})
	}
}
