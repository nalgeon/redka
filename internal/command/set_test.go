package command

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/testx"
)

func TestSetParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want Set
		err  error
	}{
		{
			name: "set",
			args: buildArgs("set"),
			want: Set{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "set name",
			args: buildArgs("set", "name"),
			want: Set{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "set name alice",
			args: buildArgs("set", "name", "alice"),
			want: Set{key: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			name: "set name alice nx",
			args: buildArgs("set", "name", "alice", "nx"),
			want: Set{key: "name", value: []byte("alice"), ifNX: true},
			err:  nil,
		},
		{
			name: "set name alice xx",
			args: buildArgs("set", "name", "alice", "xx"),
			want: Set{key: "name", value: []byte("alice"), ifXX: true},
			err:  nil,
		},
		{
			name: "set name alice nx xx",
			args: buildArgs("set", "name", "alice", "nx", "xx"),
			want: Set{},
			err:  ErrSyntaxError,
		},
		{
			name: "set name alice ex 10",
			args: buildArgs("set", "name", "alice", "ex", "10"),
			want: Set{key: "name", value: []byte("alice"), ttl: 10 * time.Second},
			err:  nil,
		},
		{
			name: "set name alice ex 0",
			args: buildArgs("set", "name", "alice", "ex", "0"),
			want: Set{key: "name", value: []byte("alice"), ttl: 0},
			err:  nil,
		},
		{
			name: "set name alice px 10",
			args: buildArgs("set", "name", "alice", "px", "10"),
			want: Set{key: "name", value: []byte("alice"), ttl: 10 * time.Millisecond},
			err:  nil,
		},
		{
			name: "set name alice exat 1577882096",
			args: buildArgs("set", "name", "alice", "exat", "1577882096"),
			want: Set{key: "name", value: []byte("alice"),
				at: time.Date(2020, 1, 1, 12, 34, 56, 0, time.UTC)},
			err: nil,
		},
		{
			name: "set name alice pxat 1577882096000",
			args: buildArgs("set", "name", "alice", "exat", "1577882096000"),
			want: Set{key: "name", value: []byte("alice"),
				at: time.Date(2020, 1, 1, 12, 34, 56, 0, time.UTC)},
			err: nil,
		},
		{
			name: "set name alice nx ex 10",
			args: buildArgs("set", "name", "alice", "nx", "ex", "10"),
			want: Set{key: "name", value: []byte("alice"), ifNX: true, ttl: 10 * time.Second},
			err:  nil,
		},
		{
			name: "set name alice xx px 10",
			args: buildArgs("set", "name", "alice", "xx", "px", "10"),
			want: Set{key: "name", value: []byte("alice"), ifXX: true, ttl: 10 * time.Millisecond},
			err:  nil,
		},
		{
			name: "set name alice ex 10 nx",
			args: buildArgs("set", "name", "alice", "ex", "10", "nx"),
			want: Set{key: "name", value: []byte("alice"), ifNX: true, ttl: 10 * time.Second},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				setCmd := cmd.(*Set)
				testx.AssertEqual(t, setCmd.key, test.want.key)
				testx.AssertEqual(t, setCmd.value, test.want.value)
				testx.AssertEqual(t, setCmd.ifNX, test.want.ifNX)
				testx.AssertEqual(t, setCmd.ifXX, test.want.ifXX)
				testx.AssertEqual(t, setCmd.ttl, test.want.ttl)
			}
		})
	}
}

func TestSetExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	tests := []struct {
		name string
		cmd  *Set
		res  any
		out  string
	}{
		{
			name: "set",
			cmd:  mustParse[*Set]("set name alice"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set nx conflict",
			cmd:  mustParse[*Set]("set name alice nx"),
			res:  false,
			out:  "(nil)",
		},
		{
			name: "set nx",
			cmd:  mustParse[*Set]("set age alice nx"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set xx",
			cmd:  mustParse[*Set]("set name bob xx"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set xx conflict",
			cmd:  mustParse[*Set]("set city paris xx"),
			res:  false,
			out:  "(nil)",
		},
		{
			name: "set ex",
			cmd:  mustParse[*Set]("set name alice ex 10"),
			res:  true,
			out:  "OK",
		},
		{
			name: "set nx ex",
			cmd:  mustParse[*Set]("set color blue nx ex 10"),
			res:  true,
			out:  "OK",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := new(fakeConn)
			res, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.out(), test.out)
		})
	}
}
