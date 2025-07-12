package string

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
)

func TestSetParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want Set
		err  error
	}{
		{
			cmd:  "set",
			want: Set{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "set name",
			want: Set{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "set name alice",
			want: Set{key: "name", value: []byte("alice")},
			err:  nil,
		},
		{
			cmd:  "set name alice nx",
			want: Set{key: "name", value: []byte("alice"), ifNX: true},
			err:  nil,
		},
		{
			cmd:  "set name alice xx",
			want: Set{key: "name", value: []byte("alice"), ifXX: true},
			err:  nil,
		},
		{
			cmd:  "set name alice nx xx",
			want: Set{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd:  "set name alice ex 10",
			want: Set{key: "name", value: []byte("alice"), ttl: 10 * time.Second},
			err:  nil,
		},
		{
			cmd:  "set name alice ex 0",
			want: Set{key: "name", value: []byte("alice"), ttl: 0},
			err:  nil,
		},
		{
			cmd:  "set name alice px 10",
			want: Set{key: "name", value: []byte("alice"), ttl: 10 * time.Millisecond},
			err:  nil,
		},
		{
			cmd: "set name alice exat 1577882096",
			want: Set{
				key: "name", value: []byte("alice"),
				at: time.Date(2020, 1, 1, 12, 34, 56, 0, time.UTC),
			},
			err: nil,
		},
		{
			cmd: "set name alice pxat 1577882096000",
			want: Set{
				key: "name", value: []byte("alice"),
				at: time.Date(2020, 1, 1, 12, 34, 56, 0, time.UTC),
			},
			err: nil,
		},
		{
			cmd:  "set name alice keepttl",
			want: Set{key: "name", value: []byte("alice"), keepTTL: true},
			err:  nil,
		},
		{
			cmd:  "set name alice ex 10 keepttl",
			want: Set{},
			err:  redis.ErrSyntaxError,
		},
		{
			cmd: "set name alice nx ex 10",
			want: Set{
				key: "name", value: []byte("alice"),
				ifNX: true, ttl: 10 * time.Second,
			},
			err: nil,
		},
		{
			cmd: "set name alice xx px 10",
			want: Set{
				key: "name", value: []byte("alice"),
				ifXX: true, ttl: 10 * time.Millisecond,
			},
			err: nil,
		},
		{
			cmd: "set name alice ex 10 nx",
			want: Set{
				key: "name", value: []byte("alice"),
				ifNX: true, ttl: 10 * time.Second,
			},
			err: nil,
		},
		{
			cmd: "set name alice nx get ex 10",
			want: Set{
				key: "name", value: []byte("alice"),
				ifNX: true, get: true, ttl: 10 * time.Second,
			},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSet, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.want.key)
				be.Equal(t, cmd.value, test.want.value)
				be.Equal(t, cmd.ifNX, test.want.ifNX)
				be.Equal(t, cmd.ifXX, test.want.ifXX)
				be.Equal(t, cmd.ttl, test.want.ttl)
			} else {
				be.Equal(t, cmd, test.want)
			}
		})
	}
}

func TestSetExec(t *testing.T) {
	red := getRedka(t)

	tests := []struct {
		cmd string
		res any
		out string
	}{
		{
			cmd: "set name alice",
			res: true,
			out: "OK",
		},
		{
			cmd: "set name alice nx",
			res: false,
			out: "(nil)",
		},
		{
			cmd: "set age alice nx",
			res: true,
			out: "OK",
		},
		{
			cmd: "set name bob xx",
			res: true,
			out: "OK",
		},
		{
			cmd: "set city paris xx",
			res: false,
			out: "(nil)",
		},
		{
			cmd: "set name alice ex 10",
			res: true,
			out: "OK",
		},
		{
			cmd: "set name alice keepttl",
			res: true,
			out: "OK",
		},
		{
			cmd: "set color blue nx ex 10",
			res: true,
			out: "OK",
		},
		{
			cmd: "set name bob get",
			res: core.Value("alice"),
			out: "alice",
		},
		{
			cmd: "set country france get",
			res: core.Value(nil),
			out: "(nil)",
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			conn := redis.NewFakeConn()
			cmd := redis.MustParse(ParseSet, test.cmd)
			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)
			be.Equal(t, res, test.res)
			be.Equal(t, conn.Out(), test.out)
		})
	}
}
