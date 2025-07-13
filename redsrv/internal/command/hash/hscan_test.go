package hash

import (
	"fmt"
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestHScanParse(t *testing.T) {
	tests := []struct {
		cmd    string
		key    string
		cursor int
		match  string
		count  int
		err    error
	}{
		{
			cmd:    "hscan",
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hscan person",
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "hscan person 15",
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "hscan person 15 match *",
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "hscan person 15 match * count 5",
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "hscan person 15 count 5 match *",
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "hscan person 15 match k2* count 5",
			key:    "person",
			cursor: 15,
			match:  "k2*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "hscan person ten",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrInvalidInt,
		},
		{
			cmd:    "hscan person 15 *",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
		{
			cmd:    "hscan person 15 * 5",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseHScan, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.cursor, test.cursor)
				be.Equal(t, cmd.match, test.match)
				be.Equal(t, cmd.count, test.count)
			} else {
				be.Equal(t, cmd, HScan{})
			}
		})
	}
}

func TestHScanExec(t *testing.T) {
	red := getRedka(t)

	_, _ = red.Hash().Set("key", "f11", "11")
	_, _ = red.Hash().Set("key", "f12", "12")
	_, _ = red.Hash().Set("key", "f21", "21")
	_, _ = red.Hash().Set("key", "f22", "22")
	_, _ = red.Hash().Set("key", "f31", "31")

	t.Run("hscan all", func(t *testing.T) {
		var cursor int
		{
			// page 1
			cmd := redis.MustParse(ParseHScan, "hscan key 0")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.True(t, sres.Cursor > 0)
			be.Equal(t, len(sres.Items), 5)
			be.Equal(t, sres.Items[0].Field, "f11")
			be.Equal(t, sres.Items[0].Value, core.Value("11"))
			be.Equal(t, sres.Items[4].Field, "f31")
			be.Equal(t, sres.Items[4].Value, core.Value("31"))
			wantOut := fmt.Sprintf("2,%d,10,f11,11,f12,12,f21,21,f22,22,f31,31", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// page 2 (empty)
			next := fmt.Sprintf("hscan key %d", cursor)
			cmd := redis.MustParse(ParseHScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.Equal(t, sres.Cursor, 0)
			be.Equal(t, len(sres.Items), 0)
			be.Equal(t, conn.Out(), "2,0,0")
		}
	})

	t.Run("hscan pattern", func(t *testing.T) {
		cmd := redis.MustParse(ParseHScan, "hscan key 0 match f2*")
		conn := redis.NewFakeConn()

		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)

		sres := res.(rhash.ScanResult)
		be.True(t, sres.Cursor > 0)
		be.Equal(t, len(sres.Items), 2)
		be.Equal(t, sres.Items[0].Field, "f21")
		be.Equal(t, sres.Items[0].Value, core.Value("21"))
		be.Equal(t, sres.Items[1].Field, "f22")
		be.Equal(t, sres.Items[1].Value, core.Value("22"))
		wantOut := fmt.Sprintf("2,%d,4,f21,21,f22,22", sres.Cursor)
		be.Equal(t, conn.Out(), wantOut)
	})

	t.Run("hscan count", func(t *testing.T) {
		var cursor int
		{
			// page 1
			cmd := redis.MustParse(ParseHScan, "hscan key 0 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Items), 2)
			be.Equal(t, sres.Items[0].Field, "f11")
			be.Equal(t, sres.Items[0].Value, core.Value("11"))
			be.Equal(t, sres.Items[1].Field, "f12")
			be.Equal(t, sres.Items[1].Value, core.Value("12"))
			wantOut := fmt.Sprintf("2,%d,4,f11,11,f12,12", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// page 2
			next := fmt.Sprintf("hscan key %d match * count 2", cursor)
			cmd := redis.MustParse(ParseHScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Items), 2)
			be.Equal(t, sres.Items[0].Field, "f21")
			be.Equal(t, sres.Items[0].Value, core.Value("21"))
			be.Equal(t, sres.Items[1].Field, "f22")
			be.Equal(t, sres.Items[1].Value, core.Value("22"))
			wantOut := fmt.Sprintf("2,%d,4,f21,21,f22,22", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// page 3
			next := fmt.Sprintf("hscan key %d match * count 2", cursor)
			cmd := redis.MustParse(ParseHScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Items), 1)
			be.Equal(t, sres.Items[0].Field, "f31")
			be.Equal(t, sres.Items[0].Value, core.Value("31"))
			wantOut := fmt.Sprintf("2,%d,2,f31,31", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// no more pages
			next := fmt.Sprintf("hscan key %d match * count 2", cursor)
			cmd := redis.MustParse(ParseHScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.Equal(t, sres.Cursor, 0)
			be.Equal(t, len(sres.Items), 0)
			be.Equal(t, conn.Out(), "2,0,0")
		}
	})
}
