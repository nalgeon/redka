package zset

import (
	"fmt"
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rzset"
)

func TestZScanParse(t *testing.T) {
	tests := []struct {
		cmd    string
		key    string
		cursor int
		match  string
		count  int
		err    error
	}{
		{
			cmd:    "zscan",
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "zscan key",
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "zscan key 15",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "zscan key 15 match *",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "zscan key 15 match * count 5",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "zscan key 15 count 5 match *",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "zscan key 15 match m2* count 5",
			key:    "key",
			cursor: 15,
			match:  "m2*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "zscan key ten",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrInvalidInt,
		},
		{
			cmd:    "zscan key 15 *",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
		{
			cmd:    "zscan key 15 * 5",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseZScan, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.key, test.key)
				be.Equal(t, cmd.cursor, test.cursor)
				be.Equal(t, cmd.match, test.match)
				be.Equal(t, cmd.count, test.count)
			} else {
				be.Equal(t, cmd, ZScan{})
			}
		})
	}
}

func TestZScanExec(t *testing.T) {
	red := getRedka(t)

	_, _ = red.ZSet().Add("key", "m11", 11)
	_, _ = red.ZSet().Add("key", "m12", 12)
	_, _ = red.ZSet().Add("key", "m21", 21)
	_, _ = red.ZSet().Add("key", "m22", 22)
	_, _ = red.ZSet().Add("key", "m31", 31)

	t.Run("zscan all", func(t *testing.T) {
		var cursor int
		{
			cmd := redis.MustParse(ParseZScan, "zscan key 0")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rzset.ScanResult)
			be.True(t, sres.Cursor > 0)
			be.Equal(t, len(sres.Items), 5)
			be.Equal(t, sres.Items[0].Elem, core.Value("m11"))
			be.Equal(t, sres.Items[0].Score, 11.0)
			be.Equal(t, sres.Items[4].Elem, core.Value("m31"))
			be.Equal(t, sres.Items[4].Score, 31.0)
			wantOut := fmt.Sprintf("2,%d,10,m11,11,m12,12,m21,21,m22,22,m31,31", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			next := fmt.Sprintf("zscan key %d", cursor)
			cmd := redis.MustParse(ParseZScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rzset.ScanResult)
			be.Equal(t, sres.Cursor, 0)
			be.Equal(t, len(sres.Items), 0)
			be.Equal(t, conn.Out(), "2,0,0")
		}
	})
	t.Run("zscan pattern", func(t *testing.T) {
		cmd := redis.MustParse(ParseZScan, "zscan key 0 match m2*")
		conn := redis.NewFakeConn()

		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)

		sres := res.(rzset.ScanResult)
		be.True(t, sres.Cursor > 0)
		be.Equal(t, len(sres.Items), 2)
		be.Equal(t, sres.Items[0].Elem.String(), "m21")
		be.Equal(t, sres.Items[0].Score, 21.0)
		be.Equal(t, sres.Items[1].Elem.String(), "m22")
		be.Equal(t, sres.Items[1].Score, 22.0)
		wantOut := fmt.Sprintf("2,%d,4,m21,21,m22,22", sres.Cursor)
		be.Equal(t, conn.Out(), wantOut)
	})
	t.Run("zscan count", func(t *testing.T) {
		var cursor int
		{
			// page 1
			cmd := redis.MustParse(ParseZScan, "zscan key 0 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rzset.ScanResult)
			be.True(t, sres.Cursor > 0)
			be.Equal(t, len(sres.Items), 2)
			be.Equal(t, sres.Items[0].Elem.String(), "m11")
			be.Equal(t, sres.Items[0].Score, 11.0)
			be.Equal(t, sres.Items[1].Elem.String(), "m12")
			be.Equal(t, sres.Items[1].Score, 12.0)
			wantOut := fmt.Sprintf("2,%d,4,m11,11,m12,12", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// page 2
			next := fmt.Sprintf("zscan key %d match * count 2", cursor)
			cmd := redis.MustParse(ParseZScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rzset.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Items), 2)
			be.Equal(t, sres.Items[0].Elem.String(), "m21")
			be.Equal(t, sres.Items[0].Score, 21.0)
			be.Equal(t, sres.Items[1].Elem.String(), "m22")
			be.Equal(t, sres.Items[1].Score, 22.0)
			wantOut := fmt.Sprintf("2,%d,4,m21,21,m22,22", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// page 3
			next := fmt.Sprintf("zscan key %d match * count 2", cursor)
			cmd := redis.MustParse(ParseZScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rzset.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Items), 1)
			be.Equal(t, sres.Items[0].Elem.String(), "m31")
			be.Equal(t, sres.Items[0].Score, 31.0)
			wantOut := fmt.Sprintf("2,%d,2,m31,31", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// no more pages
			next := fmt.Sprintf("zscan key %d match * count 2", cursor)
			cmd := redis.MustParse(ParseZScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rzset.ScanResult)
			be.Equal(t, sres.Cursor, 0)
			be.Equal(t, len(sres.Items), 0)
			be.Equal(t, conn.Out(), "2,0,0")
		}
	})
}
