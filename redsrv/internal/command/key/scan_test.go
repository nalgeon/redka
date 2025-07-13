package key

import (
	"fmt"
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/redsrv/internal/redis"
)

func TestScanParse(t *testing.T) {
	tests := []struct {
		cmd    string
		cursor int
		match  string
		ktype  string
		count  int
		err    error
	}{
		{
			cmd:    "scan",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "scan 15",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "scan 15 match *",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "scan 15 match * count 5",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "scan 15 match * count ok",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidInt,
		},
		{
			cmd:    "scan 15 count 5 match *",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "scan 15 match k2* count 5",
			cursor: 15,
			match:  "k2*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "scan 15 match k2* type string",
			cursor: 15,
			match:  "k2*",
			ktype:  "string",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "scan 15 match k2* count 5 type string",
			cursor: 15,
			match:  "k2*",
			ktype:  "string",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "scan ten",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrInvalidInt,
		},
		{
			cmd:    "scan 15 *",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
		{
			cmd:    "scan 15 * 5",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseScan, test.cmd)
			be.Equal(t, err, test.err)
			if err == nil {
				be.Equal(t, cmd.cursor, test.cursor)
				be.Equal(t, cmd.match, test.match)
				be.Equal(t, cmd.ktype, test.ktype)
				be.Equal(t, cmd.count, test.count)
			} else {
				be.Equal(t, cmd, Scan{})
			}
		})
	}
}

func TestScanExec(t *testing.T) {
	t.Run("scan all", func(t *testing.T) {
		red := getRedka(t)

		_ = red.Str().Set("k11", "11")
		_ = red.Str().Set("k12", "12")
		_ = red.Str().Set("k21", "21")
		_ = red.Str().Set("k22", "22")
		_ = red.Str().Set("k31", "31")

		var cursor int
		{
			cmd := redis.MustParse(ParseScan, "scan 0")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rkey.ScanResult)
			be.True(t, sres.Cursor > 0)
			be.Equal(t, len(sres.Keys), 5)
			be.Equal(t, sres.Keys[0].Key, "k11")
			be.Equal(t, sres.Keys[4].Key, "k31")
			wantOut := fmt.Sprintf("2,%d,5,k11,k12,k21,k22,k31", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			next := fmt.Sprintf("scan %d", cursor)
			cmd := redis.MustParse(ParseScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rkey.ScanResult)
			be.Equal(t, sres.Cursor, 0)
			be.Equal(t, len(sres.Keys), 0)
			be.Equal(t, conn.Out(), "2,0,0")
		}
	})
	t.Run("scan pattern", func(t *testing.T) {
		red := getRedka(t)

		_ = red.Str().Set("k11", "11")
		_ = red.Str().Set("k12", "12")
		_ = red.Str().Set("k21", "21")
		_ = red.Str().Set("k22", "22")
		_ = red.Str().Set("k31", "31")

		cmd := redis.MustParse(ParseScan, "scan 0 match k2*")
		conn := redis.NewFakeConn()

		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)

		sres := res.(rkey.ScanResult)
		be.True(t, sres.Cursor > 0)
		be.Equal(t, len(sres.Keys), 2)
		be.Equal(t, sres.Keys[0].Key, "k21")
		be.Equal(t, sres.Keys[1].Key, "k22")
		wantOut := fmt.Sprintf("2,%d,2,k21,k22", sres.Cursor)
		be.Equal(t, conn.Out(), wantOut)
	})
	t.Run("scan type", func(t *testing.T) {
		red := getRedka(t)

		_ = red.Str().Set("t1", "str")
		_, _ = red.List().PushBack("t2", "elem")
		_, _ = red.Hash().Set("t4", "field", "value")
		_, _ = red.ZSet().Add("t5", "elem", 11)

		cmd := redis.MustParse(ParseScan, "scan 0 match t* type hash")
		conn := redis.NewFakeConn()

		res, err := cmd.Run(conn, red)
		be.Err(t, err, nil)

		sres := res.(rkey.ScanResult)
		be.True(t, sres.Cursor > 0)
		be.Equal(t, len(sres.Keys), 1)
		be.Equal(t, sres.Keys[0].Key, "t4")
		wantOut := fmt.Sprintf("2,%d,1,t4", sres.Cursor)
		be.Equal(t, conn.Out(), wantOut)
	})
	t.Run("scan count", func(t *testing.T) {
		red := getRedka(t)

		_ = red.Str().Set("k11", "11")
		_ = red.Str().Set("k12", "12")
		_ = red.Str().Set("k21", "21")
		_ = red.Str().Set("k22", "22")
		_ = red.Str().Set("k31", "31")

		var cursor int
		{
			// page 1
			cmd := redis.MustParse(ParseScan, "scan 0 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rkey.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Keys), 2)
			be.Equal(t, sres.Keys[0].Key, "k11")
			be.Equal(t, sres.Keys[1].Key, "k12")
			wantOut := fmt.Sprintf("2,%d,2,k11,k12", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// page 2
			next := fmt.Sprintf("scan %d match * count 2", cursor)
			cmd := redis.MustParse(ParseScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rkey.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Keys), 2)
			be.Equal(t, sres.Keys[0].Key, "k21")
			be.Equal(t, sres.Keys[1].Key, "k22")
			wantOut := fmt.Sprintf("2,%d,2,k21,k22", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// page 3
			next := fmt.Sprintf("scan %d match * count 2", cursor)
			cmd := redis.MustParse(ParseScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rkey.ScanResult)
			be.True(t, sres.Cursor > cursor)
			be.Equal(t, len(sres.Keys), 1)
			be.Equal(t, sres.Keys[0].Key, "k31")
			wantOut := fmt.Sprintf("2,%d,1,k31", sres.Cursor)
			be.Equal(t, conn.Out(), wantOut)
			cursor = sres.Cursor
		}
		{
			// no more pages
			next := fmt.Sprintf("scan %d match * count 2", cursor)
			cmd := redis.MustParse(ParseScan, next)
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rkey.ScanResult)
			be.Equal(t, sres.Cursor, 0)
			be.Equal(t, len(sres.Keys), 0)
			be.Equal(t, conn.Out(), "2,0,0")
		}
	})
}
