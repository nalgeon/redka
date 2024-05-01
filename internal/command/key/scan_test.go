package key

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/testx"
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
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.cursor, test.cursor)
				testx.AssertEqual(t, cmd.match, test.match)
				testx.AssertEqual(t, cmd.ktype, test.ktype)
				testx.AssertEqual(t, cmd.count, test.count)
			}
		})
	}
}

func TestScanExec(t *testing.T) {
	t.Run("scan all", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("k11", "11")
		_ = db.Str().Set("k12", "12")
		_ = db.Str().Set("k21", "21")
		_ = db.Str().Set("k22", "22")
		_ = db.Str().Set("k31", "31")

		{
			cmd := redis.MustParse(ParseScan, "scan 0")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Keys), 5)
			testx.AssertEqual(t, sres.Keys[0].Key, "k11")
			testx.AssertEqual(t, sres.Keys[4].Key, "k31")
			testx.AssertEqual(t, conn.Out(), "2,5,5,k11,k12,k21,k22,k31")
		}
		{
			cmd := redis.MustParse(ParseScan, "scan 5")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Keys), 0)
			testx.AssertEqual(t, conn.Out(), "2,0,0")
		}
	})
	t.Run("scan pattern", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("k11", "11")
		_ = db.Str().Set("k12", "12")
		_ = db.Str().Set("k21", "21")
		_ = db.Str().Set("k22", "22")
		_ = db.Str().Set("k31", "31")

		cmd := redis.MustParse(ParseScan, "scan 0 match k2*")
		conn := redis.NewFakeConn()

		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)

		sres := res.(rkey.ScanResult)
		testx.AssertEqual(t, sres.Cursor, 4)
		testx.AssertEqual(t, len(sres.Keys), 2)
		testx.AssertEqual(t, sres.Keys[0].Key, "k21")
		testx.AssertEqual(t, sres.Keys[1].Key, "k22")
		testx.AssertEqual(t, conn.Out(), "2,4,2,k21,k22")
	})
	t.Run("scan type", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("t1", "str")
		_, _ = db.List().PushBack("t2", "elem")
		_, _ = db.Hash().Set("t4", "field", "value")
		_, _ = db.ZSet().Add("t5", "elem", 11)

		cmd := redis.MustParse(ParseScan, "scan 0 match t* type hash")
		conn := redis.NewFakeConn()

		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)

		sres := res.(rkey.ScanResult)
		testx.AssertEqual(t, sres.Cursor, 3)
		testx.AssertEqual(t, len(sres.Keys), 1)
		testx.AssertEqual(t, sres.Keys[0].Key, "t4")
		testx.AssertEqual(t, conn.Out(), "2,3,1,t4")
	})
	t.Run("scan count", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("k11", "11")
		_ = db.Str().Set("k12", "12")
		_ = db.Str().Set("k21", "21")
		_ = db.Str().Set("k22", "22")
		_ = db.Str().Set("k31", "31")

		{
			// page 1
			cmd := redis.MustParse(ParseScan, "scan 0 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 2)
			testx.AssertEqual(t, len(sres.Keys), 2)
			testx.AssertEqual(t, sres.Keys[0].Key, "k11")
			testx.AssertEqual(t, sres.Keys[1].Key, "k12")
			testx.AssertEqual(t, conn.Out(), "2,2,2,k11,k12")
		}
		{
			// page 2
			cmd := redis.MustParse(ParseScan, "scan 2 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 4)
			testx.AssertEqual(t, len(sres.Keys), 2)
			testx.AssertEqual(t, sres.Keys[0].Key, "k21")
			testx.AssertEqual(t, sres.Keys[1].Key, "k22")
			testx.AssertEqual(t, conn.Out(), "2,4,2,k21,k22")
		}
		{
			// page 3
			cmd := redis.MustParse(ParseScan, "scan 4 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Keys), 1)
			testx.AssertEqual(t, sres.Keys[0].Key, "k31")
			testx.AssertEqual(t, conn.Out(), "2,5,1,k31")
		}
		{
			// no more pages
			cmd := redis.MustParse(ParseScan, "scan 5 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Keys), 0)
			testx.AssertEqual(t, conn.Out(), "2,0,0")
		}
	})
}
