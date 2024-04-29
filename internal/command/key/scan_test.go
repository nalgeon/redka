package key_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/testx"
)

func TestScanParse(t *testing.T) {
	tests := []struct {
		name   string
		args   [][]byte
		cursor int
		match  string
		count  int
		err    error
	}{
		{
			name:   "scan",
			args:   command.BuildArgs("scan"),
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			name:   "scan 15",
			args:   command.BuildArgs("scan", "15"),
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "scan 15 match *",
			args:   command.BuildArgs("scan", "15", "match", "*"),
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "scan 15 match * count 5",
			args:   command.BuildArgs("scan", "15", "match", "*", "count", "5"),
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "scan 15 match * count ok",
			args:   command.BuildArgs("scan", "15", "match", "*", "count", "ok"),
			cursor: 15,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidInt,
		},
		{
			name:   "scan 15 count 5 match *",
			args:   command.BuildArgs("scan", "15", "count", "5", "match", "*"),
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "scan 15 match k2* count 5",
			args:   command.BuildArgs("scan", "15", "match", "k2*", "count", "5"),
			cursor: 15,
			match:  "k2*",
			count:  5,
			err:    nil,
		},
		{
			name:   "scan ten",
			args:   command.BuildArgs("scan", "ten"),
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrInvalidInt,
		},
		{
			name:   "scan 15 *",
			args:   command.BuildArgs("scan", "15", "*"),
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
		{
			name:   "scan 15 * 5",
			args:   command.BuildArgs("scan", "15", "*", "5"),
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				scmd := cmd.(*key.Scan)
				testx.AssertEqual(t, scmd.Cursor, test.cursor)
				testx.AssertEqual(t, scmd.Match, test.match)
				testx.AssertEqual(t, scmd.Count, test.count)
			}
		})
	}
}

func TestScanExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_ = db.Str().Set("k11", "11")
	_ = db.Str().Set("k12", "12")
	_ = db.Str().Set("k21", "21")
	_ = db.Str().Set("k22", "22")
	_ = db.Str().Set("k31", "31")

	t.Run("scan all", func(t *testing.T) {
		{
			cmd := command.MustParse[*key.Scan]("scan 0")
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
			cmd := command.MustParse[*key.Scan]("scan 5")
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
		cmd := command.MustParse[*key.Scan]("scan 0 match k2*")
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

	t.Run("scan count", func(t *testing.T) {
		{
			// page 1
			cmd := command.MustParse[*key.Scan]("scan 0 match * count 2")
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
			cmd := command.MustParse[*key.Scan]("scan 2 match * count 2")
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
			cmd := command.MustParse[*key.Scan]("scan 4 match * count 2")
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
			cmd := command.MustParse[*key.Scan]("scan 5 match * count 2")
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
