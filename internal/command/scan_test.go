package command

import (
	"testing"

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
			args:   buildArgs("scan"),
			cursor: 0,
			match:  "*",
			count:  0,
			err:    ErrInvalidArgNum,
		},
		{
			name:   "scan 15",
			args:   buildArgs("scan", "15"),
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "scan 15 match *",
			args:   buildArgs("scan", "15", "match", "*"),
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "scan 15 match * count 5",
			args:   buildArgs("scan", "15", "match", "*", "count", "5"),
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "scan 15 count 5 match *",
			args:   buildArgs("scan", "15", "count", "5", "match", "*"),
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "scan 15 match k2* count 5",
			args:   buildArgs("scan", "15", "match", "k2*", "count", "5"),
			cursor: 15,
			match:  "k2*",
			count:  5,
			err:    nil,
		},
		{
			name:   "scan ten",
			args:   buildArgs("scan", "ten"),
			cursor: 0,
			match:  "",
			count:  0,
			err:    ErrInvalidCursor,
		},
		{
			name:   "scan 15 *",
			args:   buildArgs("scan", "15", "*"),
			cursor: 0,
			match:  "",
			count:  0,
			err:    ErrSyntaxError,
		},
		{
			name:   "scan 15 * 5",
			args:   buildArgs("scan", "15", "*", "5"),
			cursor: 0,
			match:  "",
			count:  0,
			err:    ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				scmd := cmd.(*Scan)
				testx.AssertEqual(t, scmd.cursor, test.cursor)
				testx.AssertEqual(t, scmd.match, test.match)
				testx.AssertEqual(t, scmd.count, test.count)
			}
		})
	}
}

func TestScanExec(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("k11", "11")
	_ = db.Str().Set("k12", "12")
	_ = db.Str().Set("k21", "21")
	_ = db.Str().Set("k22", "22")
	_ = db.Str().Set("k31", "31")

	t.Run("scan all", func(t *testing.T) {
		{
			cmd := mustParse[*Scan]("scan 0")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, db)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Keys), 5)
			testx.AssertEqual(t, sres.Keys[0].Key, "k11")
			testx.AssertEqual(t, sres.Keys[4].Key, "k31")
			testx.AssertEqual(t, conn.out(), "2,5,5,k11,k12,k21,k22,k31")
		}
		{
			cmd := mustParse[*Scan]("scan 5")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, db)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Keys), 0)
			testx.AssertEqual(t, conn.out(), "2,0,0")
		}
	})

	t.Run("scan pattern", func(t *testing.T) {
		cmd := mustParse[*Scan]("scan 0 match k2*")
		conn := new(fakeConn)

		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)

		sres := res.(rkey.ScanResult)
		testx.AssertEqual(t, sres.Cursor, 4)
		testx.AssertEqual(t, len(sres.Keys), 2)
		testx.AssertEqual(t, sres.Keys[0].Key, "k21")
		testx.AssertEqual(t, sres.Keys[1].Key, "k22")
		testx.AssertEqual(t, conn.out(), "2,4,2,k21,k22")
	})

	t.Run("scan count", func(t *testing.T) {
		{
			// page 1
			cmd := mustParse[*Scan]("scan 0 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, db)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 2)
			testx.AssertEqual(t, len(sres.Keys), 2)
			testx.AssertEqual(t, sres.Keys[0].Key, "k11")
			testx.AssertEqual(t, sres.Keys[1].Key, "k12")
			testx.AssertEqual(t, conn.out(), "2,2,2,k11,k12")
		}
		{
			// page 2
			cmd := mustParse[*Scan]("scan 2 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, db)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 4)
			testx.AssertEqual(t, len(sres.Keys), 2)
			testx.AssertEqual(t, sres.Keys[0].Key, "k21")
			testx.AssertEqual(t, sres.Keys[1].Key, "k22")
			testx.AssertEqual(t, conn.out(), "2,4,2,k21,k22")
		}
		{
			// page 3
			cmd := mustParse[*Scan]("scan 4 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, db)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Keys), 1)
			testx.AssertEqual(t, sres.Keys[0].Key, "k31")
			testx.AssertEqual(t, conn.out(), "2,5,1,k31")
		}
		{
			// no more pages
			cmd := mustParse[*Scan]("scan 5 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, db)
			testx.AssertNoErr(t, err)

			sres := res.(rkey.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Keys), 0)
			testx.AssertEqual(t, conn.out(), "2,0,0")
		}
	})
}
