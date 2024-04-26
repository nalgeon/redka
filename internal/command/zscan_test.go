package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZScanParse(t *testing.T) {
	tests := []struct {
		name   string
		args   [][]byte
		key    string
		cursor int
		match  string
		count  int
		err    error
	}{
		{
			name:   "zscan",
			args:   buildArgs("zscan"),
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    ErrInvalidArgNum,
		},
		{
			name:   "zscan key",
			args:   buildArgs("zscan", "key"),
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    ErrInvalidArgNum,
		},
		{
			name:   "zscan key 15",
			args:   buildArgs("zscan", "key", "15"),
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "zscan key 15 match *",
			args:   buildArgs("zscan", "key", "15", "match", "*"),
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "zscan key 15 match * count 5",
			args:   buildArgs("zscan", "key", "15", "match", "*", "count", "5"),
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "zscan key 15 count 5 match *",
			args:   buildArgs("zscan", "key", "15", "count", "5", "match", "*"),
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "zscan key 15 match m2* count 5",
			args:   buildArgs("zscan", "key", "15", "match", "m2*", "count", "5"),
			key:    "key",
			cursor: 15,
			match:  "m2*",
			count:  5,
			err:    nil,
		},
		{
			name:   "zscan key ten",
			args:   buildArgs("zscan", "key", "ten"),
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    ErrInvalidInt,
		},
		{
			name:   "zscan key 15 *",
			args:   buildArgs("zscan", "key", "15", "*"),
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    ErrSyntaxError,
		},
		{
			name:   "zscan key 15 * 5",
			args:   buildArgs("zscan", "key", "15", "*", "5"),
			key:    "",
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
				scmd := cmd.(*ZScan)
				testx.AssertEqual(t, scmd.key, test.key)
				testx.AssertEqual(t, scmd.cursor, test.cursor)
				testx.AssertEqual(t, scmd.match, test.match)
				testx.AssertEqual(t, scmd.count, test.count)
			}
		})
	}
}

func TestZScanExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_, _ = db.ZSet().Add("key", "m11", 11)
	_, _ = db.ZSet().Add("key", "m12", 12)
	_, _ = db.ZSet().Add("key", "m21", 21)
	_, _ = db.ZSet().Add("key", "m22", 22)
	_, _ = db.ZSet().Add("key", "m31", 31)

	t.Run("zscan all", func(t *testing.T) {
		{
			cmd := mustParse[*ZScan]("zscan key 0")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rzset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Items), 5)
			testx.AssertEqual(t, sres.Items[0].Elem, core.Value("m11"))
			testx.AssertEqual(t, sres.Items[0].Score, 11.0)
			testx.AssertEqual(t, sres.Items[4].Elem, core.Value("m31"))
			testx.AssertEqual(t, sres.Items[4].Score, 31.0)
			testx.AssertEqual(t, conn.out(), "2,5,10,m11,11,m12,12,m21,21,m22,22,m31,31")
		}
		{
			cmd := mustParse[*ZScan]("zscan key 5")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rzset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Items), 0)
			testx.AssertEqual(t, conn.out(), "2,0,0")
		}
	})
	t.Run("zscan pattern", func(t *testing.T) {
		cmd := mustParse[*ZScan]("zscan key 0 match m2*")
		conn := new(fakeConn)

		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)

		sres := res.(rzset.ScanResult)
		testx.AssertEqual(t, sres.Cursor, 4)
		testx.AssertEqual(t, len(sres.Items), 2)
		testx.AssertEqual(t, sres.Items[0].Elem.String(), "m21")
		testx.AssertEqual(t, sres.Items[0].Score, 21.0)
		testx.AssertEqual(t, sres.Items[1].Elem.String(), "m22")
		testx.AssertEqual(t, sres.Items[1].Score, 22.0)
		testx.AssertEqual(t, conn.out(), "2,4,4,m21,21,m22,22")
	})
	t.Run("zscan count", func(t *testing.T) {
		{
			// page 1
			cmd := mustParse[*ZScan]("zscan key 0 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rzset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 2)
			testx.AssertEqual(t, len(sres.Items), 2)
			testx.AssertEqual(t, sres.Items[0].Elem.String(), "m11")
			testx.AssertEqual(t, sres.Items[0].Score, 11.0)
			testx.AssertEqual(t, sres.Items[1].Elem.String(), "m12")
			testx.AssertEqual(t, sres.Items[1].Score, 12.0)
			testx.AssertEqual(t, conn.out(), "2,2,4,m11,11,m12,12")
		}
		{
			// page 2
			cmd := mustParse[*ZScan]("zscan key 2 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rzset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 4)
			testx.AssertEqual(t, len(sres.Items), 2)
			testx.AssertEqual(t, sres.Items[0].Elem.String(), "m21")
			testx.AssertEqual(t, sres.Items[0].Score, 21.0)
			testx.AssertEqual(t, sres.Items[1].Elem.String(), "m22")
			testx.AssertEqual(t, sres.Items[1].Score, 22.0)
			testx.AssertEqual(t, conn.out(), "2,4,4,m21,21,m22,22")
		}
		{
			// page 3
			cmd := mustParse[*ZScan]("zscan key 4 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rzset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Items), 1)
			testx.AssertEqual(t, sres.Items[0].Elem.String(), "m31")
			testx.AssertEqual(t, sres.Items[0].Score, 31.0)
			testx.AssertEqual(t, conn.out(), "2,5,2,m31,31")
		}
		{
			// no more pages
			cmd := mustParse[*ZScan]("zscan key 5 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rzset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Items), 0)
			testx.AssertEqual(t, conn.out(), "2,0,0")
		}
	})
}
