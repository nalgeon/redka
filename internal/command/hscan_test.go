package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/internal/testx"
)

func TestHScanParse(t *testing.T) {
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
			name:   "hscan",
			args:   buildArgs("hscan"),
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    ErrInvalidArgNum,
		},
		{
			name:   "hscan person",
			args:   buildArgs("hscan", "person"),
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    ErrInvalidArgNum,
		},
		{
			name:   "hscan person 15",
			args:   buildArgs("hscan", "person", "15"),
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "hscan person 15 match *",
			args:   buildArgs("hscan", "person", "15", "match", "*"),
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			name:   "hscan person 15 match * count 5",
			args:   buildArgs("hscan", "person", "15", "match", "*", "count", "5"),
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "hscan person 15 count 5 match *",
			args:   buildArgs("hscan", "person", "15", "count", "5", "match", "*"),
			key:    "person",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			name:   "hscan person 15 match k2* count 5",
			args:   buildArgs("hscan", "person", "15", "match", "k2*", "count", "5"),
			key:    "person",
			cursor: 15,
			match:  "k2*",
			count:  5,
			err:    nil,
		},
		{
			name:   "hscan person ten",
			args:   buildArgs("hscan", "person", "ten"),
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    ErrInvalidInt,
		},
		{
			name:   "hscan person 15 *",
			args:   buildArgs("hscan", "person", "15", "*"),
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    ErrSyntaxError,
		},
		{
			name:   "hscan person 15 * 5",
			args:   buildArgs("hscan", "person", "15", "*", "5"),
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
				scmd := cmd.(*HScan)
				testx.AssertEqual(t, scmd.key, test.key)
				testx.AssertEqual(t, scmd.cursor, test.cursor)
				testx.AssertEqual(t, scmd.match, test.match)
				testx.AssertEqual(t, scmd.count, test.count)
			}
		})
	}
}

func TestHScanExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	_, _ = db.Hash().Set("key", "f11", "11")
	_, _ = db.Hash().Set("key", "f12", "12")
	_, _ = db.Hash().Set("key", "f21", "21")
	_, _ = db.Hash().Set("key", "f22", "22")
	_, _ = db.Hash().Set("key", "f31", "31")

	t.Run("hscan all", func(t *testing.T) {
		{
			cmd := mustParse[*HScan]("hscan key 0")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rhash.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Items), 5)
			testx.AssertEqual(t, sres.Items[0].Field, "f11")
			testx.AssertEqual(t, sres.Items[0].Value, core.Value("11"))
			testx.AssertEqual(t, sres.Items[4].Field, "f31")
			testx.AssertEqual(t, sres.Items[4].Value, core.Value("31"))
			testx.AssertEqual(t, conn.out(), "2,5,10,f11,11,f12,12,f21,21,f22,22,f31,31")
		}
		{
			cmd := mustParse[*HScan]("hscan key 5")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rhash.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Items), 0)
			testx.AssertEqual(t, conn.out(), "2,0,0")
		}
	})

	t.Run("hscan pattern", func(t *testing.T) {
		cmd := mustParse[*HScan]("hscan key 0 match f2*")
		conn := new(fakeConn)

		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)

		sres := res.(rhash.ScanResult)
		testx.AssertEqual(t, sres.Cursor, 4)
		testx.AssertEqual(t, len(sres.Items), 2)
		testx.AssertEqual(t, sres.Items[0].Field, "f21")
		testx.AssertEqual(t, sres.Items[0].Value, core.Value("21"))
		testx.AssertEqual(t, sres.Items[1].Field, "f22")
		testx.AssertEqual(t, sres.Items[1].Value, core.Value("22"))
		testx.AssertEqual(t, conn.out(), "2,4,4,f21,21,f22,22")
	})

	t.Run("hscan count", func(t *testing.T) {
		{
			// page 1
			cmd := mustParse[*HScan]("hscan key 0 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rhash.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 2)
			testx.AssertEqual(t, len(sres.Items), 2)
			testx.AssertEqual(t, sres.Items[0].Field, "f11")
			testx.AssertEqual(t, sres.Items[0].Value, core.Value("11"))
			testx.AssertEqual(t, sres.Items[1].Field, "f12")
			testx.AssertEqual(t, sres.Items[1].Value, core.Value("12"))
			testx.AssertEqual(t, conn.out(), "2,2,4,f11,11,f12,12")
		}
		{
			// page 2
			cmd := mustParse[*HScan]("hscan key 2 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rhash.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 4)
			testx.AssertEqual(t, len(sres.Items), 2)
			testx.AssertEqual(t, sres.Items[0].Field, "f21")
			testx.AssertEqual(t, sres.Items[0].Value, core.Value("21"))
			testx.AssertEqual(t, sres.Items[1].Field, "f22")
			testx.AssertEqual(t, sres.Items[1].Value, core.Value("22"))
			testx.AssertEqual(t, conn.out(), "2,4,4,f21,21,f22,22")
		}
		{
			// page 3
			cmd := mustParse[*HScan]("hscan key 4 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rhash.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Items), 1)
			testx.AssertEqual(t, sres.Items[0].Field, "f31")
			testx.AssertEqual(t, sres.Items[0].Value, core.Value("31"))
			testx.AssertEqual(t, conn.out(), "2,5,2,f31,31")
		}
		{
			// no more pages
			cmd := mustParse[*HScan]("hscan key 5 match * count 2")
			conn := new(fakeConn)

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rhash.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Items), 0)
			testx.AssertEqual(t, conn.out(), "2,0,0")
		}
	})
}
