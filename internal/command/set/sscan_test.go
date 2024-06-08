package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSScanParse(t *testing.T) {
	tests := []struct {
		cmd    string
		key    string
		cursor int
		match  string
		count  int
		err    error
	}{
		{
			cmd:    "sscan",
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "sscan key",
			key:    "",
			cursor: 0,
			match:  "*",
			count:  0,
			err:    redis.ErrInvalidArgNum,
		},
		{
			cmd:    "sscan key 15",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "sscan key 15 match *",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  0,
			err:    nil,
		},
		{
			cmd:    "sscan key 15 match * count 5",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "sscan key 15 count 5 match *",
			key:    "key",
			cursor: 15,
			match:  "*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "sscan key 15 match m2* count 5",
			key:    "key",
			cursor: 15,
			match:  "m2*",
			count:  5,
			err:    nil,
		},
		{
			cmd:    "sscan key ten",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrInvalidInt,
		},
		{
			cmd:    "sscan key 15 *",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
		{
			cmd:    "sscan key 15 * 5",
			key:    "",
			cursor: 0,
			match:  "",
			count:  0,
			err:    redis.ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSScan, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.key, test.key)
				testx.AssertEqual(t, cmd.cursor, test.cursor)
				testx.AssertEqual(t, cmd.match, test.match)
				testx.AssertEqual(t, cmd.count, test.count)
			} else {
				testx.AssertEqual(t, cmd, SScan{})
			}
		})
	}
}

func TestSScanExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()
	_, _ = db.Set().Add("key", "m11", "m12", "m21", "m22", "m31")

	t.Run("sscan all", func(t *testing.T) {
		{
			cmd := redis.MustParse(ParseSScan, "sscan key 0")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Items), 5)
			testx.AssertEqual(t, sres.Items[0], core.Value("m11"))
			testx.AssertEqual(t, sres.Items[4], core.Value("m31"))
			testx.AssertEqual(t, conn.Out(), "2,5,5,m11,m12,m21,m22,m31")
		}
		{
			cmd := redis.MustParse(ParseSScan, "sscan key 5")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Items), 0)
			testx.AssertEqual(t, conn.Out(), "2,0,0")
		}
	})
	t.Run("sscan pattern", func(t *testing.T) {
		cmd := redis.MustParse(ParseSScan, "sscan key 0 match m2*")
		conn := redis.NewFakeConn()

		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)

		sres := res.(rset.ScanResult)
		testx.AssertEqual(t, sres.Cursor, 4)
		testx.AssertEqual(t, len(sres.Items), 2)
		testx.AssertEqual(t, sres.Items[0].String(), "m21")
		testx.AssertEqual(t, sres.Items[1].String(), "m22")
		testx.AssertEqual(t, conn.Out(), "2,4,2,m21,m22")
	})
	t.Run("sscan count", func(t *testing.T) {
		{
			// page 1
			cmd := redis.MustParse(ParseSScan, "sscan key 0 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 2)
			testx.AssertEqual(t, len(sres.Items), 2)
			testx.AssertEqual(t, sres.Items[0].String(), "m11")
			testx.AssertEqual(t, sres.Items[1].String(), "m12")
			testx.AssertEqual(t, conn.Out(), "2,2,2,m11,m12")
		}
		{
			// page 2
			cmd := redis.MustParse(ParseSScan, "sscan key 2 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 4)
			testx.AssertEqual(t, len(sres.Items), 2)
			testx.AssertEqual(t, sres.Items[0].String(), "m21")
			testx.AssertEqual(t, sres.Items[1].String(), "m22")
			testx.AssertEqual(t, conn.Out(), "2,4,2,m21,m22")
		}
		{
			// page 3
			cmd := redis.MustParse(ParseSScan, "sscan key 4 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 5)
			testx.AssertEqual(t, len(sres.Items), 1)
			testx.AssertEqual(t, sres.Items[0].String(), "m31")
			testx.AssertEqual(t, conn.Out(), "2,5,1,m31")
		}
		{
			// no more pages
			cmd := redis.MustParse(ParseSScan, "sscan key 5 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			testx.AssertNoErr(t, err)

			sres := res.(rset.ScanResult)
			testx.AssertEqual(t, sres.Cursor, 0)
			testx.AssertEqual(t, len(sres.Items), 0)
			testx.AssertEqual(t, conn.Out(), "2,0,0")
		}
	})
}
