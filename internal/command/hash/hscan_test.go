package hash

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/rhash"
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
	db, red := getDB(t)
	defer db.Close()

	_, _ = db.Hash().Set("key", "f11", "11")
	_, _ = db.Hash().Set("key", "f12", "12")
	_, _ = db.Hash().Set("key", "f21", "21")
	_, _ = db.Hash().Set("key", "f22", "22")
	_, _ = db.Hash().Set("key", "f31", "31")

	t.Run("hscan all", func(t *testing.T) {
		{
			cmd := redis.MustParse(ParseHScan, "hscan key 0")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.Equal(t, sres.Cursor, 5)
			be.Equal(t, len(sres.Items), 5)
			be.Equal(t, sres.Items[0].Field, "f11")
			be.Equal(t, sres.Items[0].Value, core.Value("11"))
			be.Equal(t, sres.Items[4].Field, "f31")
			be.Equal(t, sres.Items[4].Value, core.Value("31"))
			be.Equal(t, conn.Out(), "2,5,10,f11,11,f12,12,f21,21,f22,22,f31,31")
		}
		{
			cmd := redis.MustParse(ParseHScan, "hscan key 5")
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
		be.Equal(t, sres.Cursor, 4)
		be.Equal(t, len(sres.Items), 2)
		be.Equal(t, sres.Items[0].Field, "f21")
		be.Equal(t, sres.Items[0].Value, core.Value("21"))
		be.Equal(t, sres.Items[1].Field, "f22")
		be.Equal(t, sres.Items[1].Value, core.Value("22"))
		be.Equal(t, conn.Out(), "2,4,4,f21,21,f22,22")
	})

	t.Run("hscan count", func(t *testing.T) {
		{
			// page 1
			cmd := redis.MustParse(ParseHScan, "hscan key 0 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.Equal(t, sres.Cursor, 2)
			be.Equal(t, len(sres.Items), 2)
			be.Equal(t, sres.Items[0].Field, "f11")
			be.Equal(t, sres.Items[0].Value, core.Value("11"))
			be.Equal(t, sres.Items[1].Field, "f12")
			be.Equal(t, sres.Items[1].Value, core.Value("12"))
			be.Equal(t, conn.Out(), "2,2,4,f11,11,f12,12")
		}
		{
			// page 2
			cmd := redis.MustParse(ParseHScan, "hscan key 2 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.Equal(t, sres.Cursor, 4)
			be.Equal(t, len(sres.Items), 2)
			be.Equal(t, sres.Items[0].Field, "f21")
			be.Equal(t, sres.Items[0].Value, core.Value("21"))
			be.Equal(t, sres.Items[1].Field, "f22")
			be.Equal(t, sres.Items[1].Value, core.Value("22"))
			be.Equal(t, conn.Out(), "2,4,4,f21,21,f22,22")
		}
		{
			// page 3
			cmd := redis.MustParse(ParseHScan, "hscan key 4 match * count 2")
			conn := redis.NewFakeConn()

			res, err := cmd.Run(conn, red)
			be.Err(t, err, nil)

			sres := res.(rhash.ScanResult)
			be.Equal(t, sres.Cursor, 5)
			be.Equal(t, len(sres.Items), 1)
			be.Equal(t, sres.Items[0].Field, "f31")
			be.Equal(t, sres.Items[0].Value, core.Value("31"))
			be.Equal(t, conn.Out(), "2,5,2,f31,31")
		}
		{
			// no more pages
			cmd := redis.MustParse(ParseHScan, "hscan key 5 match * count 2")
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
