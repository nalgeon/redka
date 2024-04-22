package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestZCountParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZCount
		err  error
	}{
		{
			name: "zcount",
			args: buildArgs("zcount"),
			want: ZCount{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zcount key",
			args: buildArgs("zcount", "key"),
			want: ZCount{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zcount key 11",
			args: buildArgs("zcount", "key", "11"),
			want: ZCount{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zcount key 11 22",
			args: buildArgs("zcount", "key", "1.1", "2.2"),
			want: ZCount{key: "key", min: 1.1, max: 2.2},
			err:  nil,
		},
		{
			name: "zcount key 11 22 33",
			args: buildArgs("zcount", "key", "1.1", "2.2", "3.3"),
			want: ZCount{},
			err:  ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZCount)
				testx.AssertEqual(t, cm.key, test.want.key)
			}
		})
	}
}

func TestZCountExec(t *testing.T) {
	t.Run("zcount", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)
		_, _ = db.ZSet().Add("key", "thr", 33)

		cmd := mustParse[*ZCount]("zcount key 15 25")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "1")
	})
	t.Run("inclusive", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)
		_, _ = db.ZSet().Add("key", "thr", 33)

		cmd := mustParse[*ZCount]("zcount key 11 33")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.out(), "3")
	})
	t.Run("zero", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)
		_, _ = db.ZSet().Add("key", "thr", 33)

		cmd := mustParse[*ZCount]("zcount key 44 55")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZCount]("zcount key 11 33")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := mustParse[*ZCount]("zcount key 11 33")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
