package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestZCardParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZCard
		err  error
	}{
		{
			name: "zcard",
			args: buildArgs("zcard"),
			want: ZCard{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zcard key",
			args: buildArgs("zcard", "key"),
			want: ZCard{key: "key"},
			err:  nil,
		},
		{
			name: "zcard key one",
			args: buildArgs("zcard", "key", "one"),
			want: ZCard{},
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZCard)
				testx.AssertEqual(t, cm.key, test.want.key)
			}
		})
	}
}

func TestZCardExec(t *testing.T) {
	t.Run("zcard", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := mustParse[*ZCard]("zcard key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Delete("key", "one")

		cmd := mustParse[*ZCard]("zcard key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZCard]("zcard key")
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

		cmd := mustParse[*ZCard]("zcard key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
