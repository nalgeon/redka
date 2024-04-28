package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestZIncrByParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZIncrBy
		err  error
	}{
		{
			name: "zincrby",
			args: buildArgs("zincrby"),
			want: ZIncrBy{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zincrby key",
			args: buildArgs("zincrby", "key"),
			want: ZIncrBy{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zincrby key one",
			args: buildArgs("zincrby", "key", "one"),
			want: ZIncrBy{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zincrby key 11 one",
			args: buildArgs("zincrby", "key", "11", "one"),
			want: ZIncrBy{key: "key", member: "one", delta: 11.0},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZIncrBy)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.member, test.want.member)
				testx.AssertEqual(t, cm.delta, test.want.delta)
			}
		})
	}
}

func TestZIncrByExec(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZIncrBy]("zincrby key 25.5 one")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 25.5)
		testx.AssertEqual(t, conn.out(), "25.5")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 25.5)
	})
	t.Run("create field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 10)

		cmd := mustParse[*ZIncrBy]("zincrby key 25.5 two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 25.5)
		testx.AssertEqual(t, conn.out(), "25.5")

		score, _ := db.ZSet().GetScore("key", "two")
		testx.AssertEqual(t, score, 25.5)
	})
	t.Run("update field", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 25.5)

		cmd := mustParse[*ZIncrBy]("zincrby key 10.5 one")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 36.0)
		testx.AssertEqual(t, conn.out(), "36")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 36.0)
	})
	t.Run("decrement", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 25.5)

		cmd := mustParse[*ZIncrBy]("zincrby key -10.5 one")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 15.0)
		testx.AssertEqual(t, conn.out(), "15")

		score, _ := db.ZSet().GetScore("key", "one")
		testx.AssertEqual(t, score, 15.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = red.Str().Set("key", "one")

		cmd := mustParse[*ZIncrBy]("zincrby key 25.5 one")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 25.5)
		testx.AssertEqual(t, conn.out(), "25.5")
	})
}
