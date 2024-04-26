package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestZRevRankParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRevRank
		err  error
	}{
		{
			name: "zrevrank",
			args: buildArgs("zrevrank"),
			want: ZRevRank{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrank key",
			args: buildArgs("zrevrank", "key"),
			want: ZRevRank{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrevrank key member",
			args: buildArgs("zrevrank", "key", "member"),
			want: ZRevRank{key: "key", member: "member"},
			err:  nil,
		},
		{
			name: "zrevrank key member withscore",
			args: buildArgs("zrevrank", "key", "member", "withscore"),
			want: ZRevRank{key: "key", member: "member", withScore: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRevRank)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.member, test.want.member)
				testx.AssertEqual(t, cm.withScore, test.want.withScore)
			}
		})
	}
}

func TestZRevRankExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := mustParse[*ZRevRank]("zrevrank key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("with score", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := mustParse[*ZRevRank]("zrevrank key two withscore")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "2,0,22")
	})
	t.Run("member not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := mustParse[*ZRevRank]("zrevrank key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRevRank]("zrevrank key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := mustParse[*ZRevRank]("zrevrank key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
}
