package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestZRankParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZRank
		err  error
	}{
		{
			name: "zrank",
			args: buildArgs("zrank"),
			want: ZRank{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrank key",
			args: buildArgs("zrank", "key"),
			want: ZRank{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zrank key member",
			args: buildArgs("zrank", "key", "member"),
			want: ZRank{key: "key", member: "member"},
			err:  nil,
		},
		{
			name: "zrank key member withscore",
			args: buildArgs("zrank", "key", "member", "withscore"),
			want: ZRank{key: "key", member: "member", withScore: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZRank)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.member, test.want.member)
				testx.AssertEqual(t, cm.withScore, test.want.withScore)
			}
		})
	}
}

func TestZRankExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := mustParse[*ZRank]("zrank key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "1")
	})
	t.Run("with score", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := mustParse[*ZRank]("zrank key two withscore")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "2,1,22")
	})
	t.Run("member not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := mustParse[*ZRank]("zrank key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZRank]("zrank key two")
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

		cmd := mustParse[*ZRank]("zrank key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
}
