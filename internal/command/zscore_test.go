package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/testx"
)

func TestZScoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZScore
		err  error
	}{
		{
			name: "zscore",
			args: buildArgs("zscore"),
			want: ZScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zscore key",
			args: buildArgs("zscore", "key"),
			want: ZScore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zscore key member",
			args: buildArgs("zscore", "key", "member"),
			want: ZScore{key: "key", member: "member"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZScore)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.member, test.want.member)
			}
		})
	}
}

func TestZScoreExec(t *testing.T) {
	t.Run("rank", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)
		_, _ = db.ZSet().Add("key", "two", 22)

		cmd := mustParse[*ZScore]("zscore key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 22.0)
		testx.AssertEqual(t, conn.out(), "22")
	})
	t.Run("member not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 11)

		cmd := mustParse[*ZScore]("zscore key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZScore]("zscore key two")
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

		cmd := mustParse[*ZScore]("zscore key two")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
}
