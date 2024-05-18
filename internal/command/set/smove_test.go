package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSMoveParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SMove
		err  error
	}{
		{
			cmd:  "smove",
			want: SMove{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "smove src",
			want: SMove{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "smove src dest",
			want: SMove{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "smove src dest one",
			want: SMove{src: "src", dest: "dest", member: []byte("one")},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSMove, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.src, test.want.src)
				testx.AssertEqual(t, cmd.dest, test.want.dest)
				testx.AssertEqual(t, cmd.member, test.want.member)
			}
		})
	}
}

func TestSMoveExec(t *testing.T) {
	t.Run("move", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("src", "one", "two")
		_, _ = db.Set().Add("dest", "thr", "fou")

		cmd := redis.MustParse(ParseSMove, "smove src dest one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		sone, _ := db.Set().Exists("src", "one")
		testx.AssertEqual(t, sone, false)
		done, _ := db.Set().Exists("dest", "one")
		testx.AssertEqual(t, done, true)
	})
	t.Run("dest not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("src", "one", "two")

		cmd := redis.MustParse(ParseSMove, "smove src dest one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.Out(), "1")

		sone, _ := db.Set().Exists("src", "one")
		testx.AssertEqual(t, sone, false)
		done, _ := db.Set().Exists("dest", "one")
		testx.AssertEqual(t, done, true)
	})
	t.Run("src elem not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("src", "two")
		_, _ = db.Set().Add("dest", "thr", "fou")

		cmd := redis.MustParse(ParseSMove, "smove src dest one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		sone, _ := db.Set().Exists("src", "one")
		testx.AssertEqual(t, sone, false)
		done, _ := db.Set().Exists("dest", "one")
		testx.AssertEqual(t, done, false)
	})
	t.Run("src key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("dest", "thr", "fou")

		cmd := redis.MustParse(ParseSMove, "smove src dest one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		sone, _ := db.Set().Exists("src", "one")
		testx.AssertEqual(t, sone, false)
		done, _ := db.Set().Exists("dest", "one")
		testx.AssertEqual(t, done, false)
	})
	t.Run("dest type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("src", "one", "two")
		_ = db.Str().Set("dest", "str")

		cmd := redis.MustParse(ParseSMove, "smove src dest one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.Out(), core.ErrKeyType.Error()+" (smove)")

		sone, _ := db.Set().Exists("src", "one")
		testx.AssertEqual(t, sone, true)
		done, _ := db.Set().Exists("dest", "one")
		testx.AssertEqual(t, done, false)
	})
	t.Run("src type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("src", "one")
		_, _ = db.Set().Add("dest", "thr", "fou")

		cmd := redis.MustParse(ParseSMove, "smove src dest one")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.Out(), "0")

		sone, _ := db.Set().Exists("src", "one")
		testx.AssertEqual(t, sone, false)
		done, _ := db.Set().Exists("dest", "one")
		testx.AssertEqual(t, done, false)
	})
}
