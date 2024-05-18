package set

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestSInterParse(t *testing.T) {
	tests := []struct {
		cmd  string
		want SInter
		err  error
	}{
		{
			cmd:  "sinter",
			want: SInter{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  "sinter key",
			want: SInter{keys: []string{"key"}},
			err:  nil,
		},
		{
			cmd:  "sinter k1 k2",
			want: SInter{keys: []string{"k1", "k2"}},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd, func(t *testing.T) {
			cmd, err := redis.Parse(ParseSInter, test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.keys, test.want.keys)
			}
		})
	}
}

func TestSInterExec(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two", "thr")
		_, _ = db.Set().Add("key2", "two", "thr", "fou")
		_, _ = db.Set().Add("key3", "one", "two", "thr", "fou")

		cmd := redis.MustParse(ParseSInter, "sinter key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 2)
		testx.AssertEqual(t, conn.Out(), "2,thr,two")
	})
	t.Run("no keys", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSInter, "sinter key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two", "thr")

		cmd := redis.MustParse(ParseSInter, "sinter key1")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 3)
		testx.AssertEqual(t, conn.Out(), "3,one,thr,two")
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one", "two")
		_, _ = db.Set().Add("key2", "two", "thr")
		_, _ = db.Set().Add("key3", "thr", "fou")

		cmd := redis.MustParse(ParseSInter, "sinter key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_, _ = db.Set().Add("key2", "one")

		cmd := redis.MustParse(ParseSInter, "sinter key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("all not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := redis.MustParse(ParseSInter, "sinter key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.Set().Add("key1", "one")
		_ = db.Str().Set("key2", "one")
		_, _ = db.Set().Add("key3", "one")

		cmd := redis.MustParse(ParseSInter, "sinter key1 key2 key3")
		conn := redis.NewFakeConn()
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]core.Value)), 0)
		testx.AssertEqual(t, conn.Out(), "0")
	})
}
