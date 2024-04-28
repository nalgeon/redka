package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZInterStoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZInterStore
		err  error
	}{
		{
			name: "zinterstore",
			args: buildArgs("zinterstore"),
			want: ZInterStore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zinterstore dest",
			args: buildArgs("zinterstore", "dest"),
			want: ZInterStore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zinterstore dest 1",
			args: buildArgs("zinterstore", "dest", "1"),
			want: ZInterStore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zinterstore dest 1 key",
			args: buildArgs("zinterstore", "dest", "1", "key"),
			want: ZInterStore{dest: "dest", keys: []string{"key"}},
			err:  nil,
		},
		{
			name: "zinterstore dest 2 k1 k2",
			args: buildArgs("zinterstore", "dest", "2", "k1", "k2"),
			want: ZInterStore{dest: "dest", keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			name: "zinterstore dest 1 k1 k2",
			args: buildArgs("zinterstore", "dest", "1", "k1", "k2"),
			want: ZInterStore{},
			err:  ErrSyntaxError,
		},
		{
			name: "zinterstore dest 2 k1 k2 min",
			args: buildArgs("zinterstore", "dest", "2", "k1", "k2", "min"),
			want: ZInterStore{},
			err:  ErrSyntaxError,
		},
		{
			name: "zinterstore dest 2 k1 k2 aggregate min",
			args: buildArgs("zinterstore", "dest", "2", "k1", "k2", "aggregate", "min"),
			want: ZInterStore{dest: "dest", keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			name: "zinterstore dest 2 k1 k2 aggregate avg",
			args: buildArgs("zinterstore", "dest", "2", "k1", "k2", "aggregate", "avg"),
			want: ZInterStore{},
			err:  ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZInterStore)
				testx.AssertEqual(t, cm.dest, test.want.dest)
				testx.AssertEqual(t, cm.keys, test.want.keys)
				testx.AssertEqual(t, cm.aggregate, test.want.aggregate)
			}
		})
	}
}

func TestZInterStoreExec(t *testing.T) {
	t.Run("inter", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.ZSet().AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.ZSet().AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		cmd := mustParse[*ZInterStore]("zinterstore dest 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 2)
	})
	t.Run("overwrite", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.ZSet().AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.ZSet().AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := mustParse[*ZInterStore]("zinterstore dest 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 2)
		_, err = db.ZSet().GetScore("dest", "one")
		testx.AssertEqual(t, err, core.ErrNotFound)
	})
	t.Run("aggregate", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.ZSet().AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.ZSet().AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		cmd := mustParse[*ZInterStore]("zinterstore dest 3 key1 key2 key3 aggregate min")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		two, _ := db.ZSet().GetScore("dest", "two")
		testx.AssertEqual(t, two, 2.0)
		thr, _ := db.ZSet().GetScore("dest", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := mustParse[*ZInterStore]("zinterstore dest 1 key1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.out(), "3")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 3)
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key2", "two", 1)
		_, _ = db.ZSet().Add("key3", "thr", 1)

		cmd := mustParse[*ZInterStore]("zinterstore dest 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := mustParse[*ZInterStore]("zinterstore dest 2 key1 key2")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := mustParse[*ZInterStore]("zinterstore dest 1 key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), "0")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_ = db.Str().Set("dest", "value")

		cmd := mustParse[*ZInterStore]("zinterstore dest 1 key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 1)
		testx.AssertEqual(t, conn.out(), "1")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 1)
	})
}
