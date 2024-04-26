package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZUnionStoreParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZUnionStore
		err  error
	}{
		{
			name: "zunionstore",
			args: buildArgs("zunionstore"),
			want: ZUnionStore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zunionstore dest",
			args: buildArgs("zunionstore", "dest"),
			want: ZUnionStore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zunionstore dest 1",
			args: buildArgs("zunionstore", "dest", "1"),
			want: ZUnionStore{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zunionstore dest 1 key",
			args: buildArgs("zunionstore", "dest", "1", "key"),
			want: ZUnionStore{dest: "dest", keys: []string{"key"}},
			err:  nil,
		},
		{
			name: "zunionstore dest 2 k1 k2",
			args: buildArgs("zunionstore", "dest", "2", "k1", "k2"),
			want: ZUnionStore{dest: "dest", keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			name: "zunionstore dest 1 k1 k2",
			args: buildArgs("zunionstore", "dest", "1", "k1", "k2"),
			want: ZUnionStore{},
			err:  ErrSyntaxError,
		},
		{
			name: "zunionstore dest 2 k1 k2 min",
			args: buildArgs("zunionstore", "dest", "2", "k1", "k2", "min"),
			want: ZUnionStore{},
			err:  ErrSyntaxError,
		},
		{
			name: "zunionstore dest 2 k1 k2 aggregate min",
			args: buildArgs("zunionstore", "dest", "2", "k1", "k2", "aggregate", "min"),
			want: ZUnionStore{dest: "dest", keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			name: "zunionstore dest 2 k1 k2 aggregate avg",
			args: buildArgs("zunionstore", "dest", "2", "k1", "k2", "aggregate", "avg"),
			want: ZUnionStore{},
			err:  ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZUnionStore)
				testx.AssertEqual(t, cm.dest, test.want.dest)
				testx.AssertEqual(t, cm.keys, test.want.keys)
				testx.AssertEqual(t, cm.aggregate, test.want.aggregate)
			}
		})
	}
}

func TestZUnionStoreExec(t *testing.T) {
	t.Run("union", func(t *testing.T) {
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

		cmd := mustParse[*ZUnionStore]("zunionstore dest 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.out(), "4")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 4)
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
		_, _ = db.ZSet().Add("dest", "fiv", 1)

		cmd := mustParse[*ZUnionStore]("zunionstore dest 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.out(), "4")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 4)
		_, err = db.ZSet().GetScore("dest", "fiv")
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

		cmd := mustParse[*ZUnionStore]("zunionstore dest 3 key1 key2 key3 aggregate min")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 4)
		testx.AssertEqual(t, conn.out(), "4")

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

		cmd := mustParse[*ZUnionStore]("zunionstore dest 1 key1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 3)
		testx.AssertEqual(t, conn.out(), "3")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 3)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key1", "two", 1)
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := mustParse[*ZUnionStore]("zunionstore dest 2 key1 key2")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 2)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key1", "two", 2)
		_ = db.Str().Set("key2", "two")
		_, _ = db.ZSet().Add("dest", "one", 1)

		cmd := mustParse[*ZUnionStore]("zunionstore dest 2 key1 key2")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, 2)
		testx.AssertEqual(t, conn.out(), "2")

		count, _ := db.ZSet().Len("dest")
		testx.AssertEqual(t, count, 2)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key", "one", 1)
		_ = db.Str().Set("dest", "value")

		cmd := mustParse[*ZUnionStore]("zunionstore dest 1 key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, res, 0)
		testx.AssertEqual(t, conn.out(), ErrKeyType.Error()+" (zunionstore)")

		dest, _ := db.Str().Get("dest")
		testx.AssertEqual(t, dest.String(), "value")
	})
}
