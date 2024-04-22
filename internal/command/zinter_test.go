package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZInterParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZInter
		err  error
	}{
		{
			name: "zinter",
			args: buildArgs("zinter"),
			want: ZInter{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zinter 1",
			args: buildArgs("zinter", "1"),
			want: ZInter{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zinter 1 key",
			args: buildArgs("zinter", "1", "key"),
			want: ZInter{keys: []string{"key"}},
			err:  nil,
		},
		{
			name: "zinter 2 k1 k2",
			args: buildArgs("zinter", "2", "k1", "k2"),
			want: ZInter{keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			name: "zinter 1 k1 k2",
			args: buildArgs("zinter", "1", "k1", "k2"),
			want: ZInter{},
			err:  ErrSyntaxError,
		},
		{
			name: "zinter 2 k1 k2 min",
			args: buildArgs("zinter", "2", "k1", "k2", "min"),
			want: ZInter{},
			err:  ErrSyntaxError,
		},
		{
			name: "zinter 2 k1 k2 aggregate min",
			args: buildArgs("zinter", "2", "k1", "k2", "aggregate", "min"),
			want: ZInter{keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			name: "zinter 2 k1 k2 aggregate avg",
			args: buildArgs("zinter", "2", "k1", "k2", "aggregate", "avg"),
			want: ZInter{},
			err:  ErrSyntaxError,
		},
		{
			name: "zinter 2 k1 k2 withscores",
			args: buildArgs("zinter", "2", "k1", "k2", "withscores"),
			want: ZInter{keys: []string{"k1", "k2"}, withScores: true},
			err:  nil,
		},
		{
			name: "zinter 3 k1 k2 k3 withscores aggregate sum",
			args: buildArgs("zinter", "3", "k1", "k2", "k3", "withscores", "aggregate", "sum"),
			want: ZInter{keys: []string{"k1", "k2", "k3"}, aggregate: "sum", withScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZInter)
				testx.AssertEqual(t, cm.keys, test.want.keys)
				testx.AssertEqual(t, cm.aggregate, test.want.aggregate)
				testx.AssertEqual(t, cm.withScores, test.want.withScores)
			}
		})
	}
}

func TestZInterExec(t *testing.T) {
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

		cmd := mustParse[*ZInter]("zinter 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
		testx.AssertEqual(t, conn.out(), "2,thr,two")
	})
	t.Run("withscores", func(t *testing.T) {
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

		cmd := mustParse[*ZInter]("zinter 3 key1 key2 key3 withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
		testx.AssertEqual(t, conn.out(), "4,thr,9,two,222")
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

		cmd := mustParse[*ZInter]("zinter 3 key1 key2 key3 aggregate min withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 2)
		testx.AssertEqual(t, conn.out(), "4,two,2,thr,3")
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := mustParse[*ZInter]("zinter 1 key1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.out(), "3,one,two,thr")
	})
	t.Run("empty", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_, _ = db.ZSet().Add("key2", "two", 1)
		_, _ = db.ZSet().Add("key3", "thr", 1)

		cmd := mustParse[*ZInter]("zinter 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*ZInter]("zinter 1 key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "value")

		cmd := mustParse[*ZInter]("zinter 1 key")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 0)
		testx.AssertEqual(t, conn.out(), "0")
	})
}
