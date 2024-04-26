package command

import (
	"testing"

	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestZUnionParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want ZUnion
		err  error
	}{
		{
			name: "zunion",
			args: buildArgs("zunion"),
			want: ZUnion{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zunion 1",
			args: buildArgs("zunion", "1"),
			want: ZUnion{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "zunion 1 key",
			args: buildArgs("zunion", "1", "key"),
			want: ZUnion{keys: []string{"key"}},
			err:  nil,
		},
		{
			name: "zunion 2 k1 k2",
			args: buildArgs("zunion", "2", "k1", "k2"),
			want: ZUnion{keys: []string{"k1", "k2"}},
			err:  nil,
		},
		{
			name: "zunion 1 k1 k2",
			args: buildArgs("zunion", "1", "k1", "k2"),
			want: ZUnion{},
			err:  ErrSyntaxError,
		},
		{
			name: "zunion 2 k1 k2 min",
			args: buildArgs("zunion", "2", "k1", "k2", "min"),
			want: ZUnion{},
			err:  ErrSyntaxError,
		},
		{
			name: "zunion 2 k1 k2 aggregate min",
			args: buildArgs("zunion", "2", "k1", "k2", "aggregate", "min"),
			want: ZUnion{keys: []string{"k1", "k2"}, aggregate: "min"},
			err:  nil,
		},
		{
			name: "zunion 2 k1 k2 aggregate avg",
			args: buildArgs("zunion", "2", "k1", "k2", "aggregate", "avg"),
			want: ZUnion{},
			err:  ErrSyntaxError,
		},
		{
			name: "zunion 2 k1 k2 withscores",
			args: buildArgs("zunion", "2", "k1", "k2", "withscores"),
			want: ZUnion{keys: []string{"k1", "k2"}, withScores: true},
			err:  nil,
		},
		{
			name: "zunion 3 k1 k2 k3 withscores aggregate sum",
			args: buildArgs("zunion", "3", "k1", "k2", "k3", "withscores", "aggregate", "sum"),
			want: ZUnion{keys: []string{"k1", "k2", "k3"}, aggregate: "sum", withScores: true},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*ZUnion)
				testx.AssertEqual(t, cm.keys, test.want.keys)
				testx.AssertEqual(t, cm.aggregate, test.want.aggregate)
				testx.AssertEqual(t, cm.withScores, test.want.withScores)
			}
		})
	}
}

func TestZUnionExec(t *testing.T) {
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

		cmd := mustParse[*ZUnion]("zunion 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.out(), "4,one,thr,two,fou")
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

		cmd := mustParse[*ZUnion]("zunion 3 key1 key2 key3 withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.out(), "8,one,2,thr,9,two,222,fou,404")
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

		cmd := mustParse[*ZUnion]("zunion 3 key1 key2 key3 aggregate min withscores")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 4)
		testx.AssertEqual(t, conn.out(), "8,one,1,two,2,thr,3,fou,4")
	})
	t.Run("single key", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		cmd := mustParse[*ZUnion]("zunion 1 key1")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 3)
		testx.AssertEqual(t, conn.out(), "3,one,two,thr")
	})
	t.Run("key not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)

		cmd := mustParse[*ZUnion]("zunion 3 key1 key2 key3")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
		testx.AssertEqual(t, conn.out(), "1,one")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()
		_, _ = db.ZSet().Add("key1", "one", 1)
		_ = db.Str().Set("key2", "value")

		cmd := mustParse[*ZUnion]("zunion 2 key1 key2")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(res.([]rzset.SetItem)), 1)
		testx.AssertEqual(t, conn.out(), "1,one")
	})
}
