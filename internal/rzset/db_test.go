package rzset_test

import (
	"math"
	"testing"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rzset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestAdd(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		created, err := db.Add("key", "one", 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = db.Add("key", "two", 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = db.Add("key", "thr", 3)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		count, _ := db.Len("key")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 3)

		one, _ := db.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
		two, _ := db.GetScore("key", "two")
		testx.AssertEqual(t, two, 2.0)
		thr, _ := db.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("update", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		created, err := db.Add("key", "one", 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = db.Add("key", "two", 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = db.Add("key", "two", 3)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, false)

		count, _ := db.Len("key")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		one, _ := db.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
		two, _ := db.GetScore("key", "two")
		testx.AssertEqual(t, two, 3.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "string")
		created, err := db.Add("key", "one", 1)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, created, false)
	})
}

func TestAddMany(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		items := map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		}
		count, err := db.AddMany("key", items)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 3)

		count, _ = db.Len("key")
		testx.AssertEqual(t, count, 3)

		one, _ := db.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
		two, _ := db.GetScore("key", "two")
		testx.AssertEqual(t, two, 2.0)
		thr, _ := db.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("update", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "thr", 3)

		items := map[any]float64{
			"one": 10,
			"two": 20,
			"fou": 4,
			"fiv": 5,
		}
		count, err := db.AddMany("key", items)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		count, _ = db.Len("key")
		testx.AssertEqual(t, count, 5)

		one, _ := db.GetScore("key", "one")
		testx.AssertEqual(t, one, 10.0)
		two, _ := db.GetScore("key", "two")
		testx.AssertEqual(t, two, 20.0)
		thr, _ := db.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
		fou, _ := db.GetScore("key", "fou")
		testx.AssertEqual(t, fou, 4.0)
		fiv, _ := db.GetScore("key", "fiv")
		testx.AssertEqual(t, fiv, 5.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "str")

		items := map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		}
		count, err := db.AddMany("key", items)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, count, 0)
	})
}

func TestCount(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "thr", 3)
		_, _ = db.Add("key", "2nd", 2)

		tests := []struct {
			min, max float64
			count    int
		}{
			{0, 0, 0},
			{1, 1, 1},
			{1, 2, 3},
			{1, 3, 4},
			{2, 2, 2},
			{2, 3, 3},
			{3, 3, 1},
			{4, 4, 0},
			{math.Inf(-1), 1, 1},
			{1, math.Inf(1), 4},
			{math.Inf(-1), math.Inf(1), 4},
		}
		for _, test := range tests {
			count, err := db.Count("key", test.min, test.max)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, test.count)
		}
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		count, err := db.Count("key", 1, 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "str")

		count, err := db.Count("key", 1, 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
}

func TestDelete(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "thr", 3)

		count, err := db.Delete("key", "one", "two")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		count, _ = db.Len("key")
		testx.AssertEqual(t, count, 1)

		thr, _ := db.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("all", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "thr", 3)

		count, err := db.Delete("key", "one", "two", "thr")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 3)

		exists, _ := red.Key().Exists("key")
		testx.AssertEqual(t, exists, true)

		count, _ = db.Len("key")
		testx.AssertEqual(t, count, 0)

		_, err = db.GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("none", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "thr", 3)

		count, err := db.Delete("key", "fou", "fiv")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)

		count, _ = db.Len("key")
		testx.AssertEqual(t, count, 3)

		one, _ := db.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		count, err := db.Delete("key", "one", "two")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "str")

		count, err := db.Delete("key", "one", "two")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
}

func TestDeleteRank(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "2nd", 2)
		_, _ = db.Add("key", "thr", 3)

		count, err := db.DeleteWith("key").ByRank(1, 2).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		count, _ = db.Len("key")
		testx.AssertEqual(t, count, 2)

		two, _ := db.GetScore("key", "one")
		testx.AssertEqual(t, two, 1.0)
		thr, _ := db.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("negative indexes", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)

		count, err := db.DeleteWith("key").ByRank(-2, -1).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
}

func TestDeleteScore(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()
	_, _ = db.Add("key", "one", 1)
	_, _ = db.Add("key", "two", 2)
	_, _ = db.Add("key", "2nd", 2)
	_, _ = db.Add("key", "thr", 3)

	count, err := db.DeleteWith("key").ByScore(1, 2).Run()
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, count, 3)

	count, _ = db.Len("key")
	testx.AssertEqual(t, count, 1)

	thr, _ := db.GetScore("key", "thr")
	testx.AssertEqual(t, thr, 3.0)
}

func TestGetRank(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Add("key", "one", 1)
	_, _ = db.Add("key", "two", 2)
	_, _ = db.Add("key", "thr", 3)
	_, _ = db.Add("key", "2nd", 2)
	_ = red.Str().Set("str", "str")

	rank, score, _ := db.GetRank("key", "one")
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 1.0)

	rank, score, _ = db.GetRank("key", "2nd")
	testx.AssertEqual(t, rank, 1)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = db.GetRank("key", "two")
	testx.AssertEqual(t, rank, 2)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = db.GetRank("key", "thr")
	testx.AssertEqual(t, rank, 3)
	testx.AssertEqual(t, score, 3.0)

	rank, score, err := db.GetRank("key", "not")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = db.GetRank("other", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = db.GetRank("str", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)
}

func TestGetRankRev(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Add("key", "one", 1)
	_, _ = db.Add("key", "two", 2)
	_, _ = db.Add("key", "thr", 3)
	_, _ = db.Add("key", "2nd", 2)
	_ = red.Str().Set("str", "str")

	rank, score, _ := db.GetRankRev("key", "thr")
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 3.0)

	rank, score, _ = db.GetRankRev("key", "two")
	testx.AssertEqual(t, rank, 1)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = db.GetRankRev("key", "2nd")
	testx.AssertEqual(t, rank, 2)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = db.GetRankRev("key", "one")
	testx.AssertEqual(t, rank, 3)
	testx.AssertEqual(t, score, 1.0)

	rank, score, err := db.GetRankRev("key", "not")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = db.GetRankRev("other", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = db.GetRankRev("str", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)
}

func TestGetScore(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Add("key", "one", 1)
	_, _ = db.Add("key", "two", 2)
	_, _ = db.Add("key", "thr", 3)
	_, _ = db.Add("key", "2nd", 2)
	_ = red.Str().Set("str", "str")

	score, _ := db.GetScore("key", "one")
	testx.AssertEqual(t, score, 1.0)

	score, _ = db.GetScore("key", "two")
	testx.AssertEqual(t, score, 2.0)

	score, _ = db.GetScore("key", "thr")
	testx.AssertEqual(t, score, 3.0)

	score, _ = db.GetScore("key", "2nd")
	testx.AssertEqual(t, score, 2.0)

	score, err := db.GetScore("key", "not")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, score, 0.0)

	score, err = db.GetScore("other", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, score, 0.0)

	score, err = db.GetScore("str", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, score, 0.0)
}

func TestIncr(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		val, err := db.Incr("key", "one", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)
	})
	t.Run("create field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 10)
		val, err := db.Incr("key", "two", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)
	})
	t.Run("update field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 25.5)
		val, err := db.Incr("key", "one", 10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 36.0)
	})
	t.Run("decrement", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 25.5)
		val, err := db.Incr("key", "one", -10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 15.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "one")
		val, err := db.Incr("key", "one", 25.0)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0.0)
	})
}

func TestInter(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
		})
	})
	t.Run("single key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		items, err := db.Inter("key1")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("two"), Score: 2},
			{Elem: core.Value("thr"), Score: 3},
		})
	})
	t.Run("empty", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "two", 1)
		_, _ = db.Add("key3", "thr", 1)

		items, err := db.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)

		items, err := db.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("all not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		items, err := db.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key1", "str")
		_, _ = db.Add("key2", "two", 2)
		_, _ = db.Add("key3", "two", 2)

		items, err := db.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
}

func TestInterWith(t *testing.T) {
	t.Run("sum", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.InterWith("key1", "key2", "key3").Sum().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
		})
	})
	t.Run("min", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.InterWith("key1", "key2", "key3").Min().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("two"), Score: 2},
			{Elem: core.Value("thr"), Score: 3},
		})
	})
	t.Run("max", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.InterWith("key1", "key2", "key3").Max().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("thr"), Score: 3},
			{Elem: core.Value("two"), Score: 200},
		})
	})
}

func TestInterStore(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		count, err := db.InterWith("key1", "key2", "key3").Dest("dest").Max().Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		thr, _ := db.GetScore("dest", "thr")
		testx.AssertEqual(t, thr, 3.0)
		two, _ := db.GetScore("dest", "two")
		testx.AssertEqual(t, two, 200.0)
	})
	t.Run("rewrite dest", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)
		_, _ = db.Add("dest", "old", 10)

		count, err := db.InterWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)

		one, _ := db.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
		_, err = db.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("empty", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "two", 2)
		_, _ = db.Add("dest", "old", 10)

		count, err := db.InterWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)

		_, err = db.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.GetScore("dest", "two")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("source key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)

		count, err := db.InterWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)

		_, err = db.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)
		_ = red.Str().Set("key3", 3)

		count, err := db.InterWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)

		_, err = db.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)
		_ = red.Str().Set("dest", 10)

		count, err := db.InterWith("key1", "key2").Dest("dest").Store()
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, count, 0)

		old, _ := red.Str().Get("dest")
		testx.AssertEqual(t, old.String(), "10")
	})
}

func TestLen(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Add("key", "one", 1)
	_, _ = db.Add("key", "two", 2)
	_, _ = db.Add("key", "thr", 3)
	_, _ = db.Add("key", "2nd", 2)

	t.Run("count", func(t *testing.T) {
		count, err := db.Len("key")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 4)
	})
	t.Run("key not found", func(t *testing.T) {
		count, err := db.Len("not")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		_ = red.Str().Set("str", "str")
		count, err := db.Len("str")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
}

func TestRangeRank(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "thr", 3)
		_, _ = db.Add("key", "2nd", 2)

		tests := []struct {
			start, stop int
			items       []rzset.SetItem
		}{
			{0, 0, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 1},
			}},
			{0, 1, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 1}, {Elem: core.Value("2nd"), Score: 2},
			}},
			{1, 2, []rzset.SetItem{
				{Elem: core.Value("2nd"), Score: 2}, {Elem: core.Value("two"), Score: 2},
			}},
			{2, 3, []rzset.SetItem{
				{Elem: core.Value("two"), Score: 2}, {Elem: core.Value("thr"), Score: 3},
			}},
			{3, 4, []rzset.SetItem{
				{Elem: core.Value("thr"), Score: 3},
			}},
			{4, 5, []rzset.SetItem(nil)},
		}

		for _, test := range tests {
			items, err := db.Range("key", test.start, test.stop)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("desc", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)
		_, _ = db.Add("key", "thr", 3)
		_, _ = db.Add("key", "2nd", 2)

		tests := []struct {
			start, stop int
			items       []rzset.SetItem
		}{
			{0, 0, []rzset.SetItem{
				{Elem: core.Value("thr"), Score: 3},
			}},
			{0, 1, []rzset.SetItem{
				{Elem: core.Value("thr"), Score: 3}, {Elem: core.Value("two"), Score: 2},
			}},
			{1, 2, []rzset.SetItem{
				{Elem: core.Value("two"), Score: 2}, {Elem: core.Value("2nd"), Score: 2},
			}},
			{2, 3, []rzset.SetItem{
				{Elem: core.Value("2nd"), Score: 2}, {Elem: core.Value("one"), Score: 1},
			}},
			{3, 4, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 1},
			}},
			{4, 5, []rzset.SetItem(nil)},
		}

		for _, test := range tests {
			items, err := db.RangeWith("key").ByRank(test.start, test.stop).Desc().Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("negative indexes", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 1)
		_, _ = db.Add("key", "two", 2)

		items, err := db.Range("key", -2, -1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		items, err := db.Range("key", 0, 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "str")

		items, err := db.Range("key", 0, 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
}

func TestRangeScore(t *testing.T) {
	t.Run("asc", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 10)
		_, _ = db.Add("key", "two", 20)
		_, _ = db.Add("key", "thr", 30)
		_, _ = db.Add("key", "2nd", 20)

		tests := []struct {
			start, stop float64
			items       []rzset.SetItem
		}{
			{0, 10, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 10},
			}},
			{10, 20, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 10},
				{Elem: core.Value("2nd"), Score: 20},
				{Elem: core.Value("two"), Score: 20},
			}},
			{20, 20, []rzset.SetItem{
				{Elem: core.Value("2nd"), Score: 20},
				{Elem: core.Value("two"), Score: 20},
			}},
			{20, 30, []rzset.SetItem{
				{Elem: core.Value("2nd"), Score: 20},
				{Elem: core.Value("two"), Score: 20},
				{Elem: core.Value("thr"), Score: 30},
			}},
			{30, 40, []rzset.SetItem{
				{Elem: core.Value("thr"), Score: 30},
			}},
			{40, 50, []rzset.SetItem(nil)},
		}

		for _, test := range tests {
			items, err := db.RangeWith("key").ByScore(test.start, test.stop).Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("desc", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 10)
		_, _ = db.Add("key", "two", 20)
		_, _ = db.Add("key", "thr", 30)
		_, _ = db.Add("key", "2nd", 20)

		tests := []struct {
			start, stop float64
			items       []rzset.SetItem
		}{
			{0, 10, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 10},
			}},
			{10, 20, []rzset.SetItem{
				{Elem: core.Value("two"), Score: 20},
				{Elem: core.Value("2nd"), Score: 20},
				{Elem: core.Value("one"), Score: 10},
			}},
			{20, 20, []rzset.SetItem{
				{Elem: core.Value("two"), Score: 20},
				{Elem: core.Value("2nd"), Score: 20},
			}},
			{20, 30, []rzset.SetItem{
				{Elem: core.Value("thr"), Score: 30},
				{Elem: core.Value("two"), Score: 20},
				{Elem: core.Value("2nd"), Score: 20},
			}},
			{30, 40, []rzset.SetItem{
				{Elem: core.Value("thr"), Score: 30},
			}},
			{40, 50, []rzset.SetItem(nil)},
		}

		for _, test := range tests {
			items, err := db.RangeWith("key").ByScore(test.start, test.stop).Desc().Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("offset/count", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "one", 10)
		_, _ = db.Add("key", "two", 20)
		_, _ = db.Add("key", "thr", 30)
		_, _ = db.Add("key", "2nd", 20)

		tests := []struct {
			start, stop   float64
			offset, count int
			items         []rzset.SetItem
		}{
			{10, 30, 0, 0, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 10},
				{Elem: core.Value("2nd"), Score: 20},
				{Elem: core.Value("two"), Score: 20},
				{Elem: core.Value("thr"), Score: 30},
			}},
			{10, 30, 0, 2, []rzset.SetItem{
				{Elem: core.Value("one"), Score: 10},
				{Elem: core.Value("2nd"), Score: 20},
			}},
			{10, 30, 1, 0, []rzset.SetItem{
				{Elem: core.Value("2nd"), Score: 20},
				{Elem: core.Value("two"), Score: 20},
				{Elem: core.Value("thr"), Score: 30},
			}},
			{10, 30, 1, 2, []rzset.SetItem{
				{Elem: core.Value("2nd"), Score: 20},
				{Elem: core.Value("two"), Score: 20},
			}},
		}

		for _, test := range tests {
			items, err := db.RangeWith("key").
				ByScore(test.start, test.stop).
				Offset(test.offset).Count(test.count).
				Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		items, err := db.RangeWith("key").ByScore(0, 10).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "str")

		items, err := db.RangeWith("key").ByScore(0, 10).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
}

func TestScan(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Add("key", "f11", 11)
	_, _ = db.Add("key", "f12", 12)
	_, _ = db.Add("key", "f21", 21)
	_, _ = db.Add("key", "f22", 22)
	_, _ = db.Add("key", "f31", 31)
	_ = red.Str().Set("str", "str")

	tests := []struct {
		name    string
		cursor  int
		pattern string
		count   int

		wantCursor int
		wantItems  []rzset.SetItem
	}{
		{"all", 0, "*", 0, 5,
			[]rzset.SetItem{
				{Elem: core.Value("f11"), Score: 11},
				{Elem: core.Value("f12"), Score: 12},
				{Elem: core.Value("f21"), Score: 21},
				{Elem: core.Value("f22"), Score: 22},
				{Elem: core.Value("f31"), Score: 31},
			},
		},
		{"some", 0, "f2*", 10, 4,
			[]rzset.SetItem{
				{Elem: core.Value("f21"), Score: 21},
				{Elem: core.Value("f22"), Score: 22},
			},
		},
		{"none", 0, "n*", 10, 0, []rzset.SetItem(nil)},
		{"cursor 1st", 0, "*", 2, 2,
			[]rzset.SetItem{
				{Elem: core.Value("f11"), Score: 11},
				{Elem: core.Value("f12"), Score: 12},
			},
		},
		{"cursor 2nd", 2, "*", 2, 4,
			[]rzset.SetItem{
				{Elem: core.Value("f21"), Score: 21},
				{Elem: core.Value("f22"), Score: 22},
			},
		},
		{"cursor 3rd", 4, "*", 2, 5,
			[]rzset.SetItem{
				{Elem: core.Value("f31"), Score: 31},
			},
		},
		{"exhausted", 6, "*", 2, 0, []rzset.SetItem(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := db.Scan("key", test.cursor, test.pattern, test.count)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, out.Cursor, test.wantCursor)
			for i, item := range out.Items {
				testx.AssertEqual(t, item.Elem, test.wantItems[i].Elem)
				testx.AssertEqual(t, item.Score, test.wantItems[i].Score)
			}
		})
	}

	t.Run("ignore other keys", func(t *testing.T) {
		_, _ = db.Add("key1", "elem", 10)
		_, _ = db.Add("key2", "elem", 20)

		out, err := db.Scan("key1", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 1)
		testx.AssertEqual(t, out.Items[0].Elem.String(), "elem")
		testx.AssertEqual(t, out.Items[0].Score, 10.0)
	})
	t.Run("key not found", func(t *testing.T) {
		out, err := db.Scan("not", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		out, err := db.Scan("str", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 0)
	})
}

func TestScanner(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key", "f11", 11)
		_, _ = db.Add("key", "f12", 12)
		_, _ = db.Add("key", "f21", 21)
		_, _ = db.Add("key", "f22", 22)
		_, _ = db.Add("key", "f31", 31)

		var items []rzset.SetItem
		err := red.View(func(tx *redka.Tx) error {
			sc := tx.ZSet().Scanner("key", "*", 2)
			for sc.Scan() {
				items = append(items, sc.Item())
			}
			return sc.Err()
		})

		testx.AssertNoErr(t, err)
		elems := make([]string, len(items))
		scores := make([]int, len(items))

		for i, it := range items {
			elems[i] = it.Elem.String()
			scores[i] = int(it.Score)
		}
		testx.AssertEqual(t, elems, []string{"f11", "f12", "f21", "f22", "f31"})
		testx.AssertEqual(t, scores, []int{11, 12, 21, 22, 31})
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		sc := db.Scanner("not", "*", 2)
		var items []rzset.SetItem
		for sc.Scan() {
			items = append(items, sc.Item())
		}

		testx.AssertNoErr(t, sc.Err())
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key", "str")

		sc := db.Scanner("key", "*", 2)
		var items []rzset.SetItem
		for sc.Scan() {
			items = append(items, sc.Item())
		}

		testx.AssertNoErr(t, sc.Err())
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
}

func TestUnion(t *testing.T) {
	t.Run("intersecting", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 2},
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
			{Elem: core.Value("fou"), Score: 404},
		})
	})
	t.Run("distinct", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "two", 1)
		_, _ = db.Add("key3", "thr", 1)

		items, err := db.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("thr"), Score: 1},
			{Elem: core.Value("two"), Score: 1},
		})
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "two", 2)

		items, err := db.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("two"), Score: 2},
		})
	})
	t.Run("all not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		items, err := db.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("key1", "str")
		_, _ = db.Add("key2", "two", 2)
		_, _ = db.Add("key3", "two", 2)

		items, err := db.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("two"), Score: 4},
		})
	})
}

func TestUnionWith(t *testing.T) {
	t.Run("sum", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.UnionWith("key1", "key2", "key3").Sum().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 2},
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
			{Elem: core.Value("fou"), Score: 404},
		})
	})
	t.Run("min", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.UnionWith("key1", "key2", "key3").Min().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("two"), Score: 2},
			{Elem: core.Value("thr"), Score: 3},
			{Elem: core.Value("fou"), Score: 4},
		})
	})
	t.Run("max", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := db.UnionWith("key1", "key2", "key3").Max().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("thr"), Score: 3},
			{Elem: core.Value("two"), Score: 200},
			{Elem: core.Value("fou"), Score: 400},
		})
	})
}

func TestUnionStore(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = db.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = db.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = db.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		count, err := db.UnionWith("key1", "key2", "key3").Dest("dest").Max().Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 4)

		one, _ := db.GetScore("dest", "one")
		testx.AssertEqual(t, one, 1.0)
		thr, _ := db.GetScore("dest", "thr")
		testx.AssertEqual(t, thr, 3.0)
		two, _ := db.GetScore("dest", "two")
		testx.AssertEqual(t, two, 200.0)
		fou, _ := db.GetScore("dest", "fou")
		testx.AssertEqual(t, fou, 400.0)
	})
	t.Run("rewrite dest", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)
		_, _ = db.Add("dest", "old", 10)

		count, err := db.UnionWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)

		one, _ := db.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
		_, err = db.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("empty", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("dest", "old", 10)

		count, err := db.UnionWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)

		_, err = db.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("source key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)

		count, err := db.UnionWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)

		one, _ := db.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)
		_ = red.Str().Set("key3", 3)

		count, err := db.UnionWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)

		one, _ := db.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Add("key1", "one", 1)
		_, _ = db.Add("key2", "one", 2)
		_ = red.Str().Set("dest", 10)

		count, err := db.UnionWith("key1", "key2").Dest("dest").Store()
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, count, 0)

		old, _ := red.Str().Get("dest")
		testx.AssertEqual(t, old.String(), "10")
	})
}

func getDB(tb testing.TB) (*redka.DB, *rzset.DB) {
	tb.Helper()
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db, db.ZSet()
}
