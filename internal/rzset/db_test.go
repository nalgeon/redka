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
		db, zset := getDB(t)
		defer db.Close()

		created, err := zset.Add("key", "one", 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = zset.Add("key", "two", 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = zset.Add("key", "thr", 3)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 3)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 3)

		one, _ := zset.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
		two, _ := zset.GetScore("key", "two")
		testx.AssertEqual(t, two, 2.0)
		thr, _ := zset.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("update", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		created, err := zset.Add("key", "one", 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = zset.Add("key", "two", 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		created, err = zset.Add("key", "two", 3)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, false)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 3)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 2)

		one, _ := zset.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
		two, _ := zset.GetScore("key", "two")
		testx.AssertEqual(t, two, 3.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "string")

		created, err := zset.Add("key", "one", 1)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, created, false)

		_, err = zset.GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("key")
		testx.AssertEqual(t, sval.String(), "string")
	})
}

func TestAddMany(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		items := map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		}
		n, err := zset.AddMany("key", items)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 3)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 3)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 3)

		one, _ := zset.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
		two, _ := zset.GetScore("key", "two")
		testx.AssertEqual(t, two, 2.0)
		thr, _ := zset.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("update", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "thr", 3)

		items := map[any]float64{
			"one": 10,
			"two": 20,
			"fou": 4,
			"fiv": 5,
		}
		n, err := zset.AddMany("key", items)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 2)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 7)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 5)

		one, _ := zset.GetScore("key", "one")
		testx.AssertEqual(t, one, 10.0)
		two, _ := zset.GetScore("key", "two")
		testx.AssertEqual(t, two, 20.0)
		thr, _ := zset.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
		fou, _ := zset.GetScore("key", "fou")
		testx.AssertEqual(t, fou, 4.0)
		fiv, _ := zset.GetScore("key", "fiv")
		testx.AssertEqual(t, fiv, 5.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		items := map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		}
		n, err := zset.AddMany("key", items)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, n, 0)

		_, err = zset.GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = zset.GetScore("key", "two")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = zset.GetScore("key", "thr")
		testx.AssertErr(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("key")
		testx.AssertEqual(t, sval.String(), "str")
	})
}

func TestCount(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "thr", 3)
		_, _ = zset.Add("key", "2nd", 2)

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
			count, err := zset.Count("key", test.min, test.max)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, test.count)
		}
	})
	t.Run("key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		count, err := zset.Count("key", 1, 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		count, err := zset.Count("key", 1, 2)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
}

func TestDelete(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "thr", 3)

		n, err := zset.Delete("key", "one", "two")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 2)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 4)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 1)

		thr, _ := zset.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("all", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "thr", 3)

		n, err := zset.Delete("key", "one", "two", "thr")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 3)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 4)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 0)

		_, err = zset.GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("none", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "thr", 3)

		n, err := zset.Delete("key", "fou", "fiv")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 3)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 3)

		one, _ := zset.GetScore("key", "one")
		testx.AssertEqual(t, one, 1.0)
	})
	t.Run("key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		n, err := zset.Delete("key", "one", "two")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		n, err := zset.Delete("key", "one", "two")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)
	})
}

func TestDeleteRank(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "2nd", 2)
		_, _ = zset.Add("key", "thr", 3)

		n, err := zset.DeleteWith("key").ByRank(1, 2).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 2)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 5)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 2)

		two, _ := zset.GetScore("key", "one")
		testx.AssertEqual(t, two, 1.0)
		thr, _ := zset.GetScore("key", "thr")
		testx.AssertEqual(t, thr, 3.0)
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)

		n, err := zset.DeleteWith("key").ByRank(-2, -1).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)
	})
}

func TestDeleteScore(t *testing.T) {
	db, zset := getDB(t)
	defer db.Close()
	_, _ = zset.Add("key", "one", 1)
	_, _ = zset.Add("key", "two", 2)
	_, _ = zset.Add("key", "2nd", 2)
	_, _ = zset.Add("key", "thr", 3)

	n, err := zset.DeleteWith("key").ByScore(1, 2).Run()
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, n, 3)

	key, _ := db.Key().Get("key")
	testx.AssertEqual(t, key.Version, 5)

	zlen, _ := zset.Len("key")
	testx.AssertEqual(t, zlen, 1)

	thr, _ := zset.GetScore("key", "thr")
	testx.AssertEqual(t, thr, 3.0)
}

func TestGetRank(t *testing.T) {
	db, zset := getDB(t)
	defer db.Close()

	_, _ = zset.Add("key", "one", 1)
	_, _ = zset.Add("key", "two", 2)
	_, _ = zset.Add("key", "thr", 3)
	_, _ = zset.Add("key", "2nd", 2)
	_ = db.Str().Set("str", "str")

	rank, score, _ := zset.GetRank("key", "one")
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 1.0)

	rank, score, _ = zset.GetRank("key", "2nd")
	testx.AssertEqual(t, rank, 1)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = zset.GetRank("key", "two")
	testx.AssertEqual(t, rank, 2)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = zset.GetRank("key", "thr")
	testx.AssertEqual(t, rank, 3)
	testx.AssertEqual(t, score, 3.0)

	rank, score, err := zset.GetRank("key", "not")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = zset.GetRank("other", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = zset.GetRank("str", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)
}

func TestGetRankRev(t *testing.T) {
	db, zset := getDB(t)
	defer db.Close()

	_, _ = zset.Add("key", "one", 1)
	_, _ = zset.Add("key", "two", 2)
	_, _ = zset.Add("key", "thr", 3)
	_, _ = zset.Add("key", "2nd", 2)
	_ = db.Str().Set("str", "str")

	rank, score, _ := zset.GetRankRev("key", "thr")
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 3.0)

	rank, score, _ = zset.GetRankRev("key", "two")
	testx.AssertEqual(t, rank, 1)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = zset.GetRankRev("key", "2nd")
	testx.AssertEqual(t, rank, 2)
	testx.AssertEqual(t, score, 2.0)

	rank, score, _ = zset.GetRankRev("key", "one")
	testx.AssertEqual(t, rank, 3)
	testx.AssertEqual(t, score, 1.0)

	rank, score, err := zset.GetRankRev("key", "not")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = zset.GetRankRev("other", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)

	rank, score, err = zset.GetRankRev("str", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, rank, 0)
	testx.AssertEqual(t, score, 0.0)
}

func TestGetScore(t *testing.T) {
	db, zset := getDB(t)
	defer db.Close()

	_, _ = zset.Add("key", "one", 1)
	_, _ = zset.Add("key", "two", 2)
	_, _ = zset.Add("key", "thr", 3)
	_, _ = zset.Add("key", "2nd", 2)
	_ = db.Str().Set("str", "str")

	score, _ := zset.GetScore("key", "one")
	testx.AssertEqual(t, score, 1.0)

	score, _ = zset.GetScore("key", "two")
	testx.AssertEqual(t, score, 2.0)

	score, _ = zset.GetScore("key", "thr")
	testx.AssertEqual(t, score, 3.0)

	score, _ = zset.GetScore("key", "2nd")
	testx.AssertEqual(t, score, 2.0)

	score, err := zset.GetScore("key", "not")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, score, 0.0)

	score, err = zset.GetScore("other", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, score, 0.0)

	score, err = zset.GetScore("str", "one")
	testx.AssertErr(t, err, core.ErrNotFound)
	testx.AssertEqual(t, score, 0.0)
}

func TestIncr(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		val, err := zset.Incr("key", "one", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 1)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 1)
	})
	t.Run("create field", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 10)
		val, err := zset.Incr("key", "two", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 2)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 2)
	})
	t.Run("update field", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 25.5)
		val, err := zset.Incr("key", "one", 10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 36.0)

		key, _ := db.Key().Get("key")
		testx.AssertEqual(t, key.Version, 2)

		zlen, _ := zset.Len("key")
		testx.AssertEqual(t, zlen, 1)
	})
	t.Run("decrement", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 25.5)
		val, err := zset.Incr("key", "one", -10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 15.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "one")

		val, err := zset.Incr("key", "one", 25.0)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0.0)

		_, err = zset.GetScore("key", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestInter(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
		})
	})
	t.Run("single key", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})

		items, err := zset.Inter("key1")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("two"), Score: 2},
			{Elem: core.Value("thr"), Score: 3},
		})
	})
	t.Run("empty", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "two", 1)
		_, _ = zset.Add("key3", "thr", 1)

		items, err := zset.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)

		items, err := zset.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("all not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		items, err := zset.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key1", "str")
		_, _ = zset.Add("key2", "two", 2)
		_, _ = zset.Add("key3", "two", 2)

		items, err := zset.Inter("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
}

func TestInterWith(t *testing.T) {
	t.Run("sum", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.InterWith("key1", "key2", "key3").Sum().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
		})
	})
	t.Run("min", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.InterWith("key1", "key2", "key3").Min().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("two"), Score: 2},
			{Elem: core.Value("thr"), Score: 3},
		})
	})
	t.Run("max", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.InterWith("key1", "key2", "key3").Max().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("thr"), Score: 3},
			{Elem: core.Value("two"), Score: 200},
		})
	})
}

func TestInterStore(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		n, err := zset.InterWith("key1", "key2", "key3").Dest("dest").Max().Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 2)

		key, _ := db.Key().Get("dest")
		testx.AssertEqual(t, key.Version, 1)

		zlen, _ := zset.Len("dest")
		testx.AssertEqual(t, zlen, 2)

		thr, _ := zset.GetScore("dest", "thr")
		testx.AssertEqual(t, thr, 3.0)
		two, _ := zset.GetScore("dest", "two")
		testx.AssertEqual(t, two, 200.0)
	})
	t.Run("rewrite dest", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)
		_, _ = zset.Add("dest", "old", 10)

		n, err := zset.InterWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 1)

		key, _ := db.Key().Get("dest")
		testx.AssertEqual(t, key.Version, 1)

		zlen, _ := zset.Len("dest")
		testx.AssertEqual(t, zlen, 1)

		one, _ := zset.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
		_, err = zset.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("empty", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "two", 2)
		_, _ = zset.Add("dest", "old", 10)

		n, err := zset.InterWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)

		key, _ := db.Key().Get("dest")
		testx.AssertEqual(t, key.Version, 1)

		zlen, _ := zset.Len("dest")
		testx.AssertEqual(t, zlen, 0)

		_, err = zset.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = zset.GetScore("dest", "two")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = zset.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)

		n, err := zset.InterWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)

		_, err = zset.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)
		_ = db.Str().Set("key3", 3)

		n, err := zset.InterWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)

		_, err = zset.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)
		_ = db.Str().Set("dest", 10)

		n, err := zset.InterWith("key1", "key2").Dest("dest").Store()
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, n, 0)

		_, err = zset.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("dest")
		testx.AssertEqual(t, sval.String(), "10")
	})
}

func TestLen(t *testing.T) {
	db, zset := getDB(t)
	defer db.Close()

	_, _ = zset.Add("key", "one", 1)
	_, _ = zset.Add("key", "two", 2)
	_, _ = zset.Add("key", "thr", 3)
	_, _ = zset.Add("key", "2nd", 2)

	t.Run("count", func(t *testing.T) {
		count, err := zset.Len("key")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 4)
	})
	t.Run("key not found", func(t *testing.T) {
		count, err := zset.Len("not")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		_ = db.Str().Set("str", "str")
		count, err := zset.Len("str")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)
	})
}

func TestRangeRank(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "thr", 3)
		_, _ = zset.Add("key", "2nd", 2)

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
			items, err := zset.Range("key", test.start, test.stop)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("desc", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)
		_, _ = zset.Add("key", "thr", 3)
		_, _ = zset.Add("key", "2nd", 2)

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
			items, err := zset.RangeWith("key").ByRank(test.start, test.stop).Desc().Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("negative indexes", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 1)
		_, _ = zset.Add("key", "two", 2)

		items, err := zset.Range("key", -2, -1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		items, err := zset.Range("key", 0, 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		items, err := zset.Range("key", 0, 1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
}

func TestRangeScore(t *testing.T) {
	t.Run("asc", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 10)
		_, _ = zset.Add("key", "two", 20)
		_, _ = zset.Add("key", "thr", 30)
		_, _ = zset.Add("key", "2nd", 20)

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
			items, err := zset.RangeWith("key").ByScore(test.start, test.stop).Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("desc", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 10)
		_, _ = zset.Add("key", "two", 20)
		_, _ = zset.Add("key", "thr", 30)
		_, _ = zset.Add("key", "2nd", 20)

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
			items, err := zset.RangeWith("key").ByScore(test.start, test.stop).Desc().Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("offset/count", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "one", 10)
		_, _ = zset.Add("key", "two", 20)
		_, _ = zset.Add("key", "thr", 30)
		_, _ = zset.Add("key", "2nd", 20)

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
			items, err := zset.RangeWith("key").
				ByScore(test.start, test.stop).
				Offset(test.offset).Count(test.count).
				Run()
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		}
	})
	t.Run("key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		items, err := zset.RangeWith("key").ByScore(0, 10).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		items, err := zset.RangeWith("key").ByScore(0, 10).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
}

func TestScan(t *testing.T) {
	db, zset := getDB(t)
	defer db.Close()

	_, _ = zset.Add("key", "f11", 11)
	_, _ = zset.Add("key", "f12", 12)
	_, _ = zset.Add("key", "f21", 21)
	_, _ = zset.Add("key", "f22", 22)
	_, _ = zset.Add("key", "f31", 31)
	_ = db.Str().Set("str", "str")

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
			out, err := zset.Scan("key", test.cursor, test.pattern, test.count)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, out.Cursor, test.wantCursor)
			for i, item := range out.Items {
				testx.AssertEqual(t, item.Elem, test.wantItems[i].Elem)
				testx.AssertEqual(t, item.Score, test.wantItems[i].Score)
			}
		})
	}

	t.Run("ignore other keys", func(t *testing.T) {
		_, _ = zset.Add("key1", "elem", 10)
		_, _ = zset.Add("key2", "elem", 20)

		out, err := zset.Scan("key1", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 1)
		testx.AssertEqual(t, out.Items[0].Elem.String(), "elem")
		testx.AssertEqual(t, out.Items[0].Score, 10.0)
	})
	t.Run("key not found", func(t *testing.T) {
		out, err := zset.Scan("not", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		out, err := zset.Scan("str", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 0)
	})
}

func TestScanner(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key", "f11", 11)
		_, _ = zset.Add("key", "f12", 12)
		_, _ = zset.Add("key", "f21", 21)
		_, _ = zset.Add("key", "f22", 22)
		_, _ = zset.Add("key", "f31", 31)

		var items []rzset.SetItem
		err := db.View(func(tx *redka.Tx) error {
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
		db, zset := getDB(t)
		defer db.Close()

		sc := zset.Scanner("not", "*", 2)
		var items []rzset.SetItem
		for sc.Scan() {
			items = append(items, sc.Item())
		}

		testx.AssertNoErr(t, sc.Err())
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		sc := zset.Scanner("key", "*", 2)
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
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 2},
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
			{Elem: core.Value("fou"), Score: 404},
		})
	})
	t.Run("distinct", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "two", 1)
		_, _ = zset.Add("key3", "thr", 1)

		items, err := zset.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("thr"), Score: 1},
			{Elem: core.Value("two"), Score: 1},
		})
	})
	t.Run("key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "two", 2)

		items, err := zset.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("two"), Score: 2},
		})
	})
	t.Run("all not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		items, err := zset.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key1", "str")
		_, _ = zset.Add("key2", "two", 2)
		_, _ = zset.Add("key3", "two", 2)

		items, err := zset.Union("key1", "key2", "key3")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("two"), Score: 4},
		})
	})
}

func TestUnionWith(t *testing.T) {
	t.Run("sum", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.UnionWith("key1", "key2", "key3").Sum().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 2},
			{Elem: core.Value("thr"), Score: 9},
			{Elem: core.Value("two"), Score: 222},
			{Elem: core.Value("fou"), Score: 404},
		})
	})
	t.Run("min", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.UnionWith("key1", "key2", "key3").Min().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, items, []rzset.SetItem{
			{Elem: core.Value("one"), Score: 1},
			{Elem: core.Value("two"), Score: 2},
			{Elem: core.Value("thr"), Score: 3},
			{Elem: core.Value("fou"), Score: 4},
		})
	})
	t.Run("max", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		items, err := zset.UnionWith("key1", "key2", "key3").Max().Run()
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
		db, zset := getDB(t)
		defer db.Close()
		_, _ = zset.AddMany("key1", map[any]float64{
			"one": 1,
			"two": 2,
			"thr": 3,
		})
		_, _ = zset.AddMany("key2", map[any]float64{
			"two": 20,
			"thr": 3,
			"fou": 4,
		})
		_, _ = zset.AddMany("key3", map[any]float64{
			"one": 1,
			"two": 200,
			"thr": 3,
			"fou": 400,
		})

		n, err := zset.UnionWith("key1", "key2", "key3").Dest("dest").Max().Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 4)

		key, _ := db.Key().Get("dest")
		testx.AssertEqual(t, key.Version, 1)

		zlen, _ := zset.Len("dest")
		testx.AssertEqual(t, zlen, 4)

		one, _ := zset.GetScore("dest", "one")
		testx.AssertEqual(t, one, 1.0)
		thr, _ := zset.GetScore("dest", "thr")
		testx.AssertEqual(t, thr, 3.0)
		two, _ := zset.GetScore("dest", "two")
		testx.AssertEqual(t, two, 200.0)
		fou, _ := zset.GetScore("dest", "fou")
		testx.AssertEqual(t, fou, 400.0)
	})
	t.Run("rewrite dest", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)
		_, _ = zset.Add("dest", "old", 10)

		n, err := zset.UnionWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 1)

		key, _ := db.Key().Get("dest")
		testx.AssertEqual(t, key.Version, 1)

		zlen, _ := zset.Len("dest")
		testx.AssertEqual(t, zlen, 1)

		one, _ := zset.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
		_, err = zset.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("empty", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("dest", "old", 10)

		n, err := zset.UnionWith("key1", "key2").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)

		key, _ := db.Key().Get("dest")
		testx.AssertEqual(t, key.Version, 1)

		zlen, _ := zset.Len("dest")
		testx.AssertEqual(t, zlen, 0)

		_, err = zset.GetScore("dest", "old")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)

		n, err := zset.UnionWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 1)

		one, _ := zset.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)
		_ = db.Str().Set("key3", 3)

		n, err := zset.UnionWith("key1", "key2", "key3").Dest("dest").Store()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 1)

		one, _ := zset.GetScore("dest", "one")
		testx.AssertEqual(t, one, 3.0)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, zset := getDB(t)
		defer db.Close()

		_, _ = zset.Add("key1", "one", 1)
		_, _ = zset.Add("key2", "one", 2)
		_ = db.Str().Set("dest", 10)

		n, err := zset.UnionWith("key1", "key2").Dest("dest").Store()
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, n, 0)

		_, err = zset.GetScore("dest", "one")
		testx.AssertErr(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("dest")
		testx.AssertEqual(t, sval.String(), "10")
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
