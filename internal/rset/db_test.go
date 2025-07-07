package rset_test

import (
	"slices"
	"sort"
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rset"
	"github.com/nalgeon/redka/internal/testx"
)

func TestAdd(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		n, err := set.Add("key", "one", "two", "thr")
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("key")
		be.Equal(t, slen, 3)

		for _, elem := range []string{"one", "two", "thr"} {
			exists, _ := set.Exists("key", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("update", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two", "thr")

		n, err := set.Add("key", "one", "two", "fou", "fiv")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		slen, _ := set.Len("key")
		be.Equal(t, slen, 5)

		for _, elem := range []string{"one", "two", "thr", "fou", "fiv"} {
			exists, _ := set.Exists("key", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		n, err := set.Add("key", "one", "two", "thr")
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, n, 0)

		for _, elem := range []string{"one", "two", "thr"} {
			exists, _ := set.Exists("key", elem)
			be.Equal(t, exists, false)
		}

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "str")
	})
}

func TestDelete(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two", "thr")

		n, err := set.Delete("key", "one", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		slen, _ := set.Len("key")
		be.Equal(t, slen, 1)

		thr, _ := set.Exists("key", "thr")
		be.Equal(t, thr, true)
	})
	t.Run("all", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two", "thr")

		n, err := set.Delete("key", "one", "two", "thr")
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		slen, _ := set.Len("key")
		be.Equal(t, slen, 0)

		one, _ := set.Exists("key", "one")
		be.Equal(t, one, false)
	})
	t.Run("none", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two", "thr")

		n, err := set.Delete("key", "fou", "fiv")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("key")
		be.Equal(t, slen, 3)

		one, _ := set.Exists("key", "one")
		be.Equal(t, one, true)
	})
	t.Run("key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		n, err := set.Delete("key", "one", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		n, err := set.Delete("key", "one", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
}

func TestDiff(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr", "fiv")
		_, _ = set.Add("key2", "two", "fou", "six")
		_, _ = set.Add("key3", "thr", "six")

		items, err := set.Diff("key1", "key2", "key3")
		be.Err(t, err, nil)
		sort.Slice(items, func(i, j int) bool {
			return slices.Compare(items[i], items[j]) < 0
		})
		be.Equal(t, items, []core.Value{
			core.Value("fiv"), core.Value("one"),
		})
	})
	t.Run("no keys", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		items, err := set.Diff()
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("single key", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr")

		items, err := set.Diff("key1")
		sort.Slice(items, func(i, j int) bool {
			return slices.Compare(items[i], items[j]) < 0
		})
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value{
			core.Value("one"), core.Value("thr"), core.Value("two"),
		})
	})
	t.Run("empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two")
		_, _ = set.Add("key2", "one", "fou")
		_, _ = set.Add("key3", "two", "fiv")

		items, err := set.Diff("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("first not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key2", "two")
		_, _ = set.Add("key3", "thr")

		items, err := set.Diff("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("rest not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "two")

		items, err := set.Diff("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value{core.Value("one")})
	})
	t.Run("all not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		items, err := set.Diff("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one")
		_ = db.Str().Set("key2", "two")
		_, _ = set.Add("key3", "thr")

		items, err := set.Diff("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value{core.Value("one")})
	})
}

func TestDiffStore(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr", "fiv")
		_, _ = set.Add("key2", "two", "fou", "six")
		_, _ = set.Add("key3", "thr", "six")

		n, err := set.DiffStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 2)

		for _, elem := range []string{"one", "fiv"} {
			exists, _ := set.Exists("dest", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("rewrite dest", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "two")
		_, _ = set.Add("dest", "old")

		n, err := set.DiffStore("dest", "key1", "key2")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, true)
		old, _ := set.Exists("dest", "old")
		be.Equal(t, old, false)
	})
	t.Run("no keys", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("dest", "old")

		n, err := set.DiffStore("dest")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "old")
		be.Equal(t, one, true)
	})
	t.Run("single key", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two")
		_, _ = set.Add("dest", "old")

		n, err := set.DiffStore("dest", "key")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 2)

		for _, elem := range []string{"one", "two"} {
			exists, _ := set.Exists("dest", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")
		_, _ = set.Add("dest", "old")

		n, err := set.DiffStore("dest", "key1", "key2")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 0)

		old, _ := set.Exists("dest", "old")
		be.Equal(t, old, false)
	})
	t.Run("source first key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key2", "two")
		_, _ = set.Add("key3", "thr")
		_, _ = set.Add("dest", "old")

		n, err := set.DiffStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 0)
	})
	t.Run("source rest key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "two")

		n, err := set.DiffStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, true)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "two")
		_ = db.Str().Set("key3", "thr")

		n, err := set.DiffStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, true)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "two")
		_ = db.Str().Set("dest", "old")

		n, err := set.DiffStore("dest", "key1", "key2")
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, n, 0)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, false)

		sval, _ := db.Str().Get("dest")
		be.Equal(t, sval.String(), "old")
	})
}

func TestExists(t *testing.T) {
	db, set := getDB(t)
	defer db.Close()

	_, _ = set.Add("key", "one", "two", "thr")
	_ = db.Str().Set("str", "str")

	one, err := set.Exists("key", "one")
	be.Err(t, err, nil)
	be.Equal(t, one, true)

	two, err := set.Exists("key", "two")
	be.Err(t, err, nil)
	be.Equal(t, two, true)

	thr, err := set.Exists("key", "thr")
	be.Err(t, err, nil)
	be.Equal(t, thr, true)

	otherElem, err := set.Exists("key", "other")
	be.Err(t, err, nil)
	be.Equal(t, otherElem, false)

	otherKey, err := set.Exists("other", "one")
	be.Err(t, err, nil)
	be.Equal(t, otherKey, false)

	str, err := set.Exists("str", "one")
	be.Err(t, err, nil)
	be.Equal(t, str, false)
}

func TestInter(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr")
		_, _ = set.Add("key2", "two", "thr", "fou")
		_, _ = set.Add("key3", "one", "two", "thr", "fou")

		items, err := set.Inter("key1", "key2", "key3")
		be.Err(t, err, nil)
		sort.Slice(items, func(i, j int) bool {
			return slices.Compare(items[i], items[j]) < 0
		})
		be.Equal(t, items, []core.Value{
			core.Value("thr"), core.Value("two"),
		})
	})
	t.Run("no keys", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		items, err := set.Inter()
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("single key", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr")

		items, err := set.Inter("key1")
		be.Err(t, err, nil)
		sort.Slice(items, func(i, j int) bool {
			return slices.Compare(items[i], items[j]) < 0
		})
		be.Equal(t, items, []core.Value{
			core.Value("one"), core.Value("thr"), core.Value("two"),
		})
	})
	t.Run("empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two")
		_, _ = set.Add("key2", "two", "thr")
		_, _ = set.Add("key3", "thr", "fou")

		items, err := set.Inter("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")

		items, err := set.Inter("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("all not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		items, err := set.Inter("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one")
		_ = db.Str().Set("key2", "one")
		_, _ = set.Add("key3", "one")

		items, err := set.Inter("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
}

func TestInterStore(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr")
		_, _ = set.Add("key2", "two", "thr", "fou")
		_, _ = set.Add("key3", "one", "two", "thr", "fou")

		n, err := set.InterStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 2)

		for _, elem := range []string{"two", "thr"} {
			exists, _ := set.Exists("dest", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("rewrite dest", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")
		_, _ = set.Add("dest", "old")

		n, err := set.InterStore("dest", "key1", "key2")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, true)
		old, _ := set.Exists("dest", "old")
		be.Equal(t, old, false)
	})
	t.Run("no keys", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("dest", "old")

		n, err := set.InterStore("dest")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "old")
		be.Equal(t, one, true)
	})
	t.Run("single key", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two")
		_, _ = set.Add("dest", "old")

		n, err := set.InterStore("dest", "key")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 2)

		for _, elem := range []string{"one", "two"} {
			exists, _ := set.Exists("dest", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "two")
		_, _ = set.Add("dest", "old")

		n, err := set.InterStore("dest", "key1", "key2")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 0)

		old, _ := set.Exists("dest", "old")
		be.Equal(t, old, false)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")

		n, err := set.InterStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, false)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")
		_ = db.Str().Set("key3", "one")

		n, err := set.InterStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, false)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")
		_ = db.Str().Set("dest", "old")

		n, err := set.InterStore("dest", "key1", "key2")
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, n, 0)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, false)

		sval, _ := db.Str().Get("dest")
		be.Equal(t, sval.String(), "old")
	})
}

func TestItems(t *testing.T) {
	t.Run("items", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two", "thr")

		items, err := set.Items("key")
		be.Err(t, err, nil)
		sort.Slice(items, func(i, j int) bool {
			return slices.Compare(items[i], items[j]) < 0
		})
		be.Equal(t, items, []core.Value{
			core.Value("one"), core.Value("thr"), core.Value("two"),
		})
	})
	t.Run("key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		items, err := set.Items("key")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		items, err := set.Items("key")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
}

func TestLen(t *testing.T) {
	db, set := getDB(t)
	defer db.Close()
	_, _ = set.Add("key", "one", "two", "thr")

	t.Run("count", func(t *testing.T) {
		slen, err := set.Len("key")
		be.Err(t, err, nil)
		be.Equal(t, slen, 3)
	})
	t.Run("key not found", func(t *testing.T) {
		slen, err := set.Len("not")
		be.Err(t, err, nil)
		be.Equal(t, slen, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		_ = db.Str().Set("str", "str")
		slen, err := set.Len("str")
		be.Err(t, err, nil)
		be.Equal(t, slen, 0)
	})
}

func TestMove(t *testing.T) {
	t.Run("move", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("src", "one", "two")
		_, _ = set.Add("dest", "thr", "fou")

		err := set.Move("src", "dest", "one")
		be.Err(t, err, nil)

		skey, _ := db.Key().Get("src")
		be.Equal(t, skey.Version, 2)
		slen, _ := set.Len("src")
		be.Equal(t, slen, 1)
		sone, _ := set.Exists("src", "one")
		be.Equal(t, sone, false)

		dkey, _ := db.Key().Get("dest")
		be.Equal(t, dkey.Version, 2)
		dlen, _ := set.Len("dest")
		be.Equal(t, dlen, 3)
		done, _ := set.Exists("dest", "one")
		be.Equal(t, done, true)
	})
	t.Run("move last", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("src", "one")
		_, _ = set.Add("dest", "thr", "fou")

		err := set.Move("src", "dest", "one")
		be.Err(t, err, nil)

		skey, _ := db.Key().Get("src")
		be.Equal(t, skey.Version, 2)
		slen, _ := set.Len("src")
		be.Equal(t, slen, 0)
		sone, _ := set.Exists("src", "one")
		be.Equal(t, sone, false)

		dkey, _ := db.Key().Get("dest")
		be.Equal(t, dkey.Version, 2)
		dlen, _ := set.Len("dest")
		be.Equal(t, dlen, 3)
		done, _ := set.Exists("dest", "one")
		be.Equal(t, done, true)
	})
	t.Run("dest not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("src", "one", "two")

		err := set.Move("src", "dest", "one")
		be.Err(t, err, nil)

		skey, _ := db.Key().Get("src")
		be.Equal(t, skey.Version, 2)
		slen, _ := set.Len("src")
		be.Equal(t, slen, 1)
		sone, _ := set.Exists("src", "one")
		be.Equal(t, sone, false)

		dkey, _ := db.Key().Get("dest")
		be.Equal(t, dkey.Version, 1)
		dlen, _ := set.Len("dest")
		be.Equal(t, dlen, 1)
		done, _ := set.Exists("dest", "one")
		be.Equal(t, done, true)
	})
	t.Run("src elem not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("src", "two")
		_, _ = set.Add("dest", "thr", "fou")

		err := set.Move("src", "dest", "one")
		be.Err(t, err, core.ErrNotFound)

		dkey, _ := db.Key().Get("dest")
		be.Equal(t, dkey.Version, 1)
		dlen, _ := set.Len("dest")
		be.Equal(t, dlen, 2)
	})
	t.Run("src key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("dest", "thr", "fou")

		err := set.Move("src", "dest", "one")
		be.Err(t, err, core.ErrNotFound)

		dkey, _ := db.Key().Get("dest")
		be.Equal(t, dkey.Version, 1)
		dlen, _ := set.Len("dest")
		be.Equal(t, dlen, 2)
	})
	t.Run("dest type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("src", "one", "two")
		_ = db.Str().Set("dest", "str")

		err := set.Move("src", "dest", "one")
		be.Err(t, err, core.ErrKeyType)

		skey, _ := db.Key().Get("src")
		be.Equal(t, skey.Version, 1)
		slen, _ := set.Len("src")
		be.Equal(t, slen, 2)
	})
	t.Run("src type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_ = db.Str().Set("src", "one")
		_, _ = set.Add("dest", "thr", "fou")

		err := set.Move("src", "dest", "one")
		be.Err(t, err, core.ErrNotFound)

		dkey, _ := db.Key().Get("dest")
		be.Equal(t, dkey.Version, 1)
		dlen, _ := set.Len("dest")
		be.Equal(t, dlen, 2)
	})
}

func TestPop(t *testing.T) {
	t.Run("pop", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two", "thr")

		elem, err := set.Pop("key")
		be.Err(t, err, nil)
		s := elem.String()
		be.Equal(t, s == "one" || s == "two" || s == "thr", true)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		slen, _ := set.Len("key")
		be.Equal(t, slen, 2)
	})
	t.Run("single elem", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one")

		elem, err := set.Pop("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		slen, _ := set.Len("key")
		be.Equal(t, slen, 0)
	})
	t.Run("key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		elem, err := set.Pop("key")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, elem.IsZero(), true)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		elem, err := set.Pop("key")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, elem.IsZero(), true)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "str")
	})
}

func TestRandom(t *testing.T) {
	t.Run("random", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two", "thr")

		elem, err := set.Random("key")
		be.Err(t, err, nil)
		s := elem.String()
		be.Equal(t, s == "one" || s == "two" || s == "thr", true)
	})
	t.Run("single elem", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one")

		elem, err := set.Random("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")
	})
	t.Run("key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		elem, err := set.Random("key")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, elem.IsZero(), true)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		elem, err := set.Random("key")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, elem.IsZero(), true)
	})
}

func TestScan(t *testing.T) {
	db, set := getDB(t)
	defer db.Close()

	_, _ = set.Add("key", "f11", "f12", "f21", "f22", "f31")
	_ = db.Str().Set("str", "str")

	tests := []struct {
		name    string
		cursor  int
		pattern string
		count   int

		wantCursor int
		wantItems  []core.Value
	}{
		{"all", 0, "*", 0, 5,
			[]core.Value{
				core.Value("f11"), core.Value("f12"),
				core.Value("f21"), core.Value("f22"),
				core.Value("f31"),
			},
		},
		{"some", 0, "f2*", 10, 4,
			[]core.Value{
				core.Value("f21"), core.Value("f22"),
			},
		},
		{"none", 0, "n*", 10, 0, []core.Value(nil)},
		{"cursor 1st", 0, "*", 2, 2,
			[]core.Value{
				core.Value("f11"), core.Value("f12"),
			},
		},
		{"cursor 2nd", 2, "*", 2, 4,
			[]core.Value{
				core.Value("f21"), core.Value("f22"),
			},
		},
		{"cursor 3rd", 4, "*", 2, 5,
			[]core.Value{
				core.Value("f31"),
			},
		},
		{"exhausted", 6, "*", 2, 0, []core.Value(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := set.Scan("key", test.cursor, test.pattern, test.count)
			be.Err(t, err, nil)
			be.Equal(t, out.Cursor, test.wantCursor)
			for i, item := range out.Items {
				be.Equal(t, item, test.wantItems[i])
			}
		})
	}

	t.Run("ignore other keys", func(t *testing.T) {
		_, _ = set.Add("key1", "elem")
		_, _ = set.Add("key2", "elem")

		out, err := set.Scan("key1", 0, "*", 0)
		be.Err(t, err, nil)
		be.Equal(t, len(out.Items), 1)
		be.Equal(t, out.Items[0].String(), "elem")
	})
	t.Run("key not found", func(t *testing.T) {
		out, err := set.Scan("not", 0, "*", 0)
		be.Err(t, err, nil)
		be.Equal(t, len(out.Items), 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		out, err := set.Scan("str", 0, "*", 0)
		be.Err(t, err, nil)
		be.Equal(t, len(out.Items), 0)
	})
}

func TestScanner(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key", "f11", "f12", "f21", "f22", "f31")

		var items []core.Value
		err := db.View(func(tx *redka.Tx) error {
			sc := set.Scanner("key", "*", 2)
			for sc.Scan() {
				items = append(items, sc.Item())
			}
			return sc.Err()
		})

		be.Err(t, err, nil)
		strs := make([]string, len(items))
		for i, it := range items {
			strs[i] = it.String()
		}
		be.Equal(t, strs, []string{"f11", "f12", "f21", "f22", "f31"})
	})
	t.Run("key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		sc := set.Scanner("not", "*", 2)
		var items []core.Value
		for sc.Scan() {
			items = append(items, sc.Item())
		}

		be.Err(t, sc.Err(), nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_ = db.Str().Set("key", "str")

		sc := set.Scanner("key", "*", 2)
		var items []core.Value
		for sc.Scan() {
			items = append(items, sc.Item())
		}

		be.Err(t, sc.Err(), nil)
		be.Equal(t, items, []core.Value(nil))
	})
}

func TestUnion(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two")
		_, _ = set.Add("key2", "two", "thr")
		_, _ = set.Add("key3", "thr", "fou")

		items, err := set.Union("key1", "key2", "key3")
		be.Err(t, err, nil)
		sort.Slice(items, func(i, j int) bool {
			return slices.Compare(items[i], items[j]) < 0
		})
		be.Equal(t, items, []core.Value{
			core.Value("fou"), core.Value("one"),
			core.Value("thr"), core.Value("two"),
		})
	})
	t.Run("no keys", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		items, err := set.Union()
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("single key", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr")

		items, err := set.Union("key1")
		be.Err(t, err, nil)
		sort.Slice(items, func(i, j int) bool {
			return slices.Compare(items[i], items[j]) < 0
		})
		be.Equal(t, items, []core.Value{
			core.Value("one"), core.Value("thr"), core.Value("two"),
		})
	})
	t.Run("key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "two")

		items, err := set.Union("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value{
			core.Value("one"), core.Value("two"),
		})
	})
	t.Run("all not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		items, err := set.Union("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one")
		_ = db.Str().Set("key2", "two")
		_, _ = set.Add("key3", "thr")

		items, err := set.Union("key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, items, []core.Value{
			core.Value("one"), core.Value("thr"),
		})
	})
}

func TestUnionStore(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key1", "one", "two", "thr")
		_, _ = set.Add("key2", "two", "thr", "fou")
		_, _ = set.Add("key3", "one", "two", "thr", "fou")

		n, err := set.UnionStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 4)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 4)

		for _, elem := range []string{"one", "two", "thr", "fou"} {
			exists, _ := set.Exists("dest", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("rewrite dest", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")
		_, _ = set.Add("dest", "old")

		n, err := set.UnionStore("dest", "key1", "key2")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, true)
		old, _ := set.Exists("dest", "old")
		be.Equal(t, old, false)
	})
	t.Run("no keys", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("dest", "old")

		n, err := set.UnionStore("dest")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 1)

		one, _ := set.Exists("dest", "old")
		be.Equal(t, one, true)
	})
	t.Run("single key", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()
		_, _ = set.Add("key", "one", "two")
		_, _ = set.Add("dest", "old")

		n, err := set.UnionStore("dest", "key")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 2)

		for _, elem := range []string{"one", "two"} {
			exists, _ := set.Exists("dest", elem)
			be.Equal(t, exists, true)
		}
	})
	t.Run("empty", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("dest", "old")

		n, err := set.UnionStore("dest", "key1", "key2")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("dest")
		be.Equal(t, key.Version, 1)

		slen, _ := set.Len("dest")
		be.Equal(t, slen, 0)

		old, _ := set.Exists("dest", "old")
		be.Equal(t, old, false)
	})
	t.Run("source key not found", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")

		n, err := set.UnionStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, true)
	})
	t.Run("source key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")
		_ = db.Str().Set("key3", "one")

		n, err := set.UnionStore("dest", "key1", "key2", "key3")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, true)
	})
	t.Run("dest key type mismatch", func(t *testing.T) {
		db, set := getDB(t)
		defer db.Close()

		_, _ = set.Add("key1", "one")
		_, _ = set.Add("key2", "one")
		_ = db.Str().Set("dest", "old")

		n, err := set.UnionStore("dest", "key1", "key2")
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, n, 0)

		one, _ := set.Exists("dest", "one")
		be.Equal(t, one, false)

		sval, _ := db.Str().Get("dest")
		be.Equal(t, sval.String(), "old")
	})
}

func getDB(tb testing.TB) (*redka.DB, *rset.DB) {
	tb.Helper()
	db := testx.OpenDB(tb)
	return db, db.Set()
}
