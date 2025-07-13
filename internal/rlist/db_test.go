package rlist_test

import (
	"testing"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rlist"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDelete(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		n, err := list.Delete("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
	t.Run("delete elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.Delete("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("delete multiple", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "fou")

		n, err := list.Delete("key", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 9)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "one")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "thr")
		el2, _ := list.Get("key", 2)
		be.Equal(t, el2.String(), "fou")
	})
	t.Run("delete duplicate", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")
		_, _ = list.PushBack("key", "elem")

		n, err := list.Delete("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 4)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("elem not found", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.Delete("key", "none")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.Delete("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "value")
	})
}

func TestDeleteBack(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		n, err := list.DeleteBack("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
	t.Run("delete elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.DeleteBack("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("delete multiple", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "fou")

		n, err := list.DeleteBack("key", "two", 2)
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 8)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 4)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "one")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "two")
		el2, _ := list.Get("key", 2)
		be.Equal(t, el2.String(), "thr")
		el3, _ := list.Get("key", 3)
		be.Equal(t, el3.String(), "fou")
	})
	t.Run("large count", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "fou")

		n, err := list.DeleteBack("key", "two", 10)
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 7)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)
	})
	t.Run("delete duplicate", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")
		_, _ = list.PushBack("key", "elem")

		n, err := list.DeleteBack("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 3)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("elem not found", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.DeleteBack("key", "none", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.DeleteBack("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "value")
	})
}

func TestDeleteFront(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		n, err := list.DeleteFront("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
	t.Run("delete elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.DeleteFront("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("delete multiple", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "fou")

		n, err := list.DeleteFront("key", "two", 2)
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 8)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 4)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "one")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "thr")
		el2, _ := list.Get("key", 2)
		be.Equal(t, el2.String(), "two")
		el3, _ := list.Get("key", 3)
		be.Equal(t, el3.String(), "fou")
	})
	t.Run("large count", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "fou")

		n, err := list.DeleteFront("key", "two", 10)
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 7)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)
	})
	t.Run("delete duplicate", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")
		_, _ = list.PushBack("key", "elem")

		n, err := list.DeleteFront("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 3)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("elem not found", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.DeleteFront("key", "none", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.DeleteFront("key", "elem", 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "value")
	})
}

func TestGet(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		_, err := list.Get("key", 0)
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("single elem", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		elem, err := list.Get("key", 0)
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "elem")
	})
	t.Run("multiple elems", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elem, err := list.Get("key", 0)
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")

		elem, err = list.Get("key", 1)
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "two")

		elem, err = list.Get("key", 2)
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "thr")
	})
	t.Run("index out of bounds", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		_, err := list.Get("key", 1)
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("negative index", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elem, err := list.Get("key", -2)
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "two")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		_, err := list.Get("key", 0)
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestInsertAfter(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		n, err := list.InsertAfter("key", "mark", "elem")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, n, 0)
	})
	t.Run("insert after first", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "mark")

		n, err := list.InsertAfter("key", "mark", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)

		elem, _ := list.Get("key", 1)
		be.Equal(t, elem.String(), "elem")
	})
	t.Run("insert after middle", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "thr")

		n, err := list.InsertAfter("key", "one", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 3)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)

		elem, _ := list.Get("key", 1)
		be.Equal(t, elem.String(), "two")
	})
	t.Run("elem not found", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")

		n, err := list.InsertAfter("key", "thr", "two")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, n, -1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.InsertAfter("key", "mark", "elem")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, n, 0)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "value")
	})
}

func TestInsertBefore(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		n, err := list.InsertBefore("key", "mark", "elem")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, n, 0)
	})
	t.Run("insert before first", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "mark")

		n, err := list.InsertBefore("key", "mark", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)

		elem, _ := list.Get("key", 0)
		be.Equal(t, elem.String(), "elem")
	})
	t.Run("insert before middle", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "thr")

		n, err := list.InsertBefore("key", "thr", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 3)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)

		elem, _ := list.Get("key", 1)
		be.Equal(t, elem.String(), "two")
	})
	t.Run("elem not found", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")

		n, err := list.InsertBefore("key", "thr", "two")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, n, -1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.InsertBefore("key", "mark", "elem")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, n, 0)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "value")
	})
}

func TestLen(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		n, err := list.Len("key")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
	t.Run("single elem", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.Len("key")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)
	})
	t.Run("multiple elems", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Len("key")
		be.Err(t, err, nil)
		be.Equal(t, n, 4)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.Len("key")
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
}

func TestPushBack(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, list := getDB(t)

		n, err := list.PushBack("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("add elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")

		n, err := list.PushBack("key", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)
	})
	t.Run("add multiple", func(t *testing.T) {
		db, list := getDB(t)

		n, err := list.PushBack("key", "one")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)
		n, err = list.PushBack("key", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)
		n, err = list.PushBack("key", "thr")
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 3)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)
	})
	t.Run("add duplicate", func(t *testing.T) {
		db, list := getDB(t)

		n, err := list.PushBack("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)
		n, err = list.PushBack("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.PushBack("key", 42)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, n, 0)

		_, err = list.Get("key", 0)
		be.Err(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "value")
	})
}

func TestPushFront(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, list := getDB(t)

		n, err := list.PushFront("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("add elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushFront("key", "one")

		n, err := list.PushFront("key", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)
	})
	t.Run("add multiple", func(t *testing.T) {
		db, list := getDB(t)

		n, err := list.PushFront("key", "one")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)
		n, err = list.PushFront("key", "two")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)
		n, err = list.PushFront("key", "thr")
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 3)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)
	})
	t.Run("add duplicate", func(t *testing.T) {
		db, list := getDB(t)

		n, err := list.PushFront("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 1)
		n, err = list.PushFront("key", "elem")
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.PushFront("key", 42)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, n, 0)

		_, err = list.Get("key", 0)
		be.Err(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("key")
		be.Equal(t, sval.String(), "value")
	})
}

func TestPopBack(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		_, err := list.PopBack("key")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("pop elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")

		elem, err := list.PopBack("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("pop multiple", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elem, err := list.PopBack("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "thr")

		elem, err = list.PopBack("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "two")

		elem, err = list.PopBack("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 6)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("pop duplicate", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "elem")
		_, _ = list.PushBack("key", "elem")

		elem, err := list.PopBack("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "elem")

		elem, err = list.PopBack("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "elem")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 5)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		_, err := list.PopBack("key")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestPopBackPushFront(t *testing.T) {
	t.Run("src not found", func(t *testing.T) {
		_, list := getDB(t)

		_, err := list.PopBackPushFront("src", "dest")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("pop elem", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("src", "elem")

		elem, err := list.PopBackPushFront("src", "dest")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "elem")

		srclen, _ := list.Len("src")
		be.Equal(t, srclen, 0)

		dstlen, _ := list.Len("dest")
		be.Equal(t, dstlen, 1)
	})
	t.Run("pop multiple", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("src", "one")
		_, _ = list.PushBack("src", "two")
		_, _ = list.PushBack("src", "thr")

		elem, err := list.PopBackPushFront("src", "dest")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "thr")
		elem, _ = list.Get("dest", 0)
		be.Equal(t, elem.String(), "thr")

		elem, err = list.PopBackPushFront("src", "dest")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "two")
		elem, _ = list.Get("dest", 0)
		be.Equal(t, elem.String(), "two")

		elem, err = list.PopBackPushFront("src", "dest")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")
		elem, _ = list.Get("dest", 0)
		be.Equal(t, elem.String(), "one")

		srclen, _ := list.Len("src")
		be.Equal(t, srclen, 0)
		dstlen, _ := list.Len("dest")
		be.Equal(t, dstlen, 3)
	})
	t.Run("push to self", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elem, err := list.PopBackPushFront("key", "key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "thr")

		elem, err = list.PopBackPushFront("key", "key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "two")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 7)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)

		elems, _ := list.Range("key", 0, 2)
		be.Equal(t, elems[0].String(), "two")
		be.Equal(t, elems[1].String(), "thr")
		be.Equal(t, elems[2].String(), "one")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("src", "value")

		_, err := list.PopBackPushFront("src", "dest")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestPopFront(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		_, err := list.PopFront("key")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("pop elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")

		elem, err := list.PopFront("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("pop multiple", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elem, err := list.PopFront("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "one")

		elem, err = list.PopFront("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "two")

		elem, err = list.PopFront("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "thr")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 6)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("pop duplicate", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")
		_, _ = list.PushBack("key", "elem")
		_, _ = list.PushBack("key", "one")

		elem, err := list.PopFront("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "elem")

		elem, err = list.PopFront("key")
		be.Err(t, err, nil)
		be.Equal(t, elem.String(), "elem")

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 5)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		_, err := list.PopFront("key")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestRange(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		elems, err := list.Range("key", 0, 0)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 0)
	})
	t.Run("single elem", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		elems, err := list.Range("key", 0, 0)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 1)
		be.Equal(t, elems[0].String(), "elem")
	})
	t.Run("multiple elems", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elems, err := list.Range("key", 0, 1)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 2)
		be.Equal(t, elems[0].String(), "one")
		be.Equal(t, elems[1].String(), "two")
	})
	t.Run("start >= len", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")

		elems, err := list.Range("key", 2, 2)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 0)
	})
	t.Run("start > stop > 0", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")

		elems, err := list.Range("key", 1, 0)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 0)
	})
	t.Run("0 > start > stop", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elems, err := list.Range("key", -1, -2)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 0)
	})
	t.Run("stop > len", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elems, err := list.Range("key", 1, 5)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 2)
		be.Equal(t, elems[0].String(), "two")
		be.Equal(t, elems[1].String(), "thr")
	})
	t.Run("start < 0", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elems, err := list.Range("key", -2, 2)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 2)
		be.Equal(t, elems[0].String(), "two")
		be.Equal(t, elems[1].String(), "thr")
	})
	t.Run("stop < 0", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elems, err := list.Range("key", 1, -1)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 2)
		be.Equal(t, elems[0].String(), "two")
		be.Equal(t, elems[1].String(), "thr")
	})
	t.Run("start < 0 and stop < 0", func(t *testing.T) {
		_, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		elems, err := list.Range("key", -2, -1)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 2)
		be.Equal(t, elems[0].String(), "two")
		be.Equal(t, elems[1].String(), "thr")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		elems, err := list.Range("key", 0, 0)
		be.Err(t, err, nil)
		be.Equal(t, len(elems), 0)
	})
}

func TestSet(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		err := list.Set("key", 0, "elem")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("single elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")

		err := list.Set("key", 0, "two")
		be.Err(t, err, nil)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 2)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)

		elem, _ := list.Get("key", 0)
		be.Equal(t, elem.String(), "two")
	})
	t.Run("multiple elems", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		err := list.Set("key", 1, "new")
		be.Err(t, err, nil)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 5)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 4)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "one")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "new")
		el2, _ := list.Get("key", 2)
		be.Equal(t, el2.String(), "two")
		el3, _ := list.Get("key", 3)
		be.Equal(t, el3.String(), "thr")
	})
	t.Run("index out of bounds", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		err := list.Set("key", 1, "new")
		be.Err(t, err, core.ErrNotFound)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)

		elem, _ := list.Get("key", 0)
		be.Equal(t, elem.String(), "elem")
	})
	t.Run("negative index", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		err := list.Set("key", -2, "new")
		be.Err(t, err, nil)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 4)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 3)

		elem, _ := list.Get("key", -2)
		be.Equal(t, elem.String(), "new")
		elem, _ = list.Get("key", 1)
		be.Equal(t, elem.String(), "new")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		err := list.Set("key", 0, "elem")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestTrim(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		_, list := getDB(t)

		n, err := list.Trim("key", 0, 0)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
	t.Run("keep single elem", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "elem")

		n, err := list.Trim("key", 0, 0)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 1)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 1)
	})
	t.Run("keep multiple elems", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")
		_, _ = list.PushBack("key", "fou")

		n, err := list.Trim("key", 1, 2)
		be.Err(t, err, nil)
		be.Equal(t, n, 2)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 6)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "two")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "thr")
	})
	t.Run("keep all elems", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")
		_, _ = list.PushBack("key", "fou")

		n, err := list.Trim("key", 0, 3)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 4)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 4)
	})
	t.Run("start >= len", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Trim("key", 3, 3)
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 6)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("start > stop > 0", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Trim("key", 2, 1)
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 6)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("0 > start > stop", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Trim("key", -1, -2)
		be.Err(t, err, nil)
		be.Equal(t, n, 3)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 6)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 0)
	})
	t.Run("stop > len", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Trim("key", 1, 5)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 4)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "two")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "thr")
	})
	t.Run("start < 0", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Trim("key", -2, 2)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 4)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "two")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "thr")
	})
	t.Run("stop < 0", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Trim("key", 1, -1)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 4)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "two")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "thr")
	})
	t.Run("start < 0 and stop < 0", func(t *testing.T) {
		db, list := getDB(t)
		_, _ = list.PushBack("key", "one")
		_, _ = list.PushBack("key", "two")
		_, _ = list.PushBack("key", "thr")

		n, err := list.Trim("key", -2, -1)
		be.Err(t, err, nil)
		be.Equal(t, n, 1)

		key, _ := db.Key().Get("key")
		be.Equal(t, key.Version, 4)

		llen, _ := list.Len("key")
		be.Equal(t, llen, 2)

		el0, _ := list.Get("key", 0)
		be.Equal(t, el0.String(), "two")
		el1, _ := list.Get("key", 1)
		be.Equal(t, el1.String(), "thr")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, list := getDB(t)
		_ = db.Str().Set("key", "value")

		n, err := list.Trim("key", 0, 0)
		be.Err(t, err, nil)
		be.Equal(t, n, 0)
	})
}

func getDB(tb testing.TB) (*redka.DB, *rlist.DB) {
	tb.Helper()
	db := testx.OpenDB(tb)
	return db, db.List()
}
