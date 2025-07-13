package rkey_test

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/testx"
)

func TestCount(t *testing.T) {
	db, kkey := getDB(t)

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	tests := []struct {
		name string
		keys []string
		want int
	}{
		{"all found", []string{"name", "age"}, 2},
		{"some found", []string{"name", "key1"}, 1},
		{"none found", []string{"key1", "key2"}, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count, err := kkey.Count(test.keys...)
			be.Err(t, err, nil)
			be.Equal(t, count, test.want)
		})
	}
}

func TestDelete(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		count, err := kkey.Delete("name", "age")
		be.Err(t, err, nil)
		be.Equal(t, count, 2)

		exists, _ := kkey.Exists("name")
		be.Equal(t, exists, false)

		exists, _ = kkey.Exists("age")
		be.Equal(t, exists, false)
	})
	t.Run("some", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		count, err := kkey.Delete("name")
		be.Err(t, err, nil)
		be.Equal(t, count, 1)

		exists, _ := kkey.Exists("name")
		be.Equal(t, exists, false)

		exists, _ = kkey.Exists("age")
		be.Equal(t, exists, true)
	})
	t.Run("none", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		count, err := kkey.Delete("key")
		be.Err(t, err, nil)
		be.Equal(t, count, 0)

		exists, _ := kkey.Exists("name")
		be.Equal(t, exists, true)

		exists, _ = kkey.Exists("age")
		be.Equal(t, exists, true)
	})
}

func TestDeleteAll(t *testing.T) {
	db, kkey := getDB(t)

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	err := kkey.DeleteAll()
	be.Err(t, err, nil)

	count, _ := kkey.Count("name", "age")
	be.Equal(t, count, 0)
}

func TestDeleteExpired(t *testing.T) {
	t.Run("delete all", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().SetExpire("name", "alice", 1*time.Millisecond)
		_ = db.Str().SetExpire("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := kkey.DeleteExpired(0)
		be.Err(t, err, nil)
		be.Equal(t, count, 2)

		count, _ = kkey.Count("name", "age")
		be.Equal(t, count, 0)
	})
	t.Run("delete n", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().SetExpire("name", "alice", 1*time.Millisecond)
		_ = db.Str().SetExpire("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := kkey.DeleteExpired(1)
		be.Err(t, err, nil)
		be.Equal(t, count, 1)
	})
}

func TestExists(t *testing.T) {
	db, kkey := getDB(t)

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"found", "name", true},
		{"not found", "city", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count, err := kkey.Exists(test.key)
			be.Err(t, err, nil)
			be.Equal(t, count, test.want)
		})
	}
}

func TestExpire(t *testing.T) {
	t.Run("expire", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		now := time.Now()
		ttl := 10 * time.Second
		err := kkey.Expire("name", ttl)
		be.Err(t, err, nil)

		key, _ := kkey.Get("name")
		be.Equal(t, key.Version, 2)
		if key.ETime == nil {
			t.Error("want expired time, got nil")
		}
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		if got != want {
			t.Errorf("want %v, got %v", want, got)
		}
	})
	t.Run("not found", func(t *testing.T) {
		_, kkey := getDB(t)

		err := kkey.Expire("name", 10*time.Second)
		be.Equal(t, err, core.ErrNotFound)
	})
	t.Run("expire then set", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")

		err := kkey.Expire("name", 0)
		be.Err(t, err, nil)

		time.Sleep(1 * time.Millisecond)
		_, err = kkey.Get("name")
		be.Equal(t, err, core.ErrNotFound)

		err = db.Str().Set("name", "bob")
		be.Err(t, err, nil)

		key, _ := kkey.Get("name")
		be.Equal(t, key.Version, 3)
		be.Equal(t, key.ETime, (*int64)(nil))
	})
}

func TestExpireAt(t *testing.T) {
	t.Run("expire", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		now := time.Now()
		at := now.Add(10 * time.Second)
		err := kkey.ExpireAt("name", at)
		be.Err(t, err, nil)

		key, _ := kkey.Get("name")
		be.Equal(t, key.Version, 2)
		if key.ETime == nil {
			t.Error("want expired time, got nil")
		}
		got := (*key.ETime) / 1000
		want := at.UnixMilli() / 1000
		if got != want {
			t.Errorf("want %v, got %v", want, got)
		}
	})
	t.Run("not found", func(t *testing.T) {
		_, kkey := getDB(t)

		err := kkey.ExpireAt("name", time.Now().Add(10*time.Second))
		be.Equal(t, err, core.ErrNotFound)
	})
}

func TestGet(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		db, kkey := getDB(t)

		now := time.Now().UnixMilli()
		_ = db.Str().Set("name", "alice")

		key, err := kkey.Get("name")
		be.Err(t, err, nil)
		be.True(t, key.ID > 0)
		be.Equal(t, key.Key, "name")
		be.Equal(t, key.Type, core.TypeString)
		be.Equal(t, key.Version, 1)
		be.Equal(t, key.ETime, (*int64)(nil))
		be.Equal(t, key.MTime >= now, true)
	})
	t.Run("not found", func(t *testing.T) {
		_, kkey := getDB(t)

		key, err := kkey.Get("name")
		be.Equal(t, err, core.ErrNotFound)
		be.Equal(t, key, core.Key{})
	})
}

func TestKeys(t *testing.T) {
	db, kkey := getDB(t)

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	tests := []struct {
		name      string
		pattern   string
		want      []string
		wantCount int
	}{
		{"all found", "*", []string{"name", "age"}, 2},
		{"some found", "na*", []string{"name"}, 1},
		{"none found", "key*", []string(nil), 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keys, err := kkey.Keys(test.pattern)
			be.Err(t, err, nil)
			be.Equal(t, len(keys), test.wantCount)
			for i, key := range keys {
				name := key.Key
				be.Equal(t, name, test.want[i])
			}
		})
	}
}

func TestLen(t *testing.T) {
	t.Run("len", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		count, err := kkey.Len()
		be.Err(t, err, nil)
		be.Equal(t, count, 2)
	})
	t.Run("empty", func(t *testing.T) {
		_, kkey := getDB(t)

		count, err := kkey.Len()
		be.Err(t, err, nil)
		be.Equal(t, count, 0)
	})
}

func TestPersist(t *testing.T) {
	t.Run("persist", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		err := kkey.Expire("name", 10*time.Second)
		be.Err(t, err, nil)

		err = kkey.Persist("name")
		be.Err(t, err, nil)

		key, _ := kkey.Get("name")
		be.Equal(t, key.Version, 3)
		if key.ETime != nil {
			t.Error("want nil, got expired time")
		}
	})
	t.Run("not found", func(t *testing.T) {
		_, kkey := getDB(t)

		err := kkey.Persist("name")
		be.Equal(t, err, core.ErrNotFound)
	})
}

func TestRandom(t *testing.T) {
	t.Run("random", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		key, err := kkey.Random()
		be.Err(t, err, nil)
		if key.Key != "name" && key.Key != "age" {
			t.Errorf("want name or age, got %s", key.Key)
		}
	})
	t.Run("empty", func(t *testing.T) {
		_, kkey := getDB(t)

		key, err := kkey.Random()
		be.Equal(t, err, core.ErrNotFound)
		be.Equal(t, key, core.Key{})
	})
}

func TestRename(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		err := kkey.Rename("name", "key")
		be.Err(t, err, nil)

		exists, _ := kkey.Exists("name")
		be.Equal(t, exists, false)

		key, _ := kkey.Get("key")
		be.Equal(t, key.Version, 2)

		val, _ := db.Str().Get("key")
		be.Equal(t, val.String(), "alice")
	})
	t.Run("rename", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		err := kkey.Rename("name", "age")
		be.Err(t, err, nil)

		exists, _ := kkey.Exists("name")
		be.Equal(t, exists, false)

		key, _ := kkey.Get("age")
		be.Equal(t, key.Version, 2)

		val, err := db.Str().Get("age")
		be.Err(t, err, nil)
		be.Equal(t, val.String(), "alice")
	})
	t.Run("same", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)

		err := kkey.Rename("name", "name")
		be.Err(t, err, nil)

		key, _ := kkey.Get("name")
		be.Equal(t, key.Version, 1)

		val, err := db.Str().Get("name")
		be.Err(t, err, nil)
		be.Equal(t, val.String(), "alice")
	})
	t.Run("not found", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		err := kkey.Rename("key1", "name")
		be.Equal(t, err, core.ErrNotFound)

		key, _ := kkey.Get("name")
		be.Equal(t, key.Version, 1)

		exists, _ := kkey.Exists("name")
		be.Equal(t, exists, true)

		exists, _ = kkey.Exists("key1")
		be.Equal(t, exists, false)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("str", "str")
		_, _ = db.Hash().Set("hash", "field", "value")

		err := kkey.Rename("str", "hash")
		be.Err(t, err, core.ErrKeyType)

		exists, _ := kkey.Exists("str")
		be.Equal(t, exists, true)

		exists, _ = kkey.Exists("hash")
		be.Equal(t, exists, true)
	})
}

func TestRenameNotExists(t *testing.T) {
	t.Run("rename", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		ok, err := kkey.RenameNotExists("name", "title")
		be.Err(t, err, nil)
		be.Equal(t, ok, true)

		key, _ := kkey.Get("title")
		be.Equal(t, key.Version, 2)

		title, _ := db.Str().Get("title")
		be.Equal(t, title.String(), "alice")
	})
	t.Run("same name", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		ok, err := kkey.RenameNotExists("name", "name")
		be.Err(t, err, nil)
		be.Equal(t, ok, false)

		key, _ := kkey.Get("name")
		be.Equal(t, key.Version, 1)

		name, _ := db.Str().Get("name")
		be.Equal(t, name.String(), "alice")
	})
	t.Run("old does not exist", func(t *testing.T) {
		_, kkey := getDB(t)

		ok, err := kkey.RenameNotExists("key1", "key2")
		be.Equal(t, err, core.ErrNotFound)
		be.Equal(t, ok, false)
	})
	t.Run("new exists", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)
		ok, err := kkey.RenameNotExists("name", "age")
		be.Err(t, err, nil)
		be.Equal(t, ok, false)

		key, _ := kkey.Get("age")
		be.Equal(t, key.Version, 1)

		age, _ := db.Str().Get("age")
		be.Equal(t, age, core.Value("25"))
	})
}

func TestScan(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("11", "11")
		_ = db.Str().Set("12", "12")
		_ = db.Str().Set("21", "21")
		_ = db.Str().Set("22", "22")
		_ = db.Str().Set("31", "31")

		tests := []struct {
			name     string
			pattern  string
			count    int
			wantKeys []string
		}{
			{"all", "*", 10, []string{"11", "12", "21", "22", "31"}},
			{"some", "2*", 10, []string{"21", "22"}},
			{"none", "n*", 10, []string{}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				out, err := kkey.Scan(0, test.pattern, core.TypeAny, test.count)
				be.Err(t, err, nil)
				keyNames := make([]string, len(out.Keys))
				for i, key := range out.Keys {
					keyNames[i] = key.Key
				}
				be.Equal(t, keyNames, test.wantKeys)
			})
		}
	})
	t.Run("pagination", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("11", "11")
		_ = db.Str().Set("12", "12")
		_ = db.Str().Set("21", "21")
		_ = db.Str().Set("22", "22")
		_ = db.Str().Set("31", "31")

		out, err := kkey.Scan(0, "*", core.TypeAny, 2)
		be.Err(t, err, nil)
		be.Equal(t, len(out.Keys), 2)
		be.Equal(t, out.Keys[0].Key, "11")
		be.Equal(t, out.Keys[1].Key, "12")

		out, err = kkey.Scan(out.Cursor, "*", core.TypeAny, 2)
		be.Err(t, err, nil)
		be.Equal(t, len(out.Keys), 2)
		be.Equal(t, out.Keys[0].Key, "21")
		be.Equal(t, out.Keys[1].Key, "22")

		out, err = kkey.Scan(out.Cursor, "*", core.TypeAny, 2)
		be.Err(t, err, nil)
		be.Equal(t, len(out.Keys), 1)
		be.Equal(t, out.Keys[0].Key, "31")
	})
	t.Run("type filter", func(t *testing.T) {
		db, kkey := getDB(t)

		_ = db.Str().Set("k11", "11")
		_ = db.Str().Set("k12", "12")
		_, _ = db.Hash().Set("k21", "field", "value")
		_, _ = db.ZSet().Add("k22", "elem", 11)
		_ = db.Str().Set("k31", "31")

		out, err := kkey.Scan(0, "*", core.TypeString, 10)
		be.Err(t, err, nil)
		be.Equal(t, len(out.Keys), 3)
		be.Equal(t, out.Keys[0].Key, "k11")
		be.Equal(t, out.Keys[1].Key, "k12")
		be.Equal(t, out.Keys[2].Key, "k31")
	})
}

func TestScanner(t *testing.T) {
	db, _ := getDB(t)

	_ = db.Str().Set("11", "11")
	_ = db.Str().Set("12", "12")
	_ = db.Str().Set("21", "21")
	_ = db.Str().Set("22", "22")
	_ = db.Str().Set("31", "31")

	var keys []core.Key
	err := db.View(func(tx *redka.Tx) error {
		sc := tx.Key().Scanner("*", core.TypeAny, 2)
		for sc.Scan() {
			keys = append(keys, sc.Key())
		}
		return sc.Err()
	})
	be.Err(t, err, nil)
	keyNames := make([]string, len(keys))
	for i, key := range keys {
		keyNames[i] = key.Key
	}
	be.Equal(t, keyNames, []string{"11", "12", "21", "22", "31"})
}

func getDB(tb testing.TB) (*redka.DB, *rkey.DB) {
	tb.Helper()
	db := testx.OpenDB(tb)
	return db, db.Key()
}
