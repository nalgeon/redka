package rkey_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/testx"
)

func TestCount(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

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
			count, err := db.Count(test.keys...)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, test.want)
		})
	}
}

func TestDelete(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		count, err := db.Delete("name", "age")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		exists, _ := db.Exists("name")
		testx.AssertEqual(t, exists, false)

		exists, _ = db.Exists("age")
		testx.AssertEqual(t, exists, false)
	})
	t.Run("some", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		count, err := db.Delete("name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)

		exists, _ := db.Exists("name")
		testx.AssertEqual(t, exists, false)

		exists, _ = db.Exists("age")
		testx.AssertEqual(t, exists, true)
	})
	t.Run("none", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		count, err := db.Delete("key")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 0)

		exists, _ := db.Exists("name")
		testx.AssertEqual(t, exists, true)

		exists, _ = db.Exists("age")
		testx.AssertEqual(t, exists, true)
	})
}

func TestDeleteAll(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	err := db.DeleteAll()
	testx.AssertNoErr(t, err)

	count, _ := db.Count("name", "age")
	testx.AssertEqual(t, count, 0)
}

func TestDeleteExpired(t *testing.T) {
	t.Run("delete all", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().SetExpires("name", "alice", 1*time.Millisecond)
		_ = red.Str().SetExpires("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := db.DeleteExpired(0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		count, _ = db.Count("name", "age")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("delete n", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().SetExpires("name", "alice", 1*time.Millisecond)
		_ = red.Str().SetExpires("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := db.DeleteExpired(1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)
	})
}

func TestExists(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

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
			count, err := db.Exists(test.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, test.want)
		})
	}
}

func TestExpire(t *testing.T) {
	t.Run("expire", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		now := time.Now()
		ttl := 10 * time.Second
		err := db.Expire("name", ttl)
		testx.AssertNoErr(t, err)

		key, _ := db.Get("name")
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
		red, db := getDB(t)
		defer red.Close()

		err := db.Expire("name", 10*time.Second)
		testx.AssertEqual(t, err, core.ErrNotFound)
	})
}

func TestExpireAt(t *testing.T) {
	t.Run("expire", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		now := time.Now()
		at := now.Add(10 * time.Second)
		err := db.ExpireAt("name", at)
		testx.AssertNoErr(t, err)

		key, _ := db.Get("name")
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
		red, db := getDB(t)
		defer red.Close()

		err := db.ExpireAt("name", time.Now().Add(10*time.Second))
		testx.AssertEqual(t, err, core.ErrNotFound)
	})
}

func TestGet(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		now := time.Now().UnixMilli()
		_ = red.Str().Set("name", "alice")

		key, err := db.Get("name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, key.ID, 1)
		testx.AssertEqual(t, key.Key, "name")
		testx.AssertEqual(t, key.Type, core.TypeString)
		testx.AssertEqual(t, key.Version, core.InitialVersion)
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
		testx.AssertEqual(t, key.MTime >= now, true)
	})
	t.Run("not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		key, err := db.Get("name")
		testx.AssertEqual(t, err, core.ErrNotFound)
		testx.AssertEqual(t, key, core.Key{})
	})
}

func TestKeys(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	tests := []struct {
		name    string
		pattern string
		want    []string
	}{
		{"all found", "*", []string{"name", "age"}},
		{"some found", "na*", []string{"name"}},
		{"none found", "key*", []string(nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keys, err := db.Keys(test.pattern)
			testx.AssertNoErr(t, err)
			for i, key := range keys {
				name := key.Key
				testx.AssertEqual(t, name, test.want[i])
			}
		})
	}
}

func TestPersist(t *testing.T) {
	t.Run("persist", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		err := db.Expire("name", 10*time.Second)
		testx.AssertNoErr(t, err)

		err = db.Persist("name")
		testx.AssertNoErr(t, err)

		key, _ := db.Get("name")
		if key.ETime != nil {
			t.Error("want nil, got expired time")
		}
	})
	t.Run("not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		err := db.Persist("name")
		testx.AssertEqual(t, err, core.ErrNotFound)
	})
}

func TestRandom(t *testing.T) {
	t.Run("random", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		key, err := db.Random()
		testx.AssertNoErr(t, err)
		if key.Key != "name" && key.Key != "age" {
			t.Errorf("want name or age, got %s", key.Key)
		}
	})
	t.Run("empty", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		key, err := db.Random()
		testx.AssertEqual(t, err, core.ErrNotFound)
		testx.AssertEqual(t, key, core.Key{})
	})
}

func TestRename(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		err := db.Rename("name", "key")
		testx.AssertNoErr(t, err)

		exists, _ := db.Exists("name")
		testx.AssertEqual(t, exists, false)

		val, _ := red.Str().Get("key")
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("rename", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		err := db.Rename("name", "age")
		testx.AssertNoErr(t, err)

		exists, _ := db.Exists("name")
		testx.AssertEqual(t, exists, false)

		val, err := red.Str().Get("age")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("same", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)

		err := db.Rename("name", "name")
		testx.AssertNoErr(t, err)

		val, err := red.Str().Get("name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		err := db.Rename("key1", "name")
		testx.AssertEqual(t, err, core.ErrNotFound)

		exists, _ := db.Exists("name")
		testx.AssertEqual(t, exists, true)

		exists, _ = db.Exists("key1")
		testx.AssertEqual(t, exists, false)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("str", "str")
		_, _ = red.Hash().Set("hash", "field", "value")

		err := db.Rename("str", "hash")
		testx.AssertNoErr(t, err)

		exists, _ := db.Exists("str")
		testx.AssertEqual(t, exists, false)

		exists, _ = db.Exists("hash")
		testx.AssertEqual(t, exists, true)
	})
}

func TestRenameNotExists(t *testing.T) {
	t.Run("rename", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		ok, err := db.RenameNotExists("name", "title")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)
		title, _ := red.Str().Get("title")
		testx.AssertEqual(t, title.String(), "alice")
	})
	t.Run("same name", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		ok, err := db.RenameNotExists("name", "name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
		name, _ := red.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})
	t.Run("old does not exist", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		ok, err := db.RenameNotExists("key1", "key2")
		testx.AssertEqual(t, err, core.ErrNotFound)
		testx.AssertEqual(t, ok, false)
	})
	t.Run("new exists", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)
		ok, err := db.RenameNotExists("name", "age")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
		age, _ := red.Str().Get("age")
		testx.AssertEqual(t, age, core.Value("25"))
	})
}

func TestScan(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("11", "11")
		_ = red.Str().Set("12", "12")
		_ = red.Str().Set("21", "21")
		_ = red.Str().Set("22", "22")
		_ = red.Str().Set("31", "31")

		tests := []struct {
			name    string
			cursor  int
			pattern string
			count   int

			wantCursor int
			wantKeys   []string
		}{
			{"all", 0, "*", 10, 5, []string{"11", "12", "21", "22", "31"}},
			{"some", 0, "2*", 10, 4, []string{"21", "22"}},
			{"none", 0, "n*", 10, 0, []string{}},
			{"cursor 1st", 0, "*", 2, 2, []string{"11", "12"}},
			{"cursor 2nd", 2, "*", 2, 4, []string{"21", "22"}},
			{"cursor 3rd", 4, "*", 2, 5, []string{"31"}},
			{"exhausted", 6, "*", 2, 0, []string{}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				out, err := db.Scan(test.cursor, test.pattern, core.TypeAny, test.count)
				testx.AssertNoErr(t, err)
				testx.AssertEqual(t, out.Cursor, test.wantCursor)
				keyNames := make([]string, len(out.Keys))
				for i, key := range out.Keys {
					keyNames[i] = key.Key
				}
				testx.AssertEqual(t, keyNames, test.wantKeys)
			})
		}
	})
	t.Run("type filter", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("k11", "11")
		_ = red.Str().Set("k12", "12")
		_, _ = red.Hash().Set("k21", "field", "value")
		_, _ = red.ZSet().Add("k22", "elem", 11)
		_ = red.Str().Set("k31", "31")

		out, err := db.Scan(0, "*", core.TypeString, 10)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Keys), 3)
		testx.AssertEqual(t, out.Keys[0].Key, "k11")
		testx.AssertEqual(t, out.Keys[1].Key, "k12")
		testx.AssertEqual(t, out.Keys[2].Key, "k31")
	})
}

func TestScanner(t *testing.T) {
	red, _ := getDB(t)
	defer red.Close()

	_ = red.Str().Set("11", "11")
	_ = red.Str().Set("12", "12")
	_ = red.Str().Set("21", "21")
	_ = red.Str().Set("22", "22")
	_ = red.Str().Set("31", "31")

	var keys []core.Key
	err := red.View(func(tx *redka.Tx) error {
		sc := tx.Key().Scanner("*", core.TypeAny, 2)
		for sc.Scan() {
			keys = append(keys, sc.Key())
		}
		return sc.Err()
	})
	testx.AssertNoErr(t, err)
	keyNames := make([]string, len(keys))
	for i, key := range keys {
		keyNames[i] = key.Key
	}
	testx.AssertEqual(t, keyNames, []string{"11", "12", "21", "22", "31"})
}

func getDB(tb testing.TB) (*redka.DB, *rkey.DB) {
	tb.Helper()
	red, err := redka.Open(":memory:", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return red, red.Key()
}
