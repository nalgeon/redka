package redka_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
)

func TestKeyExists(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := db.Exists(tt.keys...)
			assertNoErr(t, err)
			assertEqual(t, count, tt.want)
		})
	}
}

func TestKeySearch(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := db.Search(tt.pattern)
			assertNoErr(t, err)
			assertEqual(t, keys, tt.want)
		})
	}
}

func TestKeyScan(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := db.Scan(tt.cursor, tt.pattern, tt.count)
			assertNoErr(t, err)
			assertEqual(t, out.Cursor, tt.wantCursor)
			keyNames := make([]string, len(out.Keys))
			for i, key := range out.Keys {
				keyNames[i] = key.Key
			}
			assertEqual(t, keyNames, tt.wantKeys)
		})
	}
}

func TestKeyScanner(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	_ = red.Str().Set("11", "11")
	_ = red.Str().Set("12", "12")
	_ = red.Str().Set("21", "21")
	_ = red.Str().Set("22", "22")
	_ = red.Str().Set("31", "31")
	var keys []redka.Key
	err := red.View(func(tx *redka.Tx) error {
		sc := tx.Key().Scanner("*", 2)
		for sc.Scan() {
			keys = append(keys, sc.Key())
		}
		return sc.Err()
	})
	assertNoErr(t, err)
	keyNames := make([]string, len(keys))
	for i, key := range keys {
		keyNames[i] = key.Key
	}
	assertEqual(t, keyNames, []string{"11", "12", "21", "22", "31"})
}

func TestKeyRandom(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	key, err := db.Random()
	assertNoErr(t, err)
	if key != "name" && key != "age" {
		t.Errorf("want name or age, got %s", key)
	}
}

func TestKeyGet(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	tests := []struct {
		name string
		key  string
		want redka.Key
	}{
		{"found", "name",
			redka.Key{
				ID: 1, Key: "name", Type: 1, Version: 1, ETime: nil, MTime: 0,
			},
		},
		{"not found", "key1", redka.Key{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := db.Get(tt.key)
			assertNoErr(t, err)
			assertEqual(t, key.ID, tt.want.ID)
			assertEqual(t, key.Key, tt.want.Key)
			assertEqual(t, key.Type, tt.want.Type)
			assertEqual(t, key.Version, tt.want.Version)
			assertEqual(t, key.ETime, tt.want.ETime)
		})
	}
}

func TestKeyExpire(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	now := time.Now()
	ttl := 10 * time.Second
	ok, err := db.Expire("name", ttl)
	assertNoErr(t, err)
	assertEqual(t, ok, true)

	key, _ := db.Get("name")
	if key.ETime == nil {
		t.Error("want expired time, got nil")
	}
	got := (*key.ETime) / 1000
	want := now.Add(ttl).UnixMilli() / 1000
	if got != want {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestKeyExpireAt(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	now := time.Now()
	at := now.Add(10 * time.Second)
	ok, err := db.ETime("name", at)
	assertNoErr(t, err)
	assertEqual(t, ok, true)

	key, _ := db.Get("name")
	if key.ETime == nil {
		t.Error("want expired time, got nil")
	}
	got := (*key.ETime) / 1000
	want := at.UnixMilli() / 1000
	if got != want {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestKeyPersist(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	ok, err := db.Expire("name", 10*time.Second)
	assertNoErr(t, err)
	assertEqual(t, ok, true)

	ok, err = db.Persist("name")
	assertNoErr(t, err)
	assertEqual(t, ok, true)

	key, _ := db.Get("name")
	if key.ETime != nil {
		t.Error("want nil, got expired time")
	}
}

func TestKeyRename(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()

	tests := []struct {
		name   string
		key    string
		newKey string
		val    string
	}{
		{"create", "name", "city", "alice"},
		{"rename", "name", "age", "alice"},
		{"same", "name", "name", "alice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = red.Str().Set("name", "alice")
			_ = red.Str().Set("age", 25)

			ok, err := db.Rename(tt.key, tt.newKey)
			assertNoErr(t, err)
			assertEqual(t, ok, true)

			val, err := red.Str().Get(tt.newKey)
			assertNoErr(t, err)
			assertEqual(t, val.String(), tt.val)
		})
	}
	t.Run("not found", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		ok, err := db.Rename("key1", "name")
		assertEqual(t, err, redka.ErrKeyNotFound)
		assertEqual(t, ok, false)
	})
}

func TestKeyRenameNX(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()

	t.Run("rename", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		ok, err := db.RenameNX("name", "title")
		assertNoErr(t, err)
		assertEqual(t, ok, true)
		title, _ := red.Str().Get("title")
		assertEqual(t, title.String(), "alice")
	})
	t.Run("same name", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		ok, err := db.RenameNX("name", "name")
		assertNoErr(t, err)
		assertEqual(t, ok, true)
		name, _ := red.Str().Get("name")
		assertEqual(t, name.String(), "alice")
	})
	t.Run("old does not exist", func(t *testing.T) {
		ok, err := db.RenameNX("key1", "key2")
		assertEqual(t, err, redka.ErrKeyNotFound)
		assertEqual(t, ok, false)
	})
	t.Run("new exists", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)
		ok, err := db.RenameNX("name", "age")
		assertNoErr(t, err)
		assertEqual(t, ok, false)
		age, _ := red.Str().Get("age")
		assertEqual(t, age, redka.Value("25"))
	})
}

func TestDelete(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
	tests := []struct {
		name string
		keys []string
		want int
	}{
		{"all", []string{"name", "age"}, 2},
		{"some", []string{"name"}, 1},
		{"none", []string{"key1"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = red.Str().Set("name", "alice")
			_ = red.Str().Set("age", 25)

			count, err := db.Delete(tt.keys...)
			assertNoErr(t, err)
			assertEqual(t, count, tt.want)

			count, _ = db.Exists(tt.keys...)
			assertEqual(t, count, 0)

			for _, key := range tt.keys {
				val, _ := red.Str().Get(key)
				assertEqual(t, val.IsEmpty(), true)
			}
		})
	}
}
