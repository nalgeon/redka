package rkey_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/testx"
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
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, tt.want)
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
			testx.AssertNoErr(t, err)
			for i, key := range keys {
				name := key.Key
				testx.AssertEqual(t, name, tt.want[i])
			}
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
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, out.Cursor, tt.wantCursor)
			keyNames := make([]string, len(out.Keys))
			for i, key := range out.Keys {
				keyNames[i] = key.Key
			}
			testx.AssertEqual(t, keyNames, tt.wantKeys)
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
	testx.AssertNoErr(t, err)
	keyNames := make([]string, len(keys))
	for i, key := range keys {
		keyNames[i] = key.Key
	}
	testx.AssertEqual(t, keyNames, []string{"11", "12", "21", "22", "31"})
}

func TestKeyRandom(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()
	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	key, err := db.Random()
	testx.AssertNoErr(t, err)
	if key.Key != "name" && key.Key != "age" {
		t.Errorf("want name or age, got %s", key.Key)
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
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, key.ID, tt.want.ID)
			testx.AssertEqual(t, key.Key, tt.want.Key)
			testx.AssertEqual(t, key.Type, tt.want.Type)
			testx.AssertEqual(t, key.Version, tt.want.Version)
			testx.AssertEqual(t, key.ETime, tt.want.ETime)
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
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, ok, true)

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
	ok, err := db.ExpireAt("name", at)
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, ok, true)

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
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, ok, true)

	ok, err = db.Persist("name")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, ok, true)

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
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, ok, true)

			val, err := red.Str().Get(tt.newKey)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val.String(), tt.val)
		})
	}
	t.Run("not found", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		ok, err := db.Rename("key1", "name")
		testx.AssertEqual(t, err, redka.ErrKeyNotFound)
		testx.AssertEqual(t, ok, false)
	})
}

func TestKeyRenameNX(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Key()

	t.Run("rename", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		ok, err := db.RenameNX("name", "title")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)
		title, _ := red.Str().Get("title")
		testx.AssertEqual(t, title.String(), "alice")
	})
	t.Run("same name", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		ok, err := db.RenameNX("name", "name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
		name, _ := red.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})
	t.Run("old does not exist", func(t *testing.T) {
		ok, err := db.RenameNX("key1", "key2")
		testx.AssertEqual(t, err, redka.ErrKeyNotFound)
		testx.AssertEqual(t, ok, false)
	})
	t.Run("new exists", func(t *testing.T) {
		_ = red.Str().Set("name", "alice")
		_ = red.Str().Set("age", 25)
		ok, err := db.RenameNX("name", "age")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
		age, _ := red.Str().Get("age")
		testx.AssertEqual(t, age, redka.Value("25"))
	})
}

func TestKeyDelete(t *testing.T) {
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
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, tt.want)

			count, _ = db.Exists(tt.keys...)
			testx.AssertEqual(t, count, 0)

			for _, key := range tt.keys {
				val, _ := red.Str().Get(key)
				testx.AssertEqual(t, val.IsEmpty(), true)
			}
		})
	}
}

func TestKeyDeleteExpired(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := rkey.New(red.SQL)
	t.Run("delete all", func(t *testing.T) {
		_ = red.Str().SetExpires("name", "alice", 1*time.Millisecond)
		_ = red.Str().SetExpires("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := db.DeleteExpired(0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		count, _ = db.Exists("name", "age")
		testx.AssertEqual(t, count, 0)
	})
	t.Run("delete n", func(t *testing.T) {
		_ = red.Str().SetExpires("name", "alice", 1*time.Millisecond)
		_ = red.Str().SetExpires("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := db.DeleteExpired(1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)
	})
}

func TestKeyDeleteAll(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := rkey.New(red.SQL)

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	err := db.DeleteAll()
	testx.AssertNoErr(t, err)

	count, _ := db.Exists("name", "age")
	testx.AssertEqual(t, count, 0)
}

func getDB(tb testing.TB) *redka.DB {
	tb.Helper()
	db, err := redka.Open(":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	return db
}
