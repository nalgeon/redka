package rkey_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/testx"
)

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

func TestScan(t *testing.T) {
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
			out, err := db.Scan(test.cursor, test.pattern, test.count)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, out.Cursor, test.wantCursor)
			keyNames := make([]string, len(out.Keys))
			for i, key := range out.Keys {
				keyNames[i] = key.Key
			}
			testx.AssertEqual(t, keyNames, test.wantKeys)
		})
	}
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

func TestRandom(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	key, err := db.Random()
	testx.AssertNoErr(t, err)
	if key.Key != "name" && key.Key != "age" {
		t.Errorf("want name or age, got %s", key.Key)
	}
}

func TestGet(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = red.Str().Set("name", "alice")
	_ = red.Str().Set("age", 25)

	tests := []struct {
		name string
		key  string
		want core.Key
	}{
		{"found", "name",
			core.Key{
				ID: 1, Key: "name", Type: 1, Version: 1, ETime: nil, MTime: 0,
			},
		},
		{"not found", "key1", core.Key{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := db.Get(test.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, key.ID, test.want.ID)
			testx.AssertEqual(t, key.Key, test.want.Key)
			testx.AssertEqual(t, key.Type, test.want.Type)
			testx.AssertEqual(t, key.Version, test.want.Version)
			testx.AssertEqual(t, key.ETime, test.want.ETime)
		})
	}
}

func TestExpire(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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

func TestExpireAt(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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

func TestPersist(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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

func TestRename(t *testing.T) {
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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			red, db := getDB(t)
			defer red.Close()

			_ = red.Str().Set("name", "alice")
			_ = red.Str().Set("age", 25)

			err := db.Rename(test.key, test.newKey)
			testx.AssertNoErr(t, err)

			val, err := red.Str().Get(test.newKey)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val.String(), test.val)
		})
	}
	t.Run("not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = red.Str().Set("name", "alice")
		err := db.Rename("key1", "name")
		testx.AssertEqual(t, err, core.ErrNotFound)
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

func TestDelete(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want int
	}{
		{"all", []string{"name", "age"}, 2},
		{"some", []string{"name"}, 1},
		{"none", []string{"key1"}, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			red, db := getDB(t)
			defer red.Close()

			_ = red.Str().Set("name", "alice")
			_ = red.Str().Set("age", 25)

			count, err := db.Delete(test.keys...)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, test.want)

			count, _ = db.Count(test.keys...)
			testx.AssertEqual(t, count, 0)

			for _, key := range test.keys {
				val, _ := red.Str().Get(key)
				testx.AssertEqual(t, val.Exists(), false)
			}
		})
	}
}

func TestDeleteExpired(t *testing.T) {
	t.Run("delete all", func(t *testing.T) {
		red, _ := getDB(t)
		defer red.Close()
		db := rkey.New(red.SQL)

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
		red, _ := getDB(t)
		defer red.Close()
		db := rkey.New(red.SQL)

		_ = red.Str().SetExpires("name", "alice", 1*time.Millisecond)
		_ = red.Str().SetExpires("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := db.DeleteExpired(1)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)
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

func getDB(tb testing.TB) (*redka.DB, *rkey.DB) {
	tb.Helper()
	red, err := redka.Open(":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	return red, red.Key()
}
