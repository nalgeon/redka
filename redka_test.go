package redka_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/testx"
)

// #region DB

func TestDBView(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	err := db.View(func(tx *redka.Tx) error {
		count, err := tx.Key().Exists("name", "age")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		name, err := tx.Str().Get("name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, name.String(), "alice")

		age, err := tx.Str().Get("age")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, age.MustInt(), 25)
		return nil
	})
	testx.AssertNoErr(t, err)
}

func TestDBUpdate(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	err := db.Update(func(tx *redka.Tx) error {
		err := tx.Str().Set("name", "alice")
		if err != nil {
			return err
		}

		err = tx.Str().Set("age", 25)
		if err != nil {
			return err
		}
		return nil
	})
	testx.AssertNoErr(t, err)

	err = db.View(func(tx *redka.Tx) error {
		count, _ := tx.Key().Exists("name", "age")
		testx.AssertEqual(t, count, 2)

		name, _ := tx.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")

		age, _ := tx.Str().Get("age")
		testx.AssertEqual(t, age.MustInt(), 25)
		return nil
	})
	testx.AssertNoErr(t, err)
}

func TestDBUpdateRollback(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	var errRollback = errors.New("rollback")

	err := db.Update(func(tx *redka.Tx) error {
		_ = tx.Str().Set("name", "bob")
		_ = tx.Str().Set("age", 50)
		return errRollback
	})
	testx.AssertEqual(t, err, errRollback)

	name, _ := db.Str().Get("name")
	testx.AssertEqual(t, name.String(), "alice")
	age, _ := db.Str().Get("age")
	testx.AssertEqual(t, age.MustInt(), 25)
}

func TestDBFlush(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	err := db.Flush()
	testx.AssertNoErr(t, err)

	count, _ := db.Key().Exists("name", "age")
	testx.AssertEqual(t, count, 0)

}

// #endregion

// #region Key

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

// #endregion

// #region String

func TestStringGet(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name", "alice")

	tests := []struct {
		name string
		key  string
		want any
	}{
		{"key found", "name", redka.Value("alice")},
		{"key not found", "key1", redka.Value(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := db.Get(tt.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val, tt.want)
		})
	}
}

func TestStringGetMany(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name", "alice")
	_ = db.Set("age", 25)

	tests := []struct {
		name string
		keys []string
		want []redka.Value
	}{
		{"all found", []string{"name", "age"},
			[]redka.Value{redka.Value("alice"), redka.Value("25")},
		},
		{"some found", []string{"name", "key1"},
			[]redka.Value{redka.Value("alice"), redka.Value(nil)},
		},
		{"none found", []string{"key1", "key2"},
			[]redka.Value{redka.Value(nil), redka.Value(nil)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vals, err := db.GetMany(tt.keys...)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, vals, tt.want)
		})
	}
}

func TestStringSet(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	tests := []struct {
		name  string
		key   string
		value any
		want  any
	}{
		{"string", "name", "alice", redka.Value("alice")},
		{"empty string", "empty", "", redka.Value("")},
		{"int", "age", 25, redka.Value("25")},
		{"float", "pi", 3.14, redka.Value("3.14")},
		{"bool true", "ok", true, redka.Value("1")},
		{"bool false", "ok", false, redka.Value("0")},
		{"bytes", "bytes", []byte("hello"), redka.Value("hello")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.Set(tt.key, tt.value)
			testx.AssertNoErr(t, err)

			val, _ := db.Get(tt.key)
			testx.AssertEqual(t, val, tt.want)

			key, _ := red.Key().Get(tt.key)
			testx.AssertEqual(t, key.ETime, (*int64)(nil))
		})
	}
	t.Run("struct", func(t *testing.T) {
		err := db.Set("struct", struct{ Name string }{"alice"})
		testx.AssertErr(t, err, redka.ErrInvalidType)
	})
	t.Run("nil", func(t *testing.T) {
		err := db.Set("nil", nil)
		testx.AssertErr(t, err, redka.ErrInvalidType)
	})
	t.Run("update", func(t *testing.T) {
		_ = db.Set("name", "alice")
		err := db.Set("name", "bob")
		testx.AssertNoErr(t, err)
		val, _ := db.Get("name")
		testx.AssertEqual(t, val, redka.Value("bob"))
	})
	t.Run("change type", func(t *testing.T) {
		_ = db.Set("name", "alice")
		err := db.Set("name", true)
		testx.AssertNoErr(t, err)
		val, _ := db.Get("name")
		testx.AssertEqual(t, val, redka.Value("1"))
	})
	t.Run("not changed", func(t *testing.T) {
		_ = db.Set("name", "alice")
		err := db.Set("name", "alice")
		testx.AssertNoErr(t, err)
		val, _ := db.Get("name")
		testx.AssertEqual(t, val, redka.Value("alice"))
	})
}

func TestStringSetExpires(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	t.Run("no ttl", func(t *testing.T) {
		err := db.SetExpires("name", "alice", 0)
		testx.AssertNoErr(t, err)

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, redka.Value("alice"))

		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		now := time.Now()
		ttl := time.Second
		err := db.SetExpires("name", "alice", ttl)
		testx.AssertNoErr(t, err)

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, redka.Value("alice"))

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)
	})
}

func TestStringSetNotExists(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name", "alice")

	tests := []struct {
		name  string
		key   string
		value any
		want  bool
	}{
		{"new key", "age", 25, true},
		{"existing key", "name", "bob", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := db.SetNotExists(tt.key, tt.value, 0)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, ok, tt.want)

			key, _ := red.Key().Get(tt.key)
			testx.AssertEqual(t, key.ETime, (*int64)(nil))
		})
	}
	t.Run("with ttl", func(t *testing.T) {
		now := time.Now()
		ttl := time.Second
		ok, err := db.SetNotExists("city", "paris", ttl)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)

		key, _ := red.Key().Get("city")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)
	})
}

func TestStringSetExists(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name", "alice")

	tests := []struct {
		name  string
		key   string
		value any
		want  bool
	}{
		{"new key", "age", 25, false},
		{"existing key", "name", "bob", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := db.SetExists(tt.key, tt.value, 0)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, ok, tt.want)

			key, _ := red.Key().Get(tt.key)
			testx.AssertEqual(t, key.ETime, (*int64)(nil))
		})
	}
	t.Run("with ttl", func(t *testing.T) {
		now := time.Now()
		ttl := time.Second
		ok, err := db.SetExists("name", "cindy", ttl)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)
	})
}

func TestStringGetSet(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()

	t.Run("create key", func(t *testing.T) {
		val, err := db.GetSet("name", "alice", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, redka.Value(nil))
		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("update key", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.GetSet("name", "bob", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, redka.Value("alice"))
		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("not changed", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.GetSet("name", "alice", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, redka.Value("alice"))
		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		_ = db.Set("name", "alice")

		now := time.Now()
		ttl := time.Second
		val, err := db.GetSet("name", "bob", ttl)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, redka.Value("alice"))

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)
	})
}

func TestStringSetMany(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()

	t.Run("create", func(t *testing.T) {
		err := db.SetMany(
			redka.KeyValue{Key: "name", Value: "alice"},
			redka.KeyValue{Key: "age", Value: 25},
		)
		testx.AssertNoErr(t, err)
		name, _ := db.Get("name")
		testx.AssertEqual(t, name, redka.Value("alice"))
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, redka.Value("25"))
	})
	t.Run("update", func(t *testing.T) {
		_ = db.Set("name", "alice")
		_ = db.Set("age", 25)
		err := db.SetMany(
			redka.KeyValue{Key: "name", Value: "bob"},
			redka.KeyValue{Key: "age", Value: 50},
		)
		testx.AssertNoErr(t, err)
		name, _ := db.Get("name")
		testx.AssertEqual(t, name, redka.Value("bob"))
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, redka.Value("50"))
	})
	t.Run("invalid type", func(t *testing.T) {
		err := db.SetMany(
			redka.KeyValue{Key: "name", Value: "alice"},
			redka.KeyValue{Key: "age", Value: struct{ Name string }{"alice"}},
		)
		testx.AssertErr(t, err, redka.ErrInvalidType)
	})
}

func TestStringSetManyNX(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name", "alice")

	t.Run("create", func(t *testing.T) {
		ok, err := db.SetManyNX(
			redka.KeyValue{Key: "age", Value: 25},
			redka.KeyValue{Key: "city", Value: "wonderland"},
		)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, redka.Value("25"))
		city, _ := db.Get("city")
		testx.AssertEqual(t, city, redka.Value("wonderland"))
	})
	t.Run("update", func(t *testing.T) {
		_ = db.Set("age", 25)
		_ = db.Set("city", "wonderland")
		ok, err := db.SetManyNX(
			redka.KeyValue{Key: "age", Value: 50},
			redka.KeyValue{Key: "city", Value: "wonderland"},
		)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, redka.Value("25"))
		city, _ := db.Get("city")
		testx.AssertEqual(t, city, redka.Value("wonderland"))
	})
	t.Run("invalid type", func(t *testing.T) {
		ok, err := db.SetManyNX(
			redka.KeyValue{Key: "name", Value: "alice"},
			redka.KeyValue{Key: "age", Value: struct{ Name string }{"alice"}},
		)
		testx.AssertErr(t, err, redka.ErrInvalidType)
		testx.AssertEqual(t, ok, false)
	})
}

func TestStringLength(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name1", "alice")
	_ = db.Set("name2", "bobby tables")
	_ = db.Set("name3", "")
	_ = db.Set("age", 25)

	tests := []struct {
		name string
		key  string
		want int
	}{
		{"length1", "name1", 5},
		{"length2", "name2", 12},
		{"empty", "name3", 0},
		{"not found", "other", 0},
		{"int", "age", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := db.Length(tt.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, n, tt.want)
		})
	}
}

func TestStringGetRange(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name", "alice")

	tests := []struct {
		name  string
		key   string
		start int
		end   int
		want  string
	}{
		{"all", "name", 0, -1, "alice"},
		{"partial", "name", 0, 2, "ali"},
		{"empty", "name", 10, 20, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := db.GetRange(tt.key, tt.start, tt.end)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val.String(), tt.want)
		})
	}
}

func TestStringSetRange(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()

	tests := []struct {
		name  string
		key   string
		start int
		value string
		want  []byte
	}{
		{"create", "city", 0, "paris", []byte("paris")},
		{"replace", "name", 1, "xxx", []byte("axxxe")},
		{"append", "name", 5, " and charlie", []byte("alice and charlie")},
		{"empty", "name", 8, "x", []byte{'a', 'l', 'i', 'c', 'e', 0, 0, 0, 'x'}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = db.Set("name", "alice")
			n, err := db.SetRange(tt.key, tt.start, tt.value)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, n, len(tt.want))
			val, _ := db.Get(tt.key)
			testx.AssertEqual(t, val.Bytes(), tt.want)
		})
	}
}

func TestStringAppend(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()

	tests := []struct {
		name  string
		key   string
		value string
		want  []byte
	}{
		{"create", "city", "paris", []byte("paris")},
		{"append", "name", " and charlie", []byte("alice and charlie")},
		{"empty", "name", "", []byte("alice")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = db.Set("name", "alice")
			n, err := db.Append(tt.key, tt.value)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, n, len(tt.want))
			val, _ := db.Get(tt.key)
			testx.AssertEqual(t, val, redka.Value(tt.want))
		})
	}
}

func TestStringIncr(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()

	tests := []struct {
		name  string
		key   string
		value int
		want  int
	}{
		{"create", "age", 10, 10},
		{"increment", "age", 15, 25},
		{"decrement", "age", -5, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := db.Incr(tt.key, tt.value)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val, tt.want)
		})
	}
	t.Run("invalid int", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.Incr("name", 1)
		testx.AssertErr(t, err, redka.ErrInvalidType)
		testx.AssertEqual(t, val, 0)
	})
}

func TestStringIncrFloat(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()

	tests := []struct {
		name  string
		key   string
		value float64
		want  float64
	}{
		{"create", "pi", 3.14, 3.14},
		{"increment", "pi", 1.86, 5},
		{"decrement", "pi", -1.5, 3.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := db.IncrFloat(tt.key, tt.value)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val, tt.want)
		})
	}
	t.Run("invalid float", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.IncrFloat("name", 1.5)
		testx.AssertErr(t, err, redka.ErrInvalidType)
		testx.AssertEqual(t, val, 0.0)
	})
}

func TestStringDelete(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	tests := []struct {
		name string
		keys []string
		want int
	}{
		{"delete one", []string{"name"}, 1},
		{"delete some", []string{"name", "city"}, 1},
		{"delete many", []string{"name", "age"}, 2},
		{"delete none", []string{"city"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = db.Set("name", "alice")
			_ = db.Set("age", 25)
			count, err := db.Delete(tt.keys...)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, tt.want)
			for _, key := range tt.keys {
				val, _ := db.Get(key)
				testx.AssertEqual(t, val.IsEmpty(), true)
			}
		})
	}
}

// #endregion

func getDB(tb testing.TB) *redka.DB {
	tb.Helper()
	db, err := redka.Open(":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	return db
}
