package rstring_test

import (
	"testing"
	"time"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rstring"
	"github.com/nalgeon/redka/internal/testx"
)

func TestGet(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		_, str := getDB(t)
		err := str.Set("name", "alice")
		be.Err(t, err, nil)

		val, err := str.Get("name")
		be.Err(t, err, nil)
		be.Equal(t, val, core.Value("alice"))
	})
	t.Run("key not found", func(t *testing.T) {
		_, str := getDB(t)

		val, err := str.Get("name")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, val, core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")

		val, err := str.Get("person")
		be.Err(t, err, core.ErrNotFound)
		be.Equal(t, val, core.Value(nil))
	})
}

func TestGetMany(t *testing.T) {
	db, str := getDB(t)

	_ = str.Set("name", "alice")
	_ = str.Set("age", 25)
	_, _ = db.Hash().Set("hash1", "f1", "v1")
	_, _ = db.Hash().Set("hash2", "f2", "v2")

	tests := []struct {
		name string
		keys []string
		want map[string]core.Value
	}{
		{"all found", []string{"name", "age"},
			map[string]core.Value{
				"name": core.Value("alice"), "age": core.Value("25"),
			},
		},
		{"some found", []string{"name", "key1"},
			map[string]core.Value{
				"name": core.Value("alice"),
			},
		},
		{"none found", []string{"key1", "key2"},
			map[string]core.Value{},
		},
		{"key type mismatch", []string{"hash1", "hash2"},
			map[string]core.Value{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vals, err := str.GetMany(test.keys...)
			be.Err(t, err, nil)
			be.Equal(t, vals, test.want)
		})
	}
}

func TestGetSet(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, str := getDB(t)

		out, err := str.SetWith("name", "alice").Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Prev, core.Value(nil))
		be.Equal(t, out.Created, true)
		be.Equal(t, out.Updated, false)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 1)
		be.Equal(t, key.ETime, (*int64)(nil))

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))
	})
	t.Run("update key", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		out, err := str.SetWith("name", "bob").Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Prev, core.Value("alice"))
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, true)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 2)
		be.Equal(t, key.ETime, (*int64)(nil))

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("bob"))
	})
	t.Run("not changed", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		out, err := str.SetWith("name", "alice").Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Prev, core.Value("alice"))
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, true)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 2)
		be.Equal(t, key.ETime, (*int64)(nil))

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))
	})
	t.Run("with ttl", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		now := time.Now()
		ttl := time.Second
		out, err := str.SetWith("name", "bob").TTL(ttl).Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Prev, core.Value("alice"))
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, true)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 2)
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		be.Equal(t, got, want)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("bob"))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")

		out, err := str.SetWith("person", "alice").Run()
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, out.Prev, core.Value(nil))
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, false)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestIncr(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, str := getDB(t)

		val, err := str.Incr("age", 25)
		be.Err(t, err, nil)
		be.Equal(t, val, 25)

		key, _ := db.Key().Get("age")
		be.Equal(t, key.Version, 1)

		age, _ := str.Get("age")
		be.Equal(t, age.MustInt(), 25)
	})
	t.Run("increment", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("age", "25")

		val, err := str.Incr("age", 10)
		be.Err(t, err, nil)
		be.Equal(t, val, 35)

		key, _ := db.Key().Get("age")
		be.Equal(t, key.Version, 2)

		age, _ := str.Get("age")
		be.Equal(t, age.MustInt(), 35)
	})

	t.Run("decrement", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("age", "25")

		val, err := str.Incr("age", -10)
		be.Err(t, err, nil)
		be.Equal(t, val, 15)

		key, _ := db.Key().Get("age")
		be.Equal(t, key.Version, 2)

		age, _ := str.Get("age")
		be.Equal(t, age.MustInt(), 15)
	})
	t.Run("invalid int", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		val, err := str.Incr("name", 1)
		be.Err(t, err, core.ErrValueType)
		be.Equal(t, val, 0)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "age", 25)

		val, err := str.Incr("person", 10)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, val, 0)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestIncrFloat(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, str := getDB(t)

		val, err := str.IncrFloat("pi", 3.14)
		be.Err(t, err, nil)
		be.Equal(t, val, 3.14)

		key, _ := db.Key().Get("pi")
		be.Equal(t, key.Version, 1)

		pi, _ := str.Get("pi")
		be.Equal(t, pi.MustFloat(), 3.14)
	})
	t.Run("increment", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("pi", "3.14")

		val, err := str.IncrFloat("pi", 1.86)
		be.Err(t, err, nil)
		be.Equal(t, val, 5.0)

		key, _ := db.Key().Get("pi")
		be.Equal(t, key.Version, 2)

		pi, _ := str.Get("pi")
		be.Equal(t, pi.MustFloat(), 5.0)
	})

	t.Run("decrement", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("pi", "3.14")

		val, err := str.IncrFloat("pi", -1.14)
		be.Err(t, err, nil)
		be.Equal(t, val, 2.0)

		key, _ := db.Key().Get("pi")
		be.Equal(t, key.Version, 2)

		pi, _ := str.Get("pi")
		be.Equal(t, pi.MustFloat(), 2.0)
	})
	t.Run("invalid float", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		val, err := str.IncrFloat("name", 1.5)
		be.Err(t, err, core.ErrValueType)
		be.Equal(t, val, 0.0)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 1)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "age", 25.5)

		val, err := str.IncrFloat("person", 10.5)
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, val, 0.0)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestSet(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		db, str := getDB(t)

		tests := []struct {
			name  string
			key   string
			value any
			want  core.Value
		}{
			{"string", "name", "alice", core.Value("alice")},
			{"zero", "zero", 0, core.Value("0")},
			{"int", "age", 25, core.Value("25")},
			{"float", "pi", 3.14, core.Value("3.14")},
			{"bool true", "true", true, core.Value("1")},
			{"bool false", "false", false, core.Value("0")},
			{"bytes", "bytes", []byte("hello"), core.Value("hello")},
		}
		for _, test := range tests {
			err := str.Set(test.key, test.value)
			be.Err(t, err, nil)

			val, _ := str.Get(test.key)
			be.Equal(t, val, test.want)

			key, _ := db.Key().Get(test.key)
			be.Equal(t, key.Version, 1)
			be.Equal(t, key.ETime, (*int64)(nil))
		}
	})
	t.Run("empty string", func(t *testing.T) {
		db, str := getDB(t)

		err := str.Set("empty", "")
		be.Err(t, err, nil)

		val, _ := str.Get("empty")
		be.Equal(t, val.IsZero(), true)

		key, _ := db.Key().Get("empty")
		be.Equal(t, key.Version, 1)
	})
	t.Run("struct", func(t *testing.T) {
		_, str := getDB(t)

		err := str.Set("struct", struct{ Name string }{"alice"})
		be.Err(t, err, core.ErrValueType)
	})
	t.Run("nil", func(t *testing.T) {
		_, str := getDB(t)

		err := str.Set("nil", nil)
		be.Err(t, err, core.ErrValueType)
	})
	t.Run("update", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		err := str.Set("name", "bob")
		be.Err(t, err, nil)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 2)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("bob"))
	})
	t.Run("change value type", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		err := str.Set("name", true)
		be.Err(t, err, nil)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 2)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("1"))
	})
	t.Run("not changed", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		err := str.Set("name", "alice")
		be.Err(t, err, nil)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 2)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")

		err := str.Set("person", "alice")
		be.Err(t, err, core.ErrKeyType)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)

		hval, _ := db.Hash().Get("person", "name")
		be.Equal(t, hval, core.Value("alice"))
	})
}

func TestSetExists(t *testing.T) {
	t.Run("key exists", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		out, err := str.SetWith("name", "bob").IfExists().Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, true)

		name, _ := str.Get("name")
		be.Equal(t, name, core.Value("bob"))

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 2)
		be.Equal(t, key.ETime, (*int64)(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		_, str := getDB(t)

		out, err := str.SetWith("name", "alice").IfExists().Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, false)

		_, err = str.Get("name")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("with ttl", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		now := time.Now()
		ttl := time.Second
		out, err := str.SetWith("name", "cindy").IfExists().TTL(ttl).Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, true)

		key, _ := db.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		be.Equal(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")

		out, err := str.SetWith("person", "name").IfExists().Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, false)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestSetExpire(t *testing.T) {
	t.Run("no ttl", func(t *testing.T) {
		db, str := getDB(t)

		err := str.SetExpire("name", "alice", 0)
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))

		key, _ := db.Key().Get("name")
		be.Equal(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		db, str := getDB(t)

		now := time.Now()
		ttl := time.Second
		err := str.SetExpire("name", "alice", ttl)
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))

		key, _ := db.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		be.Equal(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")
		err := str.SetExpire("person", "alice", time.Second)
		be.Err(t, err, core.ErrKeyType)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestSetWithTTL(t *testing.T) {
	t.Run("no ttl", func(t *testing.T) {
		db, str := getDB(t)

		_, err := str.SetWith("name", "alice").TTL(0).Run()
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))

		key, _ := db.Key().Get("name")
		be.Equal(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		db, str := getDB(t)

		now := time.Now()
		ttl := time.Second
		_, err := str.SetWith("name", "alice").TTL(ttl).Run()
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))

		key, _ := db.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		be.Equal(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")
		_, err := str.SetWith("person", "alice").TTL(time.Second).Run()
		be.Err(t, err, core.ErrKeyType)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestSetWithAt(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		db, str := getDB(t)

		_, err := str.SetWith("name", "alice").At(time.Time{}).Run()
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))

		key, _ := db.Key().Get("name")
		be.Equal(t, key.ETime, (*int64)(nil))
	})
	t.Run("future", func(t *testing.T) {
		db, str := getDB(t)

		at := time.Now().Add(60 * time.Second)
		_, err := str.SetWith("name", "alice").At(at).Run()
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))

		key, _ := db.Key().Get("name")
		got := (*key.ETime) / 1000
		want := at.UnixMilli() / 1000
		be.Equal(t, got, want)
	})
	t.Run("past", func(t *testing.T) {
		db, str := getDB(t)

		at := time.Now().Add(-60 * time.Second)
		_, err := str.SetWith("name", "alice").At(at).Run()
		be.Err(t, err, nil)

		_, err = str.Get("name")
		be.Err(t, err, core.ErrNotFound)

		_, err = db.Key().Get("name")
		be.Err(t, err, core.ErrNotFound)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")

		at := time.Now().Add(60 * time.Second)
		_, err := str.SetWith("person", "alice").At(at).Run()
		be.Err(t, err, core.ErrKeyType)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestSetWithKeepTTL(t *testing.T) {
	t.Run("delete ttl", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.SetExpire("name", "alice", 60*time.Second)

		_, err := str.SetWith("name", "bob").Run()
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("bob"))

		key, _ := db.Key().Get("name")
		be.Equal(t, key.ETime, (*int64)(nil))
	})
	t.Run("keep ttl", func(t *testing.T) {
		db, str := getDB(t)
		ttl := 60 * time.Second
		_ = str.SetExpire("name", "alice", ttl)

		now := time.Now()
		_, err := str.SetWith("name", "alice").KeepTTL().Run()
		be.Err(t, err, nil)

		val, _ := str.Get("name")
		be.Equal(t, val, core.Value("alice"))

		key, _ := db.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		be.Equal(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")
		_, err := str.SetWith("person", "alice").KeepTTL().Run()
		be.Err(t, err, core.ErrKeyType)

		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)
	})
}

func TestSetMany(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, str := getDB(t)

		err := str.SetMany(map[string]any{
			"name": "alice",
			"age":  25,
		})
		be.Err(t, err, nil)

		key1, _ := db.Key().Get("name")
		be.Equal(t, key1.Version, 1)
		key2, _ := db.Key().Get("age")
		be.Equal(t, key2.Version, 1)

		name, _ := str.Get("name")
		be.Equal(t, name, core.Value("alice"))
		age, _ := str.Get("age")
		be.Equal(t, age, core.Value("25"))
	})
	t.Run("update", func(t *testing.T) {
		db, str := getDB(t)

		_ = str.Set("name", "alice")
		_ = str.Set("age", 25)

		err := str.SetMany(map[string]any{
			"name": "bob",
			"age":  50,
		})
		be.Err(t, err, nil)

		key1, _ := db.Key().Get("name")
		be.Equal(t, key1.Version, 2)
		key2, _ := db.Key().Get("age")
		be.Equal(t, key2.Version, 2)

		name, _ := str.Get("name")
		be.Equal(t, name, core.Value("bob"))
		age, _ := str.Get("age")
		be.Equal(t, age, core.Value("50"))
	})
	t.Run("invalid type", func(t *testing.T) {
		_, str := getDB(t)
		err := str.SetMany(map[string]any{
			"name": "alice",
			"age":  struct{ Name string }{"alice"},
		})
		be.Err(t, err, core.ErrValueType)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")

		err := str.SetMany(map[string]any{
			"name":   "alice",
			"person": "alice",
		})
		be.Err(t, err, core.ErrKeyType)

		_, err = str.Get("name")
		be.Err(t, err, core.ErrNotFound)
		_, err = str.Get("person")
		be.Err(t, err, core.ErrNotFound)

		hval, _ := db.Hash().Get("person", "name")
		be.Equal(t, hval, core.Value("alice"))
	})
}

func TestSetNotExists(t *testing.T) {
	t.Run("key exists", func(t *testing.T) {
		db, str := getDB(t)
		_ = str.Set("name", "alice")

		out, err := str.SetWith("name", "bob").IfNotExists().Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, false)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 1)

		name, _ := str.Get("name")
		be.Equal(t, name, core.Value("alice"))
	})
	t.Run("key not found", func(t *testing.T) {
		db, str := getDB(t)

		out, err := str.SetWith("name", "alice").IfNotExists().Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Created, true)
		be.Equal(t, out.Updated, false)

		key, _ := db.Key().Get("name")
		be.Equal(t, key.Version, 1)

		name, _ := str.Get("name")
		be.Equal(t, name, core.Value("alice"))
	})
	t.Run("with ttl", func(t *testing.T) {
		db, str := getDB(t)

		now := time.Now()
		ttl := time.Second
		out, err := str.SetWith("city", "paris").IfNotExists().TTL(ttl).Run()
		be.Err(t, err, nil)
		be.Equal(t, out.Created, true)
		be.Equal(t, out.Updated, false)

		key, _ := db.Key().Get("city")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		be.Equal(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, str := getDB(t)
		_, _ = db.Hash().Set("person", "name", "alice")

		out, err := str.SetWith("person", "alice").IfNotExists().Run()
		be.Err(t, err, core.ErrKeyType)
		be.Equal(t, out.Created, false)
		be.Equal(t, out.Updated, false)

		_, err = str.Get("name")
		be.Err(t, err, core.ErrNotFound)
	})
}

func getDB(tb testing.TB) (*redka.DB, *rstring.DB) {
	tb.Helper()
	db := testx.OpenDB(tb)
	return db, db.Str()
}
