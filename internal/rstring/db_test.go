package rstring_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rstring"
	"github.com/nalgeon/redka/internal/testx"
)

func TestGet(t *testing.T) {
	t.Run("key found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		val, err := db.Get("name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, core.Value("alice"))
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		val, err := db.Get("name")
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, val, core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")

		val, err := db.Get("person")
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, val, core.Value(nil))
	})
}

func TestGetMany(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = db.Set("name", "alice")
	_ = db.Set("age", 25)
	_, _ = red.Hash().Set("hash1", "f1", "v1")
	_, _ = red.Hash().Set("hash2", "f2", "v2")

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
			vals, err := db.GetMany(test.keys...)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, vals, test.want)
		})
	}
}

func TestGetSet(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		out, err := db.SetWith("name", "alice").Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Prev, core.Value(nil))
		testx.AssertEqual(t, out.Created, true)
		testx.AssertEqual(t, out.Updated, false)

		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("alice"))
	})
	t.Run("update key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		out, err := db.SetWith("name", "bob").Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Prev, core.Value("alice"))
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, true)

		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("bob"))
	})
	t.Run("not changed", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		out, err := db.SetWith("name", "alice").Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Prev, core.Value("alice"))
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, true)

		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("alice"))
	})
	t.Run("with ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		now := time.Now()
		ttl := time.Second
		out, err := db.SetWith("name", "bob").TTL(ttl).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Prev, core.Value("alice"))
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, true)

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("bob"))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")

		out, err := db.SetWith("person", "name").Run()
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, out.Prev, core.Value(nil))
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, false)
	})
}

func TestIncr(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		val, err := db.Incr("age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25)

		age, _ := db.Get("age")
		testx.AssertEqual(t, age.MustInt(), 25)
	})
	t.Run("increment", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("age", "25")

		val, err := db.Incr("age", 10)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 35)

		age, _ := db.Get("age")
		testx.AssertEqual(t, age.MustInt(), 35)
	})

	t.Run("decrement", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("age", "25")

		val, err := db.Incr("age", -10)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 15)

		age, _ := db.Get("age")
		testx.AssertEqual(t, age.MustInt(), 15)
	})
	t.Run("invalid int", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		val, err := db.Incr("name", 1)
		testx.AssertErr(t, err, core.ErrValueType)
		testx.AssertEqual(t, val, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "age", 25)

		val, err := db.Incr("person", 10)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0)
	})
}

func TestIncrFloat(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		val, err := db.IncrFloat("pi", 3.14)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 3.14)

		pi, _ := db.Get("pi")
		testx.AssertEqual(t, pi.MustFloat(), 3.14)
	})
	t.Run("increment", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("pi", "3.14")

		val, err := db.IncrFloat("pi", 1.86)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 5.0)

		pi, _ := db.Get("pi")
		testx.AssertEqual(t, pi.MustFloat(), 5.0)
	})

	t.Run("decrement", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("pi", "3.14")

		val, err := db.IncrFloat("pi", -1.14)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 2.0)

		pi, _ := db.Get("pi")
		testx.AssertEqual(t, pi.MustFloat(), 2.0)
	})
	t.Run("invalid float", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		val, err := db.IncrFloat("name", 1.5)
		testx.AssertErr(t, err, core.ErrValueType)
		testx.AssertEqual(t, val, 0.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "age", 25.5)

		val, err := db.IncrFloat("person", 10.5)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0.0)
	})
}

func TestSet(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		tests := []struct {
			name  string
			key   string
			value any
			want  any
		}{
			{"string", "name", "alice", core.Value("alice")},
			{"empty string", "empty", "", core.Value("")},
			{"int", "age", 25, core.Value("25")},
			{"float", "pi", 3.14, core.Value("3.14")},
			{"bool true", "ok", true, core.Value("1")},
			{"bool false", "ok", false, core.Value("0")},
			{"bytes", "bytes", []byte("hello"), core.Value("hello")},
		}
		for _, test := range tests {
			err := db.Set(test.key, test.value)
			testx.AssertNoErr(t, err)

			val, _ := db.Get(test.key)
			testx.AssertEqual(t, val, test.want)

			key, _ := red.Key().Get(test.key)
			testx.AssertEqual(t, key.ETime, (*int64)(nil))
		}
	})
	t.Run("struct", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		err := db.Set("struct", struct{ Name string }{"alice"})
		testx.AssertErr(t, err, core.ErrValueType)
	})
	t.Run("nil", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		err := db.Set("nil", nil)
		testx.AssertErr(t, err, core.ErrValueType)
	})
	t.Run("update", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		err := db.Set("name", "bob")
		testx.AssertNoErr(t, err)
		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("bob"))
	})
	t.Run("change value type", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		err := db.Set("name", true)
		testx.AssertNoErr(t, err)
		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("1"))
	})
	t.Run("not changed", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		err := db.Set("name", "alice")
		testx.AssertNoErr(t, err)
		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("alice"))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")

		err := db.Set("person", "name")
		testx.AssertErr(t, err, core.ErrKeyType)

		_, err = db.Get("person")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestSetExists(t *testing.T) {
	t.Run("key exists", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		out, err := db.SetWith("name", "bob").IfExists().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, true)

		name, _ := db.Get("name")
		testx.AssertEqual(t, name, core.Value("bob"))

		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		out, err := db.SetWith("name", "alice").IfExists().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, false)

		_, err = db.Get("name")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
	t.Run("with ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		now := time.Now()
		ttl := time.Second
		out, err := db.SetWith("name", "cindy").IfExists().TTL(ttl).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, true)

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")

		out, err := db.SetWith("person", "name").IfExists().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, false)

		_, err = db.Get("person")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestSetExpires(t *testing.T) {
	t.Run("no ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, err := db.SetWith("name", "alice").TTL(0).Run()
		testx.AssertNoErr(t, err)

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("alice"))

		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		now := time.Now()
		ttl := time.Second
		_, err := db.SetWith("name", "alice").TTL(ttl).Run()
		testx.AssertNoErr(t, err)

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, core.Value("alice"))

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")
		_, err := db.SetWith("person", "name").TTL(time.Second).Run()
		testx.AssertErr(t, err, core.ErrKeyType)

		_, err = db.Get("person")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestSetMany(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		err := db.SetMany(map[string]any{
			"name": "alice",
			"age":  25,
		})
		testx.AssertNoErr(t, err)
		name, _ := db.Get("name")
		testx.AssertEqual(t, name, core.Value("alice"))
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, core.Value("25"))
	})
	t.Run("update", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = db.Set("name", "alice")
		_ = db.Set("age", 25)

		err := db.SetMany(map[string]any{
			"name": "bob",
			"age":  50,
		})
		testx.AssertNoErr(t, err)
		name, _ := db.Get("name")
		testx.AssertEqual(t, name, core.Value("bob"))
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, core.Value("50"))
	})
	t.Run("invalid type", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		err := db.SetMany(map[string]any{
			"name": "alice",
			"age":  struct{ Name string }{"alice"},
		})
		testx.AssertErr(t, err, core.ErrValueType)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")

		err := db.SetMany(map[string]any{
			"name":   "alice",
			"person": "alice",
		})
		testx.AssertErr(t, err, core.ErrKeyType)

		_, err = db.Get("name")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.Get("person")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestSetManyNX(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		ok, err := db.SetManyNX(map[string]any{
			"age":  25,
			"city": "paris",
		})
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, core.Value("25"))
		city, _ := db.Get("city")
		testx.AssertEqual(t, city, core.Value("paris"))
	})
	t.Run("update", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = db.Set("age", 25)
		_ = db.Set("city", "paris")
		ok, err := db.SetManyNX(map[string]any{
			"age":  50,
			"city": "berlin",
		})
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, core.Value("25"))
		city, _ := db.Get("city")
		testx.AssertEqual(t, city, core.Value("paris"))
	})
	t.Run("invalid type", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		ok, err := db.SetManyNX(map[string]any{
			"name": "alice",
			"age":  struct{ Name string }{"alice"},
		})
		testx.AssertErr(t, err, core.ErrValueType)
		testx.AssertEqual(t, ok, false)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")

		ok, err := db.SetManyNX(map[string]any{
			"name":   "alice",
			"person": "alice",
		})
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, ok, false)

		_, err = db.Get("name")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = db.Get("person")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestSetNotExists(t *testing.T) {
	t.Run("key exists", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = db.Set("name", "alice")

		out, err := db.SetWith("name", "bob").IfNotExists().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, false)

		name, _ := db.Get("name")
		testx.AssertEqual(t, name, core.Value("alice"))
	})
	t.Run("key not found", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		out, err := db.SetWith("name", "alice").IfNotExists().Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Created, true)
		testx.AssertEqual(t, out.Updated, false)

		name, _ := db.Get("name")
		testx.AssertEqual(t, name, core.Value("alice"))
	})
	t.Run("with ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		now := time.Now()
		ttl := time.Second
		out, err := db.SetWith("city", "paris").IfNotExists().TTL(ttl).Run()
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, out.Created, true)
		testx.AssertEqual(t, out.Updated, false)

		key, _ := red.Key().Get("city")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		testx.AssertEqual(t, got, want)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")

		out, err := db.SetWith("person", "name").IfNotExists().Run()
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, out.Created, false)
		testx.AssertEqual(t, out.Updated, false)

		_, err = db.Get("person")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func getDB(tb testing.TB) (*redka.DB, *rstring.DB) {
	tb.Helper()
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db, db.Str()
}
