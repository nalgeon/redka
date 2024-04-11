package rstring_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/testx"
)

func TestGet(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = db.Set("name", "alice")

	tests := []struct {
		name string
		key  string
		want any
	}{
		{"key found", "name", redka.Value("alice")},
		{"key not found", "key1", redka.Value(nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, err := db.Get(test.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val, test.want)
		})
	}
}

func TestGetMany(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_ = db.Set("name", "alice")
	_ = db.Set("age", 25)

	tests := []struct {
		name string
		keys []string
		want map[string]redka.Value
	}{
		{"all found", []string{"name", "age"},
			map[string]redka.Value{
				"name": redka.Value("alice"), "age": redka.Value("25"),
			},
		},
		{"some found", []string{"name", "key1"},
			map[string]redka.Value{
				"name": redka.Value("alice"), "key1": redka.Value(nil),
			},
		},
		{"none found", []string{"key1", "key2"},
			map[string]redka.Value{
				"key1": redka.Value(nil), "key2": redka.Value(nil),
			},
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

func TestSet(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := db.Set(test.key, test.value)
			testx.AssertNoErr(t, err)

			val, _ := db.Get(test.key)
			testx.AssertEqual(t, val, test.want)

			key, _ := red.Key().Get(test.key)
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
	t.Run("change value type", func(t *testing.T) {
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
	t.Run("key type mismatch", func(t *testing.T) {
		_, _ = red.Hash().Set("person", "name", "alice")
		err := db.Set("person", "name")
		testx.AssertErr(t, err, redka.ErrKeyTypeMismatch)
	})
}

func TestSetExpires(t *testing.T) {
	t.Run("no ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		err := db.SetExpires("name", "alice", 0)
		testx.AssertNoErr(t, err)

		val, _ := db.Get("name")
		testx.AssertEqual(t, val, redka.Value("alice"))

		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

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
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")
		err := db.SetExpires("person", "name", time.Second)
		testx.AssertErr(t, err, redka.ErrKeyTypeMismatch)
	})
}

func TestSetNotExists(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok, err := db.SetNotExists(test.key, test.value, 0)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, ok, test.want)

			key, _ := red.Key().Get(test.key)
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
	t.Run("key type mismatch", func(t *testing.T) {
		_, _ = red.Hash().Set("person", "name", "alice")
		ok, err := db.SetNotExists("person", "name", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
	})
}

func TestSetExists(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok, err := db.SetExists(test.key, test.value, 0)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, ok, test.want)

			key, _ := red.Key().Get(test.key)
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
	t.Run("key type mismatch", func(t *testing.T) {
		_, _ = red.Hash().Set("person", "name", "alice")
		ok, err := db.SetExists("person", "name", 0)
		testx.AssertErr(t, err, redka.ErrKeyTypeMismatch)
		testx.AssertEqual(t, ok, false)
	})
}

func TestGetSet(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		val, err := db.GetSet("name", "alice", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, redka.Value(nil))
		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("update key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = db.Set("name", "alice")
		val, err := db.GetSet("name", "bob", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, redka.Value("alice"))
		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("not changed", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_ = db.Set("name", "alice")
		val, err := db.GetSet("name", "alice", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, redka.Value("alice"))
		key, _ := red.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

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
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")
		val, err := db.GetSet("person", "name", 0)
		testx.AssertErr(t, err, redka.ErrKeyTypeMismatch)
		testx.AssertEqual(t, val, redka.Value(nil))
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
		testx.AssertEqual(t, name, redka.Value("alice"))
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, redka.Value("25"))
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
		testx.AssertEqual(t, name, redka.Value("bob"))
		age, _ := db.Get("age")
		testx.AssertEqual(t, age, redka.Value("50"))
	})
	t.Run("invalid type", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		err := db.SetMany(map[string]any{
			"name": "alice",
			"age":  struct{ Name string }{"alice"},
		})
		testx.AssertErr(t, err, redka.ErrInvalidType)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_, _ = red.Hash().Set("person", "name", "alice")
		err := db.SetMany(map[string]any{
			"name":   "alice",
			"person": "alice",
		})
		testx.AssertErr(t, err, redka.ErrKeyTypeMismatch)
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
		testx.AssertEqual(t, age, redka.Value("25"))
		city, _ := db.Get("city")
		testx.AssertEqual(t, city, redka.Value("paris"))
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
		testx.AssertEqual(t, age, redka.Value("25"))
		city, _ := db.Get("city")
		testx.AssertEqual(t, city, redka.Value("paris"))
	})
	t.Run("invalid type", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		ok, err := db.SetManyNX(map[string]any{
			"name": "alice",
			"age":  struct{ Name string }{"alice"},
		})
		testx.AssertErr(t, err, redka.ErrInvalidType)
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
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
	})
}

func TestIncr(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, err := db.Incr(test.key, test.value)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val, test.want)
		})
	}
	t.Run("invalid int", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.Incr("name", 1)
		testx.AssertErr(t, err, redka.ErrInvalidType)
		testx.AssertEqual(t, val, 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		_, _ = red.Hash().Set("person", "age", 25)
		val, err := db.Incr("person", 10)
		testx.AssertErr(t, err, redka.ErrKeyTypeMismatch)
		testx.AssertEqual(t, val, 0)
	})
}

func TestIncrFloat(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, err := db.IncrFloat(test.key, test.value)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val, test.want)
		})
	}
	t.Run("invalid float", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.IncrFloat("name", 1.5)
		testx.AssertErr(t, err, redka.ErrInvalidType)
		testx.AssertEqual(t, val, 0.0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		_, _ = red.Hash().Set("person", "age", 25.5)
		val, err := db.IncrFloat("person", 10.5)
		testx.AssertErr(t, err, redka.ErrKeyTypeMismatch)
		testx.AssertEqual(t, val, 0.0)
	})
}

func getDB(tb testing.TB) (*redka.DB, redka.Strings) {
	tb.Helper()
	db, err := redka.Open(":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	return db, db.Str()
}
