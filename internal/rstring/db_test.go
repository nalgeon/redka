package rstring_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/testx"
)

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
		err := db.SetMany(map[string]any{
			"name": "alice",
			"age":  struct{ Name string }{"alice"},
		})
		testx.AssertErr(t, err, redka.ErrInvalidType)
	})
}

func TestStringSetManyNX(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	_ = db.Set("name", "alice")

	t.Run("create", func(t *testing.T) {
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
		ok, err := db.SetManyNX(map[string]any{
			"name": "alice",
			"age":  struct{ Name string }{"alice"},
		})
		testx.AssertErr(t, err, redka.ErrInvalidType)
		testx.AssertEqual(t, ok, false)
	})
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

func getDB(tb testing.TB) *redka.DB {
	tb.Helper()
	db, err := redka.Open(":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	return db
}
