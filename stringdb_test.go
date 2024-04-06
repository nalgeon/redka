package redka_test

import (
	"testing"
	"time"

	"github.com/nalgeon/redka"
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
			assertNoErr(t, err)
			assertEqual(t, val, tt.want)
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
			assertNoErr(t, err)
			assertEqual(t, vals, tt.want)
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
			assertNoErr(t, err)

			val, _ := db.Get(tt.key)
			assertEqual(t, val, tt.want)

			key, _ := red.Key().Get(tt.key)
			assertEqual(t, key.ETime, (*int64)(nil))
		})
	}
	t.Run("struct", func(t *testing.T) {
		err := db.Set("struct", struct{ Name string }{"alice"})
		assertErr(t, err, redka.ErrInvalidType)
	})
	t.Run("nil", func(t *testing.T) {
		err := db.Set("nil", nil)
		assertErr(t, err, redka.ErrInvalidType)
	})
	t.Run("update", func(t *testing.T) {
		_ = db.Set("name", "alice")
		err := db.Set("name", "bob")
		assertNoErr(t, err)
		val, _ := db.Get("name")
		assertEqual(t, val, redka.Value("bob"))
	})
	t.Run("change type", func(t *testing.T) {
		_ = db.Set("name", "alice")
		err := db.Set("name", true)
		assertNoErr(t, err)
		val, _ := db.Get("name")
		assertEqual(t, val, redka.Value("1"))
	})
	t.Run("not changed", func(t *testing.T) {
		_ = db.Set("name", "alice")
		err := db.Set("name", "alice")
		assertNoErr(t, err)
		val, _ := db.Get("name")
		assertEqual(t, val, redka.Value("alice"))
	})
}

func TestStringSetExpires(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()
	t.Run("no ttl", func(t *testing.T) {
		err := db.SetExpires("name", "alice", 0)
		assertNoErr(t, err)

		val, _ := db.Get("name")
		assertEqual(t, val, redka.Value("alice"))

		key, _ := red.Key().Get("name")
		assertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		now := time.Now()
		ttl := time.Second
		err := db.SetExpires("name", "alice", ttl)
		assertNoErr(t, err)

		val, _ := db.Get("name")
		assertEqual(t, val, redka.Value("alice"))

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		assertEqual(t, got, want)
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
			assertNoErr(t, err)
			assertEqual(t, ok, tt.want)

			key, _ := red.Key().Get(tt.key)
			assertEqual(t, key.ETime, (*int64)(nil))
		})
	}
	t.Run("with ttl", func(t *testing.T) {
		now := time.Now()
		ttl := time.Second
		ok, err := db.SetNotExists("city", "paris", ttl)
		assertNoErr(t, err)
		assertEqual(t, ok, true)

		key, _ := red.Key().Get("city")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		assertEqual(t, got, want)
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
			assertNoErr(t, err)
			assertEqual(t, ok, tt.want)

			key, _ := red.Key().Get(tt.key)
			assertEqual(t, key.ETime, (*int64)(nil))
		})
	}
	t.Run("with ttl", func(t *testing.T) {
		now := time.Now()
		ttl := time.Second
		ok, err := db.SetExists("name", "cindy", ttl)
		assertNoErr(t, err)
		assertEqual(t, ok, true)

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		assertEqual(t, got, want)
	})
}

func TestStringGetSet(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.Str()

	t.Run("create key", func(t *testing.T) {
		val, err := db.GetSet("name", "alice", 0)
		assertNoErr(t, err)
		assertEqual(t, val, redka.Value(nil))
		key, _ := red.Key().Get("name")
		assertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("update key", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.GetSet("name", "bob", 0)
		assertNoErr(t, err)
		assertEqual(t, val, redka.Value("alice"))
		key, _ := red.Key().Get("name")
		assertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("not changed", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.GetSet("name", "alice", 0)
		assertNoErr(t, err)
		assertEqual(t, val, redka.Value("alice"))
		key, _ := red.Key().Get("name")
		assertEqual(t, key.ETime, (*int64)(nil))
	})
	t.Run("with ttl", func(t *testing.T) {
		_ = db.Set("name", "alice")

		now := time.Now()
		ttl := time.Second
		val, err := db.GetSet("name", "bob", ttl)
		assertNoErr(t, err)
		assertEqual(t, val, redka.Value("alice"))

		key, _ := red.Key().Get("name")
		got := (*key.ETime) / 1000
		want := now.Add(ttl).UnixMilli() / 1000
		assertEqual(t, got, want)
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
		assertNoErr(t, err)
		name, _ := db.Get("name")
		assertEqual(t, name, redka.Value("alice"))
		age, _ := db.Get("age")
		assertEqual(t, age, redka.Value("25"))
	})
	t.Run("update", func(t *testing.T) {
		_ = db.Set("name", "alice")
		_ = db.Set("age", 25)
		err := db.SetMany(
			redka.KeyValue{Key: "name", Value: "bob"},
			redka.KeyValue{Key: "age", Value: 50},
		)
		assertNoErr(t, err)
		name, _ := db.Get("name")
		assertEqual(t, name, redka.Value("bob"))
		age, _ := db.Get("age")
		assertEqual(t, age, redka.Value("50"))
	})
	t.Run("invalid type", func(t *testing.T) {
		err := db.SetMany(
			redka.KeyValue{Key: "name", Value: "alice"},
			redka.KeyValue{Key: "age", Value: struct{ Name string }{"alice"}},
		)
		assertErr(t, err, redka.ErrInvalidType)
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
		assertNoErr(t, err)
		assertEqual(t, ok, true)
		age, _ := db.Get("age")
		assertEqual(t, age, redka.Value("25"))
		city, _ := db.Get("city")
		assertEqual(t, city, redka.Value("wonderland"))
	})
	t.Run("update", func(t *testing.T) {
		_ = db.Set("age", 25)
		_ = db.Set("city", "wonderland")
		ok, err := db.SetManyNX(
			redka.KeyValue{Key: "age", Value: 50},
			redka.KeyValue{Key: "city", Value: "wonderland"},
		)
		assertNoErr(t, err)
		assertEqual(t, ok, false)
		age, _ := db.Get("age")
		assertEqual(t, age, redka.Value("25"))
		city, _ := db.Get("city")
		assertEqual(t, city, redka.Value("wonderland"))
	})
	t.Run("invalid type", func(t *testing.T) {
		ok, err := db.SetManyNX(
			redka.KeyValue{Key: "name", Value: "alice"},
			redka.KeyValue{Key: "age", Value: struct{ Name string }{"alice"}},
		)
		assertErr(t, err, redka.ErrInvalidType)
		assertEqual(t, ok, false)
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
			assertNoErr(t, err)
			assertEqual(t, n, tt.want)
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
			assertNoErr(t, err)
			assertEqual(t, val.String(), tt.want)
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
			assertNoErr(t, err)
			assertEqual(t, n, len(tt.want))
			val, _ := db.Get(tt.key)
			assertEqual(t, val.Bytes(), tt.want)
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
			assertNoErr(t, err)
			assertEqual(t, n, len(tt.want))
			val, _ := db.Get(tt.key)
			assertEqual(t, val, redka.Value(tt.want))
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
			assertNoErr(t, err)
			assertEqual(t, val, tt.want)
		})
	}
	t.Run("invalid int", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.Incr("name", 1)
		assertErr(t, err, redka.ErrInvalidInt)
		assertEqual(t, val, 0)
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
			assertNoErr(t, err)
			assertEqual(t, val, tt.want)
		})
	}
	t.Run("invalid float", func(t *testing.T) {
		_ = db.Set("name", "alice")
		val, err := db.IncrFloat("name", 1.5)
		assertErr(t, err, redka.ErrInvalidFloat)
		assertEqual(t, val, 0.0)
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
			assertNoErr(t, err)
			assertEqual(t, count, tt.want)
			for _, key := range tt.keys {
				val, _ := db.Get(key)
				assertEqual(t, val.IsEmpty(), true)
			}
		})
	}
}
