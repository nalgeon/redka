package rhash_test

import (
	"slices"
	"sort"
	"testing"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDelete(t *testing.T) {
	t.Run("some", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		_, _ = hash.Set("person", "age", 25)
		_, _ = hash.Set("person", "city", "paris")

		n, err := hash.Delete("person", "name", "city")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 2)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 4)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)

		exist, _ := hash.Exists("person", "name")
		testx.AssertEqual(t, exist, false)
		exist, _ = hash.Exists("person", "age")
		testx.AssertEqual(t, exist, true)
		exist, _ = hash.Exists("person", "city")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("all", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		_, _ = hash.Set("person", "age", 25)
		_, _ = hash.Set("person", "city", "paris")

		n, err := hash.Delete("person", "name", "age", "city")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 3)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 4)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 0)

		exist, _ := hash.Exists("person", "name")
		testx.AssertEqual(t, exist, false)
		exist, _ = hash.Exists("person", "age")
		testx.AssertEqual(t, exist, false)
		exist, _ = hash.Exists("person", "city")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("none", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		_, _ = hash.Set("person", "age", 25)
		_, _ = hash.Set("person", "city", "paris")

		n, err := hash.Delete("person", "country", "street")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 3)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 3)

		name, _ := hash.Get("person", "name")
		testx.AssertEqual(t, name.String(), "alice")
		age, _ := hash.Get("person", "age")
		testx.AssertEqual(t, age.String(), "25")
		city, _ := hash.Get("person", "city")
		testx.AssertEqual(t, city.String(), "paris")
	})
	t.Run("key not found", func(t *testing.T) {
		db, hash := getDB(t)

		n, err := hash.Delete("person")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)

		exist, _ := db.Key().Exists("person")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)
		_ = db.Str().Set("person", "alice")
		n, err := hash.Delete("person", "name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, n, 0)
	})
}

func TestExists(t *testing.T) {
	db, hash := getDB(t)

	_, _ = hash.Set("person", "name", "alice")
	_ = db.Str().Set("str", "str")

	tests := []struct {
		name  string
		key   string
		field string
		want  bool
	}{
		{"field found", "person", "name", true},
		{"field not found", "person", "age", false},
		{"key not found", "pet", "name", false},
		{"key type mismatch", "str", "str", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exists, err := hash.Exists(test.key, test.field)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, exists, test.want)
		})
	}
}

func TestFields(t *testing.T) {
	db, hash := getDB(t)

	_, _ = hash.Set("person", "name", "alice")
	_, _ = hash.Set("person", "age", 25)
	_, _ = hash.Set("pet", "name", "doggo")
	_ = db.Str().Set("str", "str")

	tests := []struct {
		name   string
		key    string
		fields []string
	}{
		{"multiple fields", "person", []string{"name", "age"}},
		{"single field", "pet", []string{"name"}},
		{"key not found", "robot", []string{}},
		{"key type mismatch", "str", []string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields, err := hash.Fields(test.key)
			testx.AssertNoErr(t, err)
			slices.Sort(fields)
			slices.Sort(test.fields)
			testx.AssertEqual(t, fields, test.fields)
		})
	}
}

func TestGet(t *testing.T) {
	t.Run("field found", func(t *testing.T) {
		_, hash := getDB(t)
		_, _ = hash.Set("person", "name", "alice")
		val, err := hash.Get("person", "name")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, core.Value("alice"))
	})
	t.Run("field not found", func(t *testing.T) {
		_, hash := getDB(t)
		_, _ = hash.Set("person", "name", "alice")
		val, err := hash.Get("person", "age")
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, val, core.Value(nil))
	})
	t.Run("key not found", func(t *testing.T) {
		_, hash := getDB(t)
		val, err := hash.Get("person", "name")
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, val, core.Value(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)
		_ = db.Str().Set("person", "name")
		val, err := hash.Get("person", "name")
		testx.AssertErr(t, err, core.ErrNotFound)
		testx.AssertEqual(t, val, core.Value(nil))
	})
}

func TestGetMany(t *testing.T) {
	db, hash := getDB(t)

	_, _ = hash.Set("person", "name", "alice")
	_, _ = hash.Set("person", "age", 25)
	_ = db.Str().Set("str", "name")

	tests := []struct {
		name   string
		key    string
		fields []string
		want   map[string]core.Value
	}{
		{"all found", "person", []string{"name", "age"},
			map[string]core.Value{"name": core.Value("alice"), "age": core.Value("25")},
		},
		{"some found", "person", []string{"name", "city"},
			map[string]core.Value{"name": core.Value("alice")},
		},
		{"none found", "person", []string{"key1", "key2"},
			map[string]core.Value{},
		},
		{"key not found", "pet", []string{"name", "age"},
			map[string]core.Value{},
		},
		{"key type mismatch", "str", []string{"name"},
			map[string]core.Value{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vals, err := hash.GetMany(test.key, test.fields...)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, vals, test.want)
		})
	}
}

func TestIncr(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, hash := getDB(t)

		val, err := hash.Incr("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 1)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)
	})
	t.Run("create field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		val, err := hash.Incr("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 2)
	})
	t.Run("update field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "age", 25)
		val, err := hash.Incr("person", "age", 10)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 35)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)
	})
	t.Run("decrement", func(t *testing.T) {
		_, hash := getDB(t)

		_, _ = hash.Set("person", "age", 25)
		val, err := hash.Incr("person", "age", -10)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 15)
	})
	t.Run("non-integer value", func(t *testing.T) {
		_, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		_, err := hash.Incr("person", "name", 10)
		testx.AssertErr(t, err, core.ErrValueType)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)
		_ = db.Str().Set("person", "alice")

		val, err := hash.Incr("person", "age", 25)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0)

		_, err = hash.Get("person", "age")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestIncrFloat(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, hash := getDB(t)

		val, err := hash.IncrFloat("person", "age", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 1)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)
	})
	t.Run("create field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		val, err := hash.IncrFloat("person", "age", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 2)
	})
	t.Run("update field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "age", 25.5)
		val, err := hash.IncrFloat("person", "age", 10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 36.0)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)
	})
	t.Run("decrement", func(t *testing.T) {
		_, hash := getDB(t)

		_, _ = hash.Set("person", "age", 25.5)
		val, err := hash.IncrFloat("person", "age", -10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 15.0)
	})
	t.Run("non-float value", func(t *testing.T) {
		_, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		_, err := hash.IncrFloat("person", "name", 10.5)
		testx.AssertErr(t, err, core.ErrValueType)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)

		_ = db.Str().Set("person", "alice")
		val, err := hash.IncrFloat("person", "age", 25.0)

		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0.0)

		_, err = hash.Get("person", "age")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestItems(t *testing.T) {
	db, hash := getDB(t)

	_, _ = hash.Set("person", "name", "alice")
	_, _ = hash.Set("person", "age", 25)
	_, _ = hash.Set("pet", "name", "doggo")
	_ = db.Str().Set("str", "str")

	tests := []struct {
		name  string
		key   string
		items map[string]core.Value
	}{
		{"multiple item", "person", map[string]core.Value{
			"name": core.Value("alice"), "age": core.Value("25"),
		}},
		{"single item", "pet", map[string]core.Value{
			"name": core.Value("doggo"),
		}},
		{"key not found", "robot", map[string]core.Value{}},
		{"key type mismatch", "str", map[string]core.Value{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			items, err := hash.Items(test.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		})
	}
}

func TestLen(t *testing.T) {
	db, hash := getDB(t)

	_, _ = hash.Set("person", "name", "alice")
	_, _ = hash.Set("person", "age", 25)
	_, _ = hash.Set("pet", "name", "doggo")
	_ = db.Str().Set("str", "str")

	tests := []struct {
		name string
		key  string
		want int
	}{
		{"multiple fields", "person", 2},
		{"single field", "pet", 1},
		{"key not found", "robot", 0},
		{"key type mismatch", "str", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count, err := hash.Len(test.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, test.want)
		})
	}
}

func TestScan(t *testing.T) {
	db, hash := getDB(t)

	_, _ = hash.Set("key", "f11", "11")
	_, _ = hash.Set("key", "f12", "12")
	_, _ = hash.Set("key", "f21", "21")
	_, _ = hash.Set("key", "f22", "22")
	_, _ = hash.Set("key", "f31", "31")
	_ = db.Str().Set("str", "str")

	tests := []struct {
		name    string
		cursor  int
		pattern string
		count   int

		wantCursor int
		wantItems  []rhash.HashItem
	}{
		{"all", 0, "*", 0, 5,
			[]rhash.HashItem{
				{Field: "f11", Value: core.Value("11")},
				{Field: "f12", Value: core.Value("12")},
				{Field: "f21", Value: core.Value("21")},
				{Field: "f22", Value: core.Value("22")},
				{Field: "f31", Value: core.Value("31")},
			},
		},
		{"some", 0, "f2*", 10, 4,
			[]rhash.HashItem{
				{Field: "f21", Value: core.Value("21")},
				{Field: "f22", Value: core.Value("22")},
			},
		},
		{"none", 0, "n*", 10, 0, []rhash.HashItem(nil)},
		{"cursor 1st", 0, "*", 2, 2,
			[]rhash.HashItem{
				{Field: "f11", Value: core.Value("11")},
				{Field: "f12", Value: core.Value("12")},
			},
		},
		{"cursor 2nd", 2, "*", 2, 4,
			[]rhash.HashItem{
				{Field: "f21", Value: core.Value("21")},
				{Field: "f22", Value: core.Value("22")},
			},
		},
		{"cursor 3rd", 4, "*", 2, 5,
			[]rhash.HashItem{
				{Field: "f31", Value: core.Value("31")},
			},
		},
		{"exhausted", 6, "*", 2, 0, []rhash.HashItem(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := hash.Scan("key", test.cursor, test.pattern, test.count)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, out.Cursor, test.wantCursor)
			for i, item := range out.Items {
				testx.AssertEqual(t, item.Field, test.wantItems[i].Field)
				testx.AssertEqual(t, item.Value, test.wantItems[i].Value)
			}
		})
	}
	t.Run("ignore other keys", func(t *testing.T) {
		_, _ = hash.Set("person", "name", "alice")
		_, _ = hash.Set("pet", "name", "doggo")

		out, err := hash.Scan("person", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 1)
		testx.AssertEqual(t, out.Items[0].Field, "name")
		testx.AssertEqual(t, out.Items[0].Value.String(), "alice")
	})
	t.Run("key not found", func(t *testing.T) {
		out, err := hash.Scan("not", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 0)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		out, err := hash.Scan("str", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 0)
	})
}

func TestScanner(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("key", "f11", "11")
		_, _ = hash.Set("key", "f12", "12")
		_, _ = hash.Set("key", "f21", "21")
		_, _ = hash.Set("key", "f22", "22")
		_, _ = hash.Set("key", "f31", "31")

		var items []rhash.HashItem
		err := db.View(func(tx *redka.Tx) error {
			sc := tx.Hash().Scanner("key", "*", 2)
			for sc.Scan() {
				items = append(items, sc.Item())
			}
			return sc.Err()
		})

		testx.AssertNoErr(t, err)
		fields := make([]string, len(items))
		vals := make([]string, len(items))

		for i, it := range items {
			fields[i] = it.Field
			vals[i] = it.Value.String()
		}
		testx.AssertEqual(t, fields, []string{"f11", "f12", "f21", "f22", "f31"})
		testx.AssertEqual(t, vals, []string{"11", "12", "21", "22", "31"})
	})
	t.Run("key not found", func(t *testing.T) {
		_, hash := getDB(t)

		sc := hash.Scanner("not", "*", 2)
		var items []rhash.HashItem
		for sc.Scan() {
			items = append(items, sc.Item())
		}

		testx.AssertNoErr(t, sc.Err())
		testx.AssertEqual(t, items, []rhash.HashItem(nil))
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)
		_ = db.Str().Set("key", "str")

		sc := hash.Scanner("key", "*", 2)
		var items []rhash.HashItem
		for sc.Scan() {
			items = append(items, sc.Item())
		}

		testx.AssertNoErr(t, sc.Err())
		testx.AssertEqual(t, items, []rhash.HashItem(nil))
	})
}

func TestSet(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, hash := getDB(t)

		created, err := hash.Set("person", "name", "alice")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 1)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)

		val, _ := hash.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("create field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		created, err := hash.Set("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 2)

		val, _ := hash.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
	})
	t.Run("update field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		created, err := hash.Set("person", "name", "bob")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, false)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)

		val, _ := hash.Get("person", "name")
		testx.AssertEqual(t, val.String(), "bob")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)
		_ = db.Str().Set("person", "alice")

		ok, err := hash.Set("person", "name", "alice")
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, ok, false)

		_, err = hash.Get("person", "name")
		testx.AssertErr(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("person")
		testx.AssertEqual(t, sval.String(), "alice")
	})
}

func TestSetMany(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, hash := getDB(t)

		fvals := map[string]any{
			"name": "alice",
			"age":  25,
		}
		count, err := hash.SetMany("person", fvals)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 2)

		var val core.Value
		val, _ = hash.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
		val, _ = hash.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
	})
	t.Run("create fields", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		fvals := map[string]any{
			"age":  25,
			"city": "paris",
		}
		count, err := hash.SetMany("person", fvals)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 3)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 3)

		var val core.Value
		val, _ = hash.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
		val, _ = hash.Get("person", "city")
		testx.AssertEqual(t, val.String(), "paris")
	})
	t.Run("update fields", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		fvals := map[string]any{
			"name": "bob",
			"age":  50,
		}
		count, err := hash.SetMany("person", fvals)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 3)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 2)

		var val core.Value
		val, _ = hash.Get("person", "name")
		testx.AssertEqual(t, val.String(), "bob")
		val, _ = hash.Get("person", "age")
		testx.AssertEqual(t, val.String(), "50")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)
		_ = db.Str().Set("person", "alice")

		count, err := hash.SetMany("person", map[string]any{
			"name": "alice", "age": 25,
		})
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, count, 0)

		_, err = hash.Get("person", "name")
		testx.AssertErr(t, err, core.ErrNotFound)
		_, err = hash.Get("person", "age")
		testx.AssertErr(t, err, core.ErrNotFound)

		sval, _ := db.Str().Get("person")
		testx.AssertEqual(t, sval.String(), "alice")
	})
}

func TestSetNotExists(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		db, hash := getDB(t)

		ok, err := hash.SetNotExists("person", "name", "alice")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 1)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)

		val, _ := hash.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("create field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		ok, err := hash.SetNotExists("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 2)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 2)

		val, _ := hash.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
	})
	t.Run("update field", func(t *testing.T) {
		db, hash := getDB(t)

		_, _ = hash.Set("person", "name", "alice")
		ok, err := hash.SetNotExists("person", "name", "bob")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)

		key, _ := db.Key().Get("person")
		testx.AssertEqual(t, key.Version, 1)

		hlen, _ := hash.Len("person")
		testx.AssertEqual(t, hlen, 1)

		val, _ := hash.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		db, hash := getDB(t)
		_ = db.Str().Set("person", "alice")

		ok, err := hash.SetNotExists("person", "name", "alice")
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, ok, false)

		_, err = hash.Get("person", "name")
		testx.AssertErr(t, err, core.ErrNotFound)
	})
}

func TestValues(t *testing.T) {
	db, hash := getDB(t)

	_, _ = hash.Set("person", "name", "alice")
	_, _ = hash.Set("person", "age", 25)
	_, _ = hash.Set("pet", "name", "doggo")
	_ = db.Str().Set("str", "str")

	tests := []struct {
		name string
		key  string
		vals []core.Value
	}{
		{"multiple fields", "person", []core.Value{
			core.Value("alice"), core.Value("25"),
		}},
		{"single field", "pet", []core.Value{
			core.Value("doggo"),
		}},
		{"key not found", "robot", []core.Value{}},
		{"key type mismatch", "str", []core.Value{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values, err := hash.Values(test.key)
			testx.AssertNoErr(t, err)
			sort.Slice(values, func(i, j int) bool {
				return values[i].String() < values[j].String()
			})
			sort.Slice(test.vals, func(i, j int) bool {
				return test.vals[i].String() < test.vals[j].String()
			})
			testx.AssertEqual(t, values, test.vals)
		})
	}
}

func getDB(tb testing.TB) (*redka.DB, *rhash.DB) {
	tb.Helper()
	db := testx.OpenDB(tb)
	return db, db.Hash()
}
