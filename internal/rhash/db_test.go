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

func TestGet(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("person", "name", "alice")

	tests := []struct {
		name  string
		key   string
		field string
		want  any
	}{
		{"field found", "person", "name", core.Value("alice")},
		{"field not found", "person", "age", core.Value(nil)},
		{"key not found", "pet", "name", core.Value(nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, err := db.Get(test.key, test.field)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, val, test.want)
		})
	}
}

func TestGetMany(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("person", "name", "alice")
	_, _ = db.Set("person", "age", 25)

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
			map[string]core.Value{"name": core.Value("alice"), "city": core.Value(nil)},
		},
		{"none found", "person", []string{"key1", "key2"},
			map[string]core.Value{"key1": core.Value(nil), "key2": core.Value(nil)},
		},
		{"key not found", "pet", []string{"name", "age"},
			map[string]core.Value{"name": core.Value(nil), "age": core.Value(nil)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vals, err := db.GetMany(test.key, test.fields...)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, vals, test.want)
		})
	}
}

func TestExists(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("person", "name", "alice")

	tests := []struct {
		name  string
		key   string
		field string
		want  bool
	}{
		{"field found", "person", "name", true},
		{"field not found", "person", "age", false},
		{"key not found", "pet", "name", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exists, err := db.Exists(test.key, test.field)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, exists, test.want)
		})
	}
}

func TestItems(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("person", "name", "alice")
	_, _ = db.Set("person", "age", 25)
	_, _ = db.Set("pet", "name", "doggo")

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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			items, err := db.Items(test.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, items, test.items)
		})
	}
}

func TestFields(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("person", "name", "alice")
	_, _ = db.Set("person", "age", 25)
	_, _ = db.Set("pet", "name", "doggo")

	tests := []struct {
		name   string
		key    string
		fields []string
	}{
		{"multiple fields", "person", []string{"name", "age"}},
		{"single field", "pet", []string{"name"}},
		{"key not found", "robot", []string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields, err := db.Fields(test.key)
			testx.AssertNoErr(t, err)
			slices.Sort(fields)
			slices.Sort(test.fields)
			testx.AssertEqual(t, fields, test.fields)
		})
	}
}

func TestValues(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("person", "name", "alice")
	_, _ = db.Set("person", "age", 25)
	_, _ = db.Set("pet", "name", "doggo")

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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values, err := db.Values(test.key)
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

func TestScan(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("key", "f11", "11")
	_, _ = db.Set("key", "f12", "12")
	_, _ = db.Set("key", "f21", "21")
	_, _ = db.Set("key", "f22", "22")
	_, _ = db.Set("key", "f31", "31")

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
			out, err := db.Scan("key", test.cursor, test.pattern, test.count)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, out.Cursor, test.wantCursor)
			for i, item := range out.Items {
				testx.AssertEqual(t, item.Field, test.wantItems[i].Field)
				testx.AssertEqual(t, item.Value, test.wantItems[i].Value)
			}
		})
	}

	t.Run("different keys", func(t *testing.T) {
		_, _ = db.Set("person", "name", "alice")
		_, _ = db.Set("pet", "name", "doggo")

		out, err := db.Scan("person", 0, "*", 0)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, len(out.Items), 1)
		testx.AssertEqual(t, out.Items[0].Field, "name")
		testx.AssertEqual(t, out.Items[0].Value.String(), "alice")
	})
}

func TestScanner(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("key", "f11", "11")
	_, _ = db.Set("key", "f12", "12")
	_, _ = db.Set("key", "f21", "21")
	_, _ = db.Set("key", "f22", "22")
	_, _ = db.Set("key", "f31", "31")

	var items []rhash.HashItem
	err := red.View(func(tx *redka.Tx) error {
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
}

func TestLen(t *testing.T) {
	red, db := getDB(t)
	defer red.Close()

	_, _ = db.Set("person", "name", "alice")
	_, _ = db.Set("person", "age", 25)
	_, _ = db.Set("pet", "name", "doggo")

	tests := []struct {
		name string
		key  string
		want int
	}{
		{"multiple fields", "person", 2},
		{"single field", "pet", 1},
		{"key not found", "robot", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count, err := db.Len(test.key)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, count, test.want)
		})
	}
}

func TestSet(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		created, err := db.Set("person", "name", "alice")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)
		val, _ := db.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("create field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		created, err := db.Set("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, true)
		val, _ := db.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
	})
	t.Run("update field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		created, err := db.Set("person", "name", "bob")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, created, false)
		val, _ := db.Get("person", "name")
		testx.AssertEqual(t, val.String(), "bob")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("person", "alice")
		ok, err := db.Set("person", "name", "alice")
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, ok, false)
	})
}

func TestSetNotExists(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		ok, err := db.SetNotExists("person", "name", "alice")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)
		val, _ := db.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("create field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		ok, err := db.SetNotExists("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, true)
		val, _ := db.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
	})
	t.Run("update field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		ok, err := db.SetNotExists("person", "name", "bob")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, ok, false)
		val, _ := db.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("person", "alice")
		ok, err := db.SetNotExists("person", "name", "alice")
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, ok, false)
	})
}

func TestSetMany(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		fvals := map[string]any{
			"name": "alice",
			"age":  25,
		}
		count, err := db.SetMany("person", fvals)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)
		var val core.Value
		val, _ = db.Get("person", "name")
		testx.AssertEqual(t, val.String(), "alice")
		val, _ = db.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
	})
	t.Run("create fields", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		fvals := map[string]any{
			"age":  25,
			"city": "paris",
		}
		count, err := db.SetMany("person", fvals)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 2)
		var val core.Value
		val, _ = db.Get("person", "age")
		testx.AssertEqual(t, val.String(), "25")
		val, _ = db.Get("person", "city")
		testx.AssertEqual(t, val.String(), "paris")
	})
	t.Run("update fields", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		fvals := map[string]any{
			"name": "bob",
			"age":  50,
		}
		count, err := db.SetMany("person", fvals)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, count, 1)
		var val core.Value
		val, _ = db.Get("person", "name")
		testx.AssertEqual(t, val.String(), "bob")
		val, _ = db.Get("person", "age")
		testx.AssertEqual(t, val.String(), "50")
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("person", "alice")
		count, err := db.SetMany("person", map[string]any{
			"name": "alice", "age": 25,
		})
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, count, 0)
	})
}

func TestIncr(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		val, err := db.Incr("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25)
	})
	t.Run("create field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		val, err := db.Incr("person", "age", 25)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25)
	})
	t.Run("update field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "age", 25)
		val, err := db.Incr("person", "age", 10)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 35)
	})
	t.Run("decrement", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "age", 25)
		val, err := db.Incr("person", "age", -10)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 15)
	})
	t.Run("non-integer value", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		_, err := db.Incr("person", "name", 10)
		testx.AssertErr(t, err, core.ErrValueType)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("person", "alice")
		val, err := db.Incr("person", "age", 25)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0)
	})
}

func TestIncrFloat(t *testing.T) {
	t.Run("create key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		val, err := db.IncrFloat("person", "age", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)
	})
	t.Run("create field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		val, err := db.IncrFloat("person", "age", 25.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 25.5)
	})
	t.Run("update field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "age", 25.5)
		val, err := db.IncrFloat("person", "age", 10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 36.0)
	})
	t.Run("decrement", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "age", 25.5)
		val, err := db.IncrFloat("person", "age", -10.5)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, val, 15.0)
	})
	t.Run("non-float value", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		_, err := db.IncrFloat("person", "name", 10.5)
		testx.AssertErr(t, err, core.ErrValueType)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("person", "alice")
		val, err := db.IncrFloat("person", "age", 25.0)
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0.0)
	})
}

func TestDelete(t *testing.T) {
	t.Run("delete key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		_, _ = db.Set("person", "age", 25)

		delCount, err := db.Delete("person")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, delCount, 2)

		exist, _ := db.Exists("person", "name")
		testx.AssertEqual(t, exist, false)

		exist, _ = red.Key().Exists("person")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("delete some fields", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		_, _ = db.Set("person", "age", 25)
		_, _ = db.Set("person", "city", "paris")

		delCount, err := db.Delete("person", "name", "city")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, delCount, 2)

		exist, _ := db.Exists("person", "name")
		testx.AssertEqual(t, exist, false)
		exist, _ = db.Exists("person", "age")
		testx.AssertEqual(t, exist, true)
		exist, _ = db.Exists("person", "city")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("delete all fields", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		_, _ = db.Set("person", "age", 25)
		_, _ = db.Set("person", "city", "paris")

		delCount, err := db.Delete("person", "name", "age", "city")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, delCount, 3)

		exist, _ := db.Exists("person", "name")
		testx.AssertEqual(t, exist, false)
		exist, _ = db.Exists("person", "age")
		testx.AssertEqual(t, exist, false)
		exist, _ = db.Exists("person", "city")
		testx.AssertEqual(t, exist, false)

		exist, _ = red.Key().Exists("person")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("delete non-existent key", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		delCount, err := db.Delete("person")
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, delCount, 0)

		exist, _ := red.Key().Exists("person")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("delete non-existent field", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()

		_, _ = db.Set("person", "name", "alice")
		_, _ = db.Set("person", "city", "paris")

		delCount, err := db.Delete("person", "age", "city")
		testx.AssertEqual(t, delCount, 1)

		testx.AssertNoErr(t, err)
		exist, _ := db.Exists("person", "name")
		testx.AssertEqual(t, exist, true)
		exist, _ = db.Exists("person", "age")
		testx.AssertEqual(t, exist, false)
		exist, _ = db.Exists("person", "city")
		testx.AssertEqual(t, exist, false)
	})
	t.Run("key type mismatch", func(t *testing.T) {
		red, db := getDB(t)
		defer red.Close()
		_ = red.Str().Set("person", "alice")
		val, err := db.Delete("person", "name")
		testx.AssertErr(t, err, core.ErrKeyType)
		testx.AssertEqual(t, val, 0)
	})
}

func getDB(tb testing.TB) (*redka.DB, *rhash.DB) {
	tb.Helper()
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db, db.Hash()
}
