package redka_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/testx"
)

func ExampleOpen() {
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// ...
}

func ExampleDB_Close() {
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// ...
}

func ExampleDB_Hash() {
	// Error handling is omitted for brevity.
	// In real code, always check for errors.

	db, _ := redka.Open(":memory:", nil)
	defer db.Close()

	ok, err := db.Hash().Set("user:1", "name", "alice")
	fmt.Printf("ok=%v, err=%v\n", ok, err)
	ok, err = db.Hash().Set("user:1", "age", 25)
	fmt.Printf("ok=%v, err=%v\n", ok, err)

	name, err := db.Hash().Get("user:1", "name")
	fmt.Printf("name=%v, err=%v\n", name, err)
	age, err := db.Hash().Get("user:1", "age")
	fmt.Printf("age=%v, err=%v\n", age, err)

	// Output:
	// ok=true, err=<nil>
	// ok=true, err=<nil>
	// name=alice, err=<nil>
	// age=25, err=<nil>
}

func ExampleDB_Key() {
	// Error handling is omitted for brevity.
	// In real code, always check for errors.

	db, _ := redka.Open(":memory:", nil)
	defer db.Close()

	_ = db.Str().SetExpires("name", "alice", 60*time.Second)

	key, _ := db.Key().Get("name")
	fmt.Printf("key=%v, type=%v, version=%v, exists=%v\n",
		key.Key, key.TypeName(), key.Version, key.Exists())

	key, _ = db.Key().Get("nonexistent")
	fmt.Printf("key=%v, type=%v, version=%v, exists=%v\n",
		key.Key, key.TypeName(), key.Version, key.Exists())

	// Output:
	// key=name, type=string, version=1, exists=true
	// key=, type=unknown, version=0, exists=false
}

func ExampleDB_Str() {
	// Error handling is omitted for brevity.
	// In real code, always check for errors.

	db, _ := redka.Open(":memory:", nil)
	defer db.Close()

	_ = db.Str().Set("name", "alice")

	name, _ := db.Str().Get("name")
	fmt.Printf("name=%v\n", name)

	name, _ = db.Str().Get("nonexistent")
	fmt.Printf("name=%v\n", name)

	// Output:
	// name=alice
	// name=
}

func ExampleDB_ZSet() {
	// Error handling is omitted for brevity.
	// In real code, always check for errors.

	db, _ := redka.Open(":memory:", nil)
	defer db.Close()

	ok, err := db.ZSet().Add("race", "alice", 11)
	fmt.Printf("ok=%v, err=%v\n", ok, err)
	ok, err = db.ZSet().Add("race", "bob", 22)
	fmt.Printf("ok=%v, err=%v\n", ok, err)

	rank, score, err := db.ZSet().GetRank("race", "alice")
	fmt.Printf("alice: rank=%v, score=%v, err=%v\n", rank, score, err)

	rank, score, err = db.ZSet().GetRank("race", "bob")
	fmt.Printf("bob: rank=%v, score=%v, err=%v\n", rank, score, err)

	// Output:
	// ok=true, err=<nil>
	// ok=true, err=<nil>
	// alice: rank=0, score=11, err=<nil>
	// bob: rank=1, score=22, err=<nil>
}

func ExampleDB_Update() {
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	updCount := 0
	err = db.Update(func(tx *redka.Tx) error {
		err := tx.Str().Set("name", "alice")
		if err != nil {
			return err
		}
		updCount++

		err = tx.Str().Set("age", 25)
		if err != nil {
			return err
		}
		updCount++

		return nil
	})
	fmt.Printf("updated: count=%v, err=%v\n", updCount, err)

	// Output:
	// updated: count=2, err=<nil>
}

func ExampleDB_View() {
	// Error handling is omitted for brevity.
	// In real code, always check for errors.

	db, _ := redka.Open(":memory:", nil)
	defer db.Close()

	_ = db.Str().SetMany(map[string]any{
		"name": "alice",
		"age":  25,
	})

	type person struct {
		name string
		age  int
	}

	var p person
	err := db.View(func(tx *redka.Tx) error {
		name, err := tx.Str().Get("name")
		if err != nil {
			return err
		}
		p.name = name.String()

		age, err := tx.Str().Get("age")
		if err != nil {
			return err
		}
		// Only use MustInt() if you are sure that
		// the key exists and is an integer.
		p.age = age.MustInt()
		return nil
	})
	fmt.Printf("person=%+v, err=%v\n", p, err)

	// Output:
	// person={name:alice age:25}, err=<nil>
}

func TestOpenDB(t *testing.T) {
	sdb, err := sql.Open("sqlite3", ":memory:")
	testx.AssertNoErr(t, err)

	db, err := redka.OpenDB(sdb, sdb, nil)
	testx.AssertNoErr(t, err)
	defer db.Close()

	n, err := db.Key().Len()
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, n, 0)
}

func TestDBView(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	err := db.View(func(tx *redka.Tx) error {
		count, err := tx.Key().Count("name", "age")
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
		count, _ := tx.Key().Count("name", "age")
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

func getDB(tb testing.TB) *redka.DB {
	tb.Helper()
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db
}
