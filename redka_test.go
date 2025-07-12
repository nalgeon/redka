package redka_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/nalgeon/be"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/testx"
)

func ExampleOpen_file() {
	// Open a file-based SQLite database.
	// The database will be created if it doesn't exist.
	db, err := redka.Open("redka.db", nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	// ...

}

func ExampleOpen_memory() {
	// Open an in-memory SQLite database.
	db, err := redka.Open("file:/redka.db?vfs=memdb", nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	// ...
}

func ExampleOpen_options() {
	// Set custom options for the database.
	opts := &redka.Options{
		DriverName: "sqlite3",
		Timeout:    5 * time.Second,
		Pragma: map[string]string{
			"synchronous": "off",
		},
	}

	db, err := redka.Open("file:/redka.db?vfs=memdb", opts)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	// ...
}

func ExampleOpen_postgres() {
	// Open an existing PostgreSQL database.
	// Pass the connection string in this format:
	connString := "postgres://user:password@localhost:5432/mydb?sslmode=disable"
	// You also need to explicitly pass the driver name:
	opts := &redka.Options{DriverName: "postgres"}

	db, err := redka.Open(connString, opts)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	// ...
}

func ExampleOpenRead() {
	// open a writable database
	db, err := redka.Open("redka.db", nil)
	if err != nil {
		panic(err)
	}
	_ = db.Str().Set("name", "alice")
	_ = db.Close()

	// open a read-only database
	db, err = redka.OpenRead("redka.db", nil)
	if err != nil {
		panic(err)
	}
	// read operations work fine
	name, _ := db.Str().Get("name")
	fmt.Println(name)
	// write operations will fail
	err = db.Str().Set("name", "bob")
	fmt.Println(err)
	// attempt to write a readonly database
	_ = db.Close()

	// Output:
	// alice
	// attempt to write a readonly database
}

func ExampleDB_Close() {
	db, err := redka.Open("file:/redka.db?vfs=memdb", nil)
	if err != nil {
		panic(err)
	}

	// Perform some operations on the database.
	// ...

	err = db.Close()
	if err != nil {
		panic(err)
	}
}

func ExampleDB_Hash() {
	// Error handling is omitted for brevity.
	// In real code, always check for errors.

	db, _ := redka.Open("file:/redka.db?vfs=memdb", nil)
	defer func() { _ = db.Close() }()

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

	db, _ := redka.Open("file:/redka.db?vfs=memdb", nil)
	defer func() { _ = db.Close() }()

	_ = db.Str().SetExpires("name", "alice", 60*time.Second)
	_ = db.Str().Set("city", "paris")

	key, _ := db.Key().Get("name")
	fmt.Printf("key=%v, type=%v, version=%v, exists=%v\n",
		key.Key, key.TypeName(), key.Version, key.Exists())

	key, _ = db.Key().Get("nonexistent")
	fmt.Printf("key=%v, type=%v, version=%v, exists=%v\n",
		key.Key, key.TypeName(), key.Version, key.Exists())

	scan, _ := db.Key().Scan(0, "*", redka.TypeString, 100)
	fmt.Print("keys:")
	for _, key := range scan.Keys {
		fmt.Print(" ", key.Key)
	}
	fmt.Println()

	// Output:
	// key=name, type=string, version=1, exists=true
	// key=, type=unknown, version=0, exists=false
	// keys: name city
}

func ExampleDB_Str() {
	// Error handling is omitted for brevity.
	// In real code, always check for errors.

	db, _ := redka.Open("file:/redka.db?vfs=memdb", nil)
	defer func() { _ = db.Close() }()

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

	db, _ := redka.Open("file:/redka.db?vfs=memdb", nil)
	defer func() { _ = db.Close() }()

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
	db, err := redka.Open("file:/redka.db?vfs=memdb", nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()

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

	db, _ := redka.Open("file:/redka.db?vfs=memdb", nil)
	defer func() { _ = db.Close() }()

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
	sdb, err := sql.Open("sqlite3", "file:/redka.db?vfs=memdb")
	be.Err(t, err, nil)

	db, err := redka.OpenDB(sdb, sdb, nil)
	be.Err(t, err, nil)
	defer func() { _ = db.Close() }()

	n, err := db.Key().Len()
	be.Err(t, err, nil)
	be.Equal(t, n, 0)
}

func TestDB_View(t *testing.T) {
	db := testx.OpenDB(t)

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	err := db.View(func(tx *redka.Tx) error {
		count, err := tx.Key().Count("name", "age")
		be.Err(t, err, nil)
		be.Equal(t, count, 2)

		name, err := tx.Str().Get("name")
		be.Err(t, err, nil)
		be.Equal(t, name.String(), "alice")

		age, err := tx.Str().Get("age")
		be.Err(t, err, nil)
		be.Equal(t, age.MustInt(), 25)
		return nil
	})
	be.Err(t, err, nil)
}

func TestDB_Update(t *testing.T) {
	db := testx.OpenDB(t)

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
	be.Err(t, err, nil)

	err = db.View(func(tx *redka.Tx) error {
		count, _ := tx.Key().Count("name", "age")
		be.Equal(t, count, 2)

		name, _ := tx.Str().Get("name")
		be.Equal(t, name.String(), "alice")

		age, _ := tx.Str().Get("age")
		be.Equal(t, age.MustInt(), 25)
		return nil
	})
	be.Err(t, err, nil)
}

func TestRollback(t *testing.T) {
	db := testx.OpenDB(t)

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	var errRollback = errors.New("rollback")

	err := db.Update(func(tx *redka.Tx) error {
		_ = tx.Str().Set("name", "bob")
		_ = tx.Str().Set("age", 50)
		return errRollback
	})
	be.Equal(t, err, errRollback)

	name, _ := db.Str().Get("name")
	be.Equal(t, name.String(), "alice")
	age, _ := db.Str().Get("age")
	be.Equal(t, age.MustInt(), 25)
}

func TestTimeout(t *testing.T) {
	opts := &redka.Options{Timeout: time.Nanosecond}
	db, err := redka.Open("file:/redka.db?vfs=memdb", opts)
	be.Err(t, err, nil)
	defer func() { _ = db.Close() }()
	err = db.Str().Set("name", "alice")
	be.Err(t, err, context.DeadlineExceeded)
}
