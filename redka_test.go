package redka_test

import (
	"errors"
	"testing"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/testx"
)

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
