package redka_test

import (
	"errors"
	"testing"

	"github.com/nalgeon/redka"
)

func TestDBView(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	err := db.View(func(tx *redka.Tx) error {
		count, err := tx.Key().Exists("name", "age")
		assertNoErr(t, err)
		assertEqual(t, count, 2)

		name, err := tx.Str().Get("name")
		assertNoErr(t, err)
		assertEqual(t, name.String(), "alice")

		age, err := tx.Str().Get("age")
		assertNoErr(t, err)
		assertEqual(t, age.MustInt(), 25)
		return nil
	})
	assertNoErr(t, err)
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
	assertNoErr(t, err)

	err = db.View(func(tx *redka.Tx) error {
		count, _ := tx.Key().Exists("name", "age")
		assertEqual(t, count, 2)

		name, _ := tx.Str().Get("name")
		assertEqual(t, name.String(), "alice")

		age, _ := tx.Str().Get("age")
		assertEqual(t, age.MustInt(), 25)
		return nil
	})
	assertNoErr(t, err)
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
	assertEqual(t, err, errRollback)

	name, _ := db.Str().Get("name")
	assertEqual(t, name.String(), "alice")
	age, _ := db.Str().Get("age")
	assertEqual(t, age.MustInt(), 25)
}

func TestDBFlush(t *testing.T) {
	db := getDB(t)
	defer db.Close()

	_ = db.Str().Set("name", "alice")
	_ = db.Str().Set("age", 25)

	err := db.Flush()
	assertNoErr(t, err)

	count, _ := db.Key().Exists("name", "age")
	assertEqual(t, count, 0)

}
