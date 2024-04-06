package redka

import (
	"reflect"
	"testing"
	"time"
)

func Test_deleteExpired(t *testing.T) {
	red := getDB(t)
	defer red.Close()

	db := red.keyDB
	t.Run("delete all", func(t *testing.T) {
		_ = red.Str().SetExpires("name", "alice", 1*time.Millisecond)
		_ = red.Str().SetExpires("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := db.deleteExpired(0)
		assertNoErr(t, err)
		assertEqual(t, count, 2)

		count, _ = db.Exists("name", "age")
		assertEqual(t, count, 0)
	})
	t.Run("delete n", func(t *testing.T) {
		_ = red.Str().SetExpires("name", "alice", 1*time.Millisecond)
		_ = red.Str().SetExpires("age", 25, 1*time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		count, err := db.deleteExpired(1)
		assertNoErr(t, err)
		assertEqual(t, count, 1)
	})
}

func getDB(tb testing.TB) *DB {
	tb.Helper()
	db, err := Open(":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	return db
}

func assertEqual(tb testing.TB, got, want any) {
	tb.Helper()
	if !reflect.DeepEqual(got, want) {
		tb.Errorf("want %#v, got %#v", want, got)
	}
}

func assertNoErr(tb testing.TB, got error) {
	tb.Helper()
	if got != nil {
		tb.Errorf("unexpected error %T (%v)", got, got)
	}
}
