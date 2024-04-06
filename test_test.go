package redka_test

import (
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
)

func getDB(tb testing.TB) *redka.DB {
	tb.Helper()
	db, err := redka.Open(":memory:")
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

func assertErr(tb testing.TB, got, want error) {
	tb.Helper()
	if got == nil {
		tb.Errorf("want %T (%v) error, got nil", want, want)
		return
	}
	if got != want {
		tb.Errorf("want %T (%v) error, got %T (%v)", want, want, got, got)
		return
	}
}

func assertNoErr(tb testing.TB, got error) {
	tb.Helper()
	if got != nil {
		tb.Errorf("unexpected error %T (%v)", got, got)
	}
}
