// Package testx provides helper functions for testing.
package testx

import (
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func AssertEqual(tb testing.TB, got, want any) {
	tb.Helper()
	if !reflect.DeepEqual(got, want) {
		tb.Errorf("want %#v, got %#v", want, got)
	}
}

func AssertErr(tb testing.TB, got, want error) {
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

func AssertNoErr(tb testing.TB, got error) {
	tb.Helper()
	if got != nil {
		tb.Fatalf("unexpected error %T (%v)", got, got)
	}
}
