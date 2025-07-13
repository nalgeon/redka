//go:build sqlite3

package testx

import (
	_ "github.com/mattn/go-sqlite3"
)

func init() {
	driver = "sqlite3"
}
