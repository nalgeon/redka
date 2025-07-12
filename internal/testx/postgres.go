//go:build postgres

package testx

import (
	_ "github.com/lib/pq"
)

func init() {
	driver = "postgres"
}
