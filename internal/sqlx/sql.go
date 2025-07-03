// Package sqlx provides base types and helper functions
// to work with SQL databases.
package sqlx

import (
	"database/sql"
	"strings"

	"github.com/nalgeon/redka/internal/core"
)

// Sorting direction.
const (
	Asc  = "asc"
	Desc = "desc"
)

// Aggregation functions.
const (
	Sum = "sum"
	Min = "min"
	Max = "max"
)

// Tx is a database transaction (or a transaction-like object).
// This is an abstraction over sql.Tx and sql.DB.
type Tx interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

// rowScanner is an interface to scan rows.
type RowScanner interface {
	Scan(dest ...any) error
}

// ExpandIn expands the IN clause in the query for a given parameter.
func ExpandIn[T any](query string, param string, args []T) (string, []any) {
	anyArgs := make([]any, len(args))
	pholders := make([]string, len(args))
	for i, arg := range args {
		anyArgs[i] = arg
		pholders[i] = "?"
	}
	query = strings.Replace(query, param, strings.Join(pholders, ","), 1)
	return query, anyArgs
}

func Select[T any](db Tx, query string, args []any,
	scan func(rows *sql.Rows) (T, error)) ([]T, error) {

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return vals, err
}

// Returns typed errors for some specific cases.
func TypedError(err error) error {
	if err == nil {
		return nil
	}
	if ConstraintFailed(err, "NOT NULL", "rkey.type") {
		return core.ErrKeyType
	}
	return err
}

// ConstraintFailed checks if the error is due to
// a constraint violation on a column.
func ConstraintFailed(err error, constraint, column string) bool {
	msg := constraint + " constraint failed: " + column
	return strings.Contains(err.Error(), msg)
}
