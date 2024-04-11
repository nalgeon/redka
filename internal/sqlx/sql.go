// SQL schema and query helpers.
package sqlx

import (
	"database/sql"
	_ "embed"
	"slices"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
)

// Database schema version.
// const schemaVersion = 1

// Default SQL settings.
const sqlSettings = `
pragma journal_mode = wal;
pragma synchronous = normal;
pragma temp_store = memory;
pragma mmap_size = 268435456;
pragma foreign_keys = on;
`

//go:embed schema.sql
var sqlSchema string

const SQLKeyCount = `
select count(id) from rkey
where key in (:keys) and (etime is null or etime > :now)`

const SQLKeyGet = `
select id, key, type, version, etime, mtime
from rkey
where key = ? and (etime is null or etime > ?)`

const SQLKeyDel = `
delete from rkey where key in (:keys)
  and (etime is null or etime > :now)`

const sqlTruncate = `
delete from rkey;
vacuum;
pragma integrity_check;`

// Tx is a database transaction (or a transaction-like object).
type Tx interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

// rowScanner is an interface to scan rows.
type RowScanner interface {
	Scan(dest ...any) error
}

// GetKey returns the key data structure.
func GetKey(tx Tx, key string) (k core.Key, err error) {
	now := time.Now().UnixMilli()
	row := tx.QueryRow(SQLKeyGet, key, now)
	err = row.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
	if err == sql.ErrNoRows {
		return k, nil
	}
	if err != nil {
		return k, err
	}
	return k, nil
}

// DeleteKey deletes a key and its associated values.
func DeleteKey(tx Tx, keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := ExpandIn(SQLKeyDel, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affectedCount, _ := res.RowsAffected()
	return int(affectedCount), nil
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

// Truncate deletes all data from the database.
func Truncate[T any](db *DB[T]) error {
	_, err := db.SQL.Exec(sqlTruncate)
	return err
}
