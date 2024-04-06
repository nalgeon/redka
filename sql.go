// SQL schema and query helpers.
package redka

import (
	"database/sql"
	_ "embed"
	"slices"
	"strings"
	"time"
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

//go:embed sql/schema.sql
var sqlSchema string

const sqlKeyCount = `
select count(id) from rkey
where key in (:keys) and (etime is null or etime > :now)`

const sqlKeyGet = `
select id, key, type, version, etime, mtime
from rkey
where key = ? and (etime is null or etime > ?)`

const sqlKeyDel = `
delete from rkey where key in (:keys)
  and (etime is null or etime > :now)`

// rowScanner is an interface to scan rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// txKeyGet returns the key data structure.
func txKeyGet(tx *sql.Tx, key string) (k Key, err error) {
	now := time.Now().UnixMilli()
	row := tx.QueryRow(sqlKeyGet, key, now)
	err = row.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
	if err == sql.ErrNoRows {
		return k, nil
	}
	if err != nil {
		return k, err
	}
	return k, nil
}

// txKeyDelete deletes a key and its associated values.
func txKeyDelete(tx *sql.Tx, keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlExpandIn(sqlKeyDel, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affectedCount, _ := res.RowsAffected()
	return int(affectedCount), nil
}

// scanValue scans a key value from the row (rows).
func scanValue(scanner rowScanner) (key string, val Value, err error) {
	var value []byte
	err = scanner.Scan(&key, &value)
	if err == sql.ErrNoRows {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	return key, Value(value), nil
}

// expandIn expands the IN clause in the query for a given parameter.
func sqlExpandIn[T any](query string, param string, args []T) (string, []any) {
	anyArgs := make([]any, len(args))
	pholders := make([]string, len(args))
	for i, arg := range args {
		anyArgs[i] = arg
		pholders[i] = "?"
	}
	query = strings.Replace(query, param, strings.Join(pholders, ","), 1)
	return query, anyArgs
}

func sqlSelect[T any](db *sql.Tx, query string, args []any,
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
