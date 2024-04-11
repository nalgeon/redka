// Redis-like key repository in SQLite.
package rkey

import (
	"database/sql"
	"slices"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const sqlKeyGet = `
select id, key, type, version, etime, mtime
from rkey
where key = ? and (etime is null or etime > ?)`

const sqlKeyCount = `
select count(id) from rkey
where key in (:keys) and (etime is null or etime > :now)`

const sqlKeySearch = `
select id, key, type, version, etime, mtime from rkey
where key glob :pattern and (etime is null or etime > :now)`

const sqlKeyScan = `
select id, key, type, version, etime, mtime from rkey
where id > :cursor and key glob :pattern and (etime is null or etime > :now)
limit :count`

const sqlKeyRandom = `
select id, key, type, version, etime, mtime from rkey
where etime is null or etime > ?
order by random() limit 1`

const sqlKeyExpire = `
update rkey set etime = :at
where key = :key and (etime is null or etime > :now)`

const sqlKeyPersist = `
update rkey set etime = null
where key = :key and (etime is null or etime > :now)`

const sqlKeyRename = `
update or replace rkey set
  id = old.id,
  key = :new_key,
  type = old.type,
  version = old.version+1,
  etime = old.etime,
  mtime = :now
from (
  select id, key, type, version, etime, mtime
  from rkey
  where key = :key and (etime is null or etime > :now)
) as old
where rkey.key = :key and (
  rkey.etime is null or rkey.etime > :now
)`

const sqlKeyDel = `
delete from rkey where key in (:keys)
  and (etime is null or etime > :now)`

const sqlKeyDelAllExpired = `
delete from rkey
where etime <= :now`

const sqlKeyDelNExpired = `
delete from rkey
where rowid in (
  select rowid from rkey
  where etime <= :now
  limit :n
)`

const scanPageSize = 10

// Tx is a key repository transaction.
type Tx struct {
	tx sqlx.Tx
}

// NewTx creates a key repository transaction
// from a generic database transaction.
func NewTx(tx sqlx.Tx) *Tx {
	return &Tx{tx}
}

// Exists returns the number of existing keys among specified.
func (tx *Tx) Exists(keys ...string) (int, error) {
	return CountKeys(tx.tx, keys...)
}

// Search returns all keys matching pattern.
func (tx *Tx) Search(pattern string) ([]core.Key, error) {
	now := time.Now().UnixMilli()
	args := []any{sql.Named("pattern", pattern), sql.Named("now", now)}
	scan := func(rows *sql.Rows) (core.Key, error) {
		var k core.Key
		err := rows.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
		return k, err
	}
	var keys []core.Key
	keys, err := sqlx.Select(tx.tx, sqlKeySearch, args, scan)
	return keys, err
}

// Scan iterates over keys matching pattern by returning
// the next page based on the current state of the cursor.
// Count regulates the number of keys returned (count = 0 for default).
func (tx *Tx) Scan(cursor int, pattern string, count int) (ScanResult, error) {
	now := time.Now().UnixMilli()
	if count == 0 {
		count = scanPageSize
	}
	args := []any{
		sql.Named("cursor", cursor),
		sql.Named("pattern", pattern),
		sql.Named("now", now),
		sql.Named("count", count),
	}
	scan := func(rows *sql.Rows) (core.Key, error) {
		var k core.Key
		err := rows.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
		return k, err
	}
	var keys []core.Key
	keys, err := sqlx.Select(tx.tx, sqlKeyScan, args, scan)
	if err != nil {
		return ScanResult{}, err
	}

	// Select the maximum ID.
	maxID := 0
	for _, key := range keys {
		if key.ID > maxID {
			maxID = key.ID
		}
	}

	return ScanResult{maxID, keys}, nil
}

// Scanner returns an iterator for keys matching pattern.
// The scanner returns keys one by one, fetching a new page
// when the current one is exhausted. Set pageSize to 0 for default.
func (tx *Tx) Scanner(pattern string, pageSize int) *Scanner {
	return newScanner(tx, pattern, pageSize)
}

// Random returns a random key.
func (tx *Tx) Random() (core.Key, error) {
	now := time.Now().UnixMilli()
	var k core.Key
	err := tx.tx.QueryRow(sqlKeyRandom, now).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return core.Key{}, nil
	}
	return k, err
}

// Get returns a specific key with all associated details.
func (tx *Tx) Get(key string) (core.Key, error) {
	return GetKey(tx.tx, key)
}

// Expire sets a timeout on the key using a relative duration.
func (tx *Tx) Expire(key string, ttl time.Duration) (bool, error) {
	at := time.Now().Add(ttl)
	return tx.ExpireAt(key, at)
}

// ExpireAt sets a timeout on the key using an absolute time.
func (tx *Tx) ExpireAt(key string, at time.Time) (bool, error) {
	now := time.Now().UnixMilli()
	args := []any{
		sql.Named("key", key),
		sql.Named("now", now),
		sql.Named("at", at.UnixMilli()),
	}
	res, err := tx.tx.Exec(sqlKeyExpire, args...)
	if err != nil {
		return false, err
	}
	count, _ := res.RowsAffected()
	return count > 0, nil
}

// Persist removes a timeout on the key.
func (tx *Tx) Persist(key string) (bool, error) {
	now := time.Now().UnixMilli()
	args := []any{sql.Named("key", key), sql.Named("now", now)}
	res, err := tx.tx.Exec(sqlKeyPersist, args...)
	if err != nil {
		return false, err
	}
	count, _ := res.RowsAffected()
	return count > 0, nil
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
func (tx *Tx) Rename(key, newKey string) (bool, error) {
	// Make sure the old key exists.
	oldK, err := GetKey(tx.tx, key)
	if err != nil {
		return false, err
	}
	if !oldK.Exists() {
		return false, core.ErrKeyNotFound
	}

	// If the keys are the same, do nothing.
	if key == newKey {
		return true, nil
	}

	// Delete the new key if it exists.
	_, err = tx.Delete(newKey)
	if err != nil {
		return false, err
	}

	// Rename the old key to the new key.
	now := time.Now().UnixMilli()
	args := []any{
		sql.Named("key", key),
		sql.Named("new_key", newKey),
		sql.Named("now", now),
	}
	_, err = tx.tx.Exec(sqlKeyRename, args...)
	return err == nil, err
}

// RenameNX changes the key name.
// If there is an existing key with the new name, does nothing.
func (tx *Tx) RenameNX(key, newKey string) (bool, error) {
	// Make sure the old key exists.
	oldK, err := GetKey(tx.tx, key)
	if err != nil {
		return false, err
	}
	if !oldK.Exists() {
		return false, core.ErrKeyNotFound
	}

	// If the keys are the same, do nothing.
	if key == newKey {
		return false, nil
	}

	// Make sure the new key does not exist.
	count, err := tx.Exists(newKey)
	if err != nil {
		return false, err
	}
	if count != 0 {
		return false, nil
	}

	// Rename the old key to the new key.
	now := time.Now().UnixMilli()
	args := []any{
		sql.Named("key", key),
		sql.Named("new_key", newKey),
		sql.Named("now", now),
	}
	_, err = tx.tx.Exec(sqlKeyRename, args...)
	return err == nil, err
}

// Delete deletes keys and their values.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (tx *Tx) Delete(keys ...string) (int, error) {
	return DeleteKeys(tx.tx, keys...)
}

// deleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (tx *Tx) deleteExpired(n int) (int, error) {
	now := time.Now().UnixMilli()
	var res sql.Result
	var err error
	if n > 0 {
		args := []any{sql.Named("now", now), sql.Named("n", n)}
		res, err = tx.tx.Exec(sqlKeyDelNExpired, args...)
	} else {
		res, err = tx.tx.Exec(sqlKeyDelAllExpired, now)
	}
	if err != nil {
		return 0, err
	}
	count, _ := res.RowsAffected()
	return int(count), err
}

// ScanResult represents a result of the scan command.
type ScanResult struct {
	Cursor int
	Keys   []core.Key
}

// Scanner is the iterator for keys.
// Stops when there are no more keys or an error occurs.
type Scanner struct {
	db       *Tx
	cursor   int
	pattern  string
	pageSize int
	index    int
	cur      core.Key
	keys     []core.Key
	err      error
}

func newScanner(db *Tx, pattern string, pageSize int) *Scanner {
	if pageSize == 0 {
		pageSize = scanPageSize
	}
	return &Scanner{
		db:       db,
		cursor:   0,
		pattern:  pattern,
		pageSize: pageSize,
		index:    0,
		keys:     []core.Key{},
	}
}

// Scan advances to the next key, fetching keys from db as necessary.
// Returns false when there are no more keys or an error occurs.
func (sc *Scanner) Scan() bool {
	if sc.index >= len(sc.keys) {
		// Fetch a new page of keys.
		out, err := sc.db.Scan(sc.cursor, sc.pattern, sc.pageSize)
		if err != nil {
			sc.err = err
			return false
		}
		sc.cursor = out.Cursor
		sc.keys = out.Keys
		sc.index = 0
		if len(sc.keys) == 0 {
			return false
		}
	}
	// Advance to the next key from the current page.
	sc.cur = sc.keys[sc.index]
	sc.index++
	return true
}

// Key returns the current key.
func (sc *Scanner) Key() core.Key {
	return sc.cur
}

// Err returns the first error encountered during iteration.
func (sc *Scanner) Err() error {
	return sc.err
}

// GetKey returns the key data structure.
func GetKey(tx sqlx.Tx, key string) (core.Key, error) {
	now := time.Now().UnixMilli()
	var k core.Key
	err := tx.QueryRow(sqlKeyGet, key, now).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return core.Key{}, nil
	}
	return k, err
}

// CountKeys returns the number of existing keys among specified.
func CountKeys(tx sqlx.Tx, keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(sqlKeyCount, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})
	var count int
	err := tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

// DeleteKeys deletes keys and their values (regardless of the type).
func DeleteKeys(tx sqlx.Tx, keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(sqlKeyDel, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affectedCount, _ := res.RowsAffected()
	return int(affectedCount), nil
}
