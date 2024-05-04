package rkey

import (
	"database/sql"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlCount = `
	select count(id) from rkey
	where key in (:keys) and (etime is null or etime > ?)`

	sqlDelete = `
	delete from rkey
	where key in (:keys) and (etime is null or etime > ?)`

	sqlDeleteAll = `
	delete from rkey;
	vacuum;
	pragma integrity_check;`

	sqlDeleteAllExpired = `
	delete from rkey
	where etime <= ?`

	sqlDeleteNExpired = `
	delete from rkey
	where rowid in (
		select rowid from rkey
		where etime <= ?
		limit ?
	)`

	sqlExpire = `
	update rkey set
		version = version + 1,
		etime = ?
	where key = ? and (etime is null or etime > ?)`

	sqlGet = `
	select id, key, type, version, etime, mtime
	from rkey
	where key = ? and (etime is null or etime > ?)`

	sqlKeys = `
	select id, key, type, version, etime, mtime from rkey
	where key glob ? and (etime is null or etime > ?)`

	sqlLen = `
	select count(*) from rkey`

	sqlPersist = `
	update rkey set
		version = version + 1,
		etime = null
	where key = ? and (etime is null or etime > ?)`

	sqlRandom = `
	select id, key, type, version, etime, mtime from rkey
	where etime is null or etime > ?
	order by random() limit 1`

	sqlRename = `
	update or replace rkey set
		id = old.id,
		key = ?,
		type = old.type,
		version = old.version+1,
		etime = old.etime,
		mtime = ?
	from (
		select id, key, type, version, etime, mtime
		from rkey
		where key = ? and (etime is null or etime > ?)
	) as old
	where rkey.key = ? and (rkey.etime is null or rkey.etime > ?)`

	sqlScan = `
	select id, key, type, version, etime, mtime from rkey
	where
		id > ? and key glob ? and (type = ? or true)
		and (etime is null or etime > ?)
	limit ?`
)

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

// Count returns the number of existing keys among specified.
func (tx *Tx) Count(keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(sqlCount, ":keys", keys)
	args := append(keyArgs, now)
	var count int
	err := tx.tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

// Delete deletes keys and their values, regardless of the type.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (tx *Tx) Delete(keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlx.ExpandIn(sqlDelete, ":keys", keys)
	args := append(keyArgs, now)
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affectedCount, _ := res.RowsAffected()
	return int(affectedCount), nil
}

// DeleteAll deletes all keys and their values, effectively resetting
// the database. Should not be run inside a database transaction.
func (tx *Tx) DeleteAll() error {
	_, err := tx.tx.Exec(sqlDeleteAll)
	return err
}

// Exists reports whether the key exists.
func (tx *Tx) Exists(key string) (bool, error) {
	count, err := tx.Count(key)
	return count > 0, err
}

// Expire sets a time-to-live (ttl) for the key using a relative duration.
// After the ttl passes, the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (tx *Tx) Expire(key string, ttl time.Duration) error {
	at := time.Now().Add(ttl)
	return tx.ExpireAt(key, at)
}

// ExpireAt sets an expiration time for the key. After this time,
// the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (tx *Tx) ExpireAt(key string, at time.Time) error {
	args := []any{at.UnixMilli(), key, time.Now().UnixMilli()}
	res, err := tx.tx.Exec(sqlExpire, args...)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return core.ErrNotFound
	}
	return nil
}

// Get returns a specific key with all associated details.
// If the key does not exist, returns ErrNotFound.
func (tx *Tx) Get(key string) (core.Key, error) {
	args := []any{key, time.Now().UnixMilli()}
	var k core.Key
	err := tx.tx.QueryRow(sqlGet, args...).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return core.Key{}, core.ErrNotFound
	}
	return k, err
}

// Keys returns all keys matching pattern.
// Supports glob-style patterns like these:
//
//	key*  k?y  k[bce]y  k[!a-c][y-z]
//
// Use this method only if you are sure that the number of keys is
// limited. Otherwise, use the [Tx.Scan] or [Tx.Scanner] methods.
func (tx *Tx) Keys(pattern string) ([]core.Key, error) {
	args := []any{pattern, time.Now().UnixMilli()}
	scan := func(rows *sql.Rows) (core.Key, error) {
		var k core.Key
		err := rows.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
		return k, err
	}
	var keys []core.Key
	keys, err := sqlx.Select(tx.tx, sqlKeys, args, scan)
	return keys, err
}

// Len returns the total number of keys, including expired ones.
func (tx *Tx) Len() (int, error) {
	var n int
	err := tx.tx.QueryRow(sqlLen).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Persist removes the expiration time for the key.
// If the key does not exist, returns ErrNotFound.
func (tx *Tx) Persist(key string) error {
	args := []any{key, time.Now().UnixMilli()}
	res, err := tx.tx.Exec(sqlPersist, args...)
	if err != nil {
		return err
	}
	count, _ := res.RowsAffected()
	if count == 0 {
		return core.ErrNotFound
	}
	return nil
}

// Random returns a random key.
// If there are no keys, returns ErrNotFound.
func (tx *Tx) Random() (core.Key, error) {
	now := time.Now().UnixMilli()
	var k core.Key
	err := tx.tx.QueryRow(sqlRandom, now).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return core.Key{}, core.ErrNotFound
	}
	return k, err
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
// If the old key does not exist, returns ErrNotFound.
func (tx *Tx) Rename(key, newKey string) error {
	// Make sure the old key exists.
	oldK, err := tx.Get(key)
	if err != nil {
		return err
	}
	if !oldK.Exists() {
		return core.ErrNotFound
	}

	// If the keys are the same, do nothing.
	if key == newKey {
		return nil
	}

	// Make sure the new key does not exist or has the same type.
	newK, err := tx.Get(newKey)
	if err != nil && err != core.ErrNotFound {
		return err
	}
	if err == nil && oldK.Type != newK.Type {
		// Cannot overwrite a key with a different type.
		return core.ErrKeyType
	}

	// Rename the old key to the new key.
	now := time.Now().UnixMilli()
	args := []any{
		newKey, now,
		key, now,
		key, now,
	}
	_, err = tx.tx.Exec(sqlRename, args...)
	return err
}

// RenameNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
// Returns true if the key was renamed, false otherwise.
func (tx *Tx) RenameNotExists(key, newKey string) (bool, error) {
	// Make sure the old key exists.
	oldK, err := tx.Get(key)
	if err != nil {
		return false, err
	}
	if !oldK.Exists() {
		return false, core.ErrNotFound
	}

	// If the keys are the same, do nothing.
	if key == newKey {
		return false, nil
	}

	// Make sure the new key does not exist.
	exist, err := tx.Exists(newKey)
	if err != nil {
		return false, err
	}
	if exist {
		return false, nil
	}

	// Rename the old key to the new key.
	now := time.Now().UnixMilli()
	args := []any{
		newKey, now,
		key, now,
		key, now,
	}
	_, err = tx.tx.Exec(sqlRename, args...)
	return err == nil, err
}

// Scan iterates over keys matching pattern.
// Returns a slice of keys (see [core.Key]) of size count
// based on the current state of the cursor.
// Returns an empty slice when there are no more keys.
//
// Filtering and limiting options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - count to limit the number of keys returned (0 = default).
func (tx *Tx) Scan(cursor int, pattern string, ktype core.TypeID, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}
	query := sqlScan
	if ktype != core.TypeAny {
		query = strings.Replace(query, "(type = ? or true)", "(type = ?)", 1)
	}
	args := []any{
		cursor,
		pattern,
		int(ktype),
		time.Now().UnixMilli(),
		count,
	}
	scan := func(rows *sql.Rows) (core.Key, error) {
		var k core.Key
		err := rows.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
		return k, err
	}
	var keys []core.Key
	keys, err := sqlx.Select(tx.tx, query, args, scan)
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
// The scanner returns keys one by one, fetching them
// from the database in pageSize batches when necessary.
// Stops when there are no more items or an error occurs.
//
// Filtering and pagination options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - pageSize to limit the number of keys fetched at once (0 = default).
func (tx *Tx) Scanner(pattern string, ktype core.TypeID, pageSize int) *Scanner {
	return newScanner(tx, pattern, ktype, pageSize)
}

// ScanResult represents a result of the Scan call.
type ScanResult struct {
	Cursor int
	Keys   []core.Key
}

// deleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (tx *Tx) deleteExpired(n int) (int, error) {
	now := time.Now().UnixMilli()
	var res sql.Result
	var err error
	if n > 0 {
		res, err = tx.tx.Exec(sqlDeleteNExpired, now, n)
	} else {
		res, err = tx.tx.Exec(sqlDeleteAllExpired, now)
	}
	if err != nil {
		return 0, err
	}
	count, _ := res.RowsAffected()
	return int(count), err
}
