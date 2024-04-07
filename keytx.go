// Redis-like key repository in SQLite.
package redka

import (
	"database/sql"
	"slices"
	"time"
)

const sqlKeyKeys = `
select key from rkey
where key glob :pattern and (etime is null or etime > :now)`

const sqlKeyScan = `
select id, key, type, version, etime, mtime from rkey
where id > :cursor and key glob :pattern and (etime is null or etime > :now)
limit :count`

const sqlKeyRandom = `
select key from rkey
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

// KeyTx is a key repository transaction.
type KeyTx struct {
	tx sqlTx
}

// newKeyTx creates a key repository transaction
// from a generic database transaction.
func newKeyTx(tx sqlTx) *KeyTx {
	return &KeyTx{tx}
}

// Exists returns the number of existing keys among specified.
func (tx *KeyTx) Exists(keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	query, keyArgs := sqlExpandIn(sqlKeyCount, ":keys", keys)
	args := slices.Concat(keyArgs, []any{sql.Named("now", now)})
	var count int
	err := tx.tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

// Search returns all keys matching pattern.
func (tx *KeyTx) Search(pattern string) ([]string, error) {
	now := time.Now().UnixMilli()
	args := []any{sql.Named("pattern", pattern), sql.Named("now", now)}
	scan := func(rows *sql.Rows) (string, error) {
		var key string
		err := rows.Scan(&key)
		return key, err
	}
	var keys []string
	keys, err := sqlSelect(tx.tx, sqlKeyKeys, args, scan)
	return keys, err
}

// Scan iterates over keys matching pattern by returning
// the next page based on the current state of the cursor.
// Count regulates the number of keys returned (count = 0 for default).
func (tx *KeyTx) Scan(cursor int, pattern string, count int) (ScanResult, error) {
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
	scan := func(rows *sql.Rows) (Key, error) {
		var k Key
		err := rows.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
		return k, err
	}
	var keys []Key
	keys, err := sqlSelect(tx.tx, sqlKeyScan, args, scan)
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
func (tx *KeyTx) Scanner(pattern string, pageSize int) *keyScanner {
	return newKeyScanner(tx, pattern, pageSize)
}

// Random returns a random key.
func (tx *KeyTx) Random() (string, error) {
	now := time.Now().UnixMilli()
	var key string
	err := tx.tx.QueryRow(sqlKeyRandom, now).Scan(&key)
	if err == sql.ErrNoRows {
		return "", nil
	}
	// err := tx.tx.Get(&key, sqlKeyRandom, now)
	return key, err
}

// Get returns a specific key with all associated details.
func (tx *KeyTx) Get(key string) (Key, error) {
	now := time.Now().UnixMilli()
	var k Key
	// err := tx.tx.Get(&k, sqlKeyGet, key, now)
	err := tx.tx.QueryRow(sqlKeyGet, key, now).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return Key{}, nil
	}
	return k, err
}

// Expire sets a timeout on the key using a relative duration.
func (tx *KeyTx) Expire(key string, ttl time.Duration) (bool, error) {
	at := time.Now().Add(ttl)
	return tx.ETime(key, at)
}

// ETime sets a timeout on the key using an absolute time.
func (tx *KeyTx) ETime(key string, at time.Time) (bool, error) {
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
func (tx *KeyTx) Persist(key string) (bool, error) {
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
func (tx *KeyTx) Rename(key, newKey string) (bool, error) {
	// Make sure the old key exists.
	oldK, err := txKeyGet(tx.tx, key)
	if err != nil {
		return false, err
	}
	if !oldK.Exists() {
		return false, ErrKeyNotFound
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
func (tx *KeyTx) RenameNX(key, newKey string) (bool, error) {
	// Make sure the old key exists.
	oldK, err := txKeyGet(tx.tx, key)
	if err != nil {
		return false, err
	}
	if !oldK.Exists() {
		return false, ErrKeyNotFound
	}

	// If the keys are the same, do nothing.
	if key == newKey {
		return true, nil
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
func (tx *KeyTx) Delete(keys ...string) (int, error) {
	return txKeyDelete(tx.tx, keys...)
}

// deleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (tx *KeyTx) deleteExpired(n int) (int, error) {
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
	Keys   []Key
}

// keyScanner is the iterator for keys.
// Stops when there are no more keys or an error occurs.
type keyScanner struct {
	db       Keys
	cursor   int
	pattern  string
	pageSize int
	index    int
	cur      Key
	keys     []Key
	err      error
}

func newKeyScanner(db Keys, pattern string, pageSize int) *keyScanner {
	if pageSize == 0 {
		pageSize = scanPageSize
	}
	return &keyScanner{
		db:       db,
		cursor:   0,
		pattern:  pattern,
		pageSize: pageSize,
		index:    0,
		keys:     []Key{},
	}
}

// Scan advances to the next key, fetching keys from db as necessary.
// Returns false when there are no more keys or an error occurs.
func (sc *keyScanner) Scan() bool {
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
func (sc *keyScanner) Key() Key {
	return sc.cur
}

// Err returns the first error encountered during iteration.
func (sc *keyScanner) Err() error {
	return sc.err
}
