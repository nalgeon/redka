package rkey

import (
	"database/sql"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

// scanPageSize is the default number
// of keys per page when scanning.
const scanPageSize = 10

// Tx is a key repository transaction.
type Tx struct {
	dialect sqlx.Dialect
	tx      sqlx.Tx
	keys    storage.KeyStorage
}

// NewTx creates a key repository transaction
// from a generic database transaction.
func NewTx(dialect sqlx.Dialect, st storage.Storage, tx sqlx.Tx) *Tx {
	return &Tx{dialect: dialect, tx: tx, keys: st.Keys()}
}

// Count returns the number of existing keys among specified.
func (tx *Tx) Count(keys ...string) (int, error) {
	return tx.keys.Count(tx.tx, keys, time.Now().UnixMilli())
}

// Delete deletes keys and their values, regardless of the type.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (tx *Tx) Delete(keys ...string) (int, error) {
	return tx.keys.Delete(tx.tx, keys, time.Now().UnixMilli())
}

// DeleteAll deletes all keys and their values, effectively resetting
// the database. Should not be run inside a database transaction.
func (tx *Tx) DeleteAll() error {
	return tx.keys.DeleteAll(tx.tx)
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
	res, err := tx.keys.Expire(tx.tx, at.UnixMilli(), key, time.Now().UnixMilli())
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
	return tx.keys.Get(tx.tx, key, time.Now().UnixMilli())
}

// Keys returns all keys matching pattern.
// Supports glob-style patterns like these:
//
//	key*  k?y  k[bce]y  k[!a-c][y-z]
//
// Use this method only if you are sure that the number of keys is
// limited. Otherwise, use the [Tx.Scan] or [Tx.Scanner] methods.
func (tx *Tx) Keys(pattern string) ([]core.Key, error) {
	return tx.keys.Keys(tx.tx, pattern, time.Now().UnixMilli())
}

// Len returns the total number of keys, including expired ones.
func (tx *Tx) Len() (int, error) {
	return tx.keys.Len(tx.tx)
}

// Persist removes the expiration time for the key.
// If the key does not exist, returns ErrNotFound.
func (tx *Tx) Persist(key string) error {
	res, err := tx.keys.Persist(tx.tx, key, time.Now().UnixMilli())
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
	return tx.keys.Random(tx.tx, time.Now().UnixMilli())
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
// If the old key does not exist, returns ErrNotFound.
func (tx *Tx) Rename(key, newKey string) error {
	oldK, err := tx.Get(key)
	if err != nil {
		return err
	}
	if !oldK.Exists() {
		return core.ErrNotFound
	}

	if key == newKey {
		return nil
	}

	newK, err := tx.Get(newKey)
	if err != nil && err != core.ErrNotFound {
		return err
	}
	if err == nil && oldK.Type != newK.Type {
		return core.ErrKeyType
	}

	if newK.Exists() {
		if err := tx.keys.RenameDeleteByID(tx.tx, newK.ID); err != nil {
			return err
		}
	}

	now := time.Now().UnixMilli()
	return tx.keys.RenameUpdateKey(tx.tx, newKey, now, key, now)
}

// RenameNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
// Returns true if the key was renamed, false otherwise.
func (tx *Tx) RenameNotExists(key, newKey string) (bool, error) {
	oldK, err := tx.Get(key)
	if err != nil {
		return false, err
	}
	if !oldK.Exists() {
		return false, core.ErrNotFound
	}

	if key == newKey {
		return false, nil
	}

	exist, err := tx.Exists(newKey)
	if err != nil {
		return false, err
	}
	if exist {
		return false, nil
	}

	now := time.Now().UnixMilli()
	err = tx.keys.RenameUpdateKey(tx.tx, newKey, now, key, now)
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
	ktypeGuard := ktype == core.TypeAny
	keys, err := tx.keys.Scan(tx.tx, cursor, pattern, int(ktype), ktypeGuard, time.Now().UnixMilli(), count)
	if err != nil {
		return ScanResult{}, err
	}
	maxID := 0
	if len(keys) > 0 {
		maxID = keys[len(keys)-1].ID
	}
	return ScanResult{maxID, keys}, nil
}

// Scanner returns an iterator for keys matching pattern.
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
		res, err = tx.keys.DeleteNExpired(tx.tx, now, n)
	} else {
		res, err = tx.keys.DeleteAllExpired(tx.tx, now)
	}
	if err != nil {
		return 0, err
	}
	count, _ := res.RowsAffected()
	return int(count), err
}
