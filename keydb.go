// Redis-like key repository in SQLite.
package redka

import (
	"database/sql"
	"time"
)

// Keys is a key repository.
type Keys interface {
	Exists(keys ...string) (int, error)
	Search(pattern string) ([]string, error)
	Scan(cursor int, pattern string, count int) (ScanResult, error)
	Scanner(pattern string, pageSize int) *keyScanner
	Random() (string, error)
	Get(key string) (Key, error)
	Expire(key string, ttl time.Duration) (bool, error)
	ETime(key string, at time.Time) (bool, error)
	Persist(key string) (bool, error)
	Rename(key, newKey string) (bool, error)
	RenameNX(key, newKey string) (bool, error)
	Delete(keys ...string) (int, error)
}

// KeyDB is a database-backed key repository.
type KeyDB struct {
	*sqlDB[*KeyTx]
}

// OpenKey creates a new database-backed key repository.
// Creates the database schema if necessary.
func OpenKey(db *sql.DB) (*KeyDB, error) {
	d, err := openSQL(db, newKeyTx)
	return &KeyDB{d}, err
}

// newKeyDB creates a new database-backed key repository.
// Does not create the database schema.
func newKeyDB(db *sql.DB) *KeyDB {
	d := newSqlDB(db, newKeyTx)
	return &KeyDB{d}
}

// Exists returns the number of existing keys among specified.
func (db *KeyDB) Exists(keys ...string) (int, error) {
	var count int
	err := db.View(func(tx *KeyTx) error {
		var err error
		count, err = tx.Exists(keys...)
		return err
	})
	return count, err
}

// Search returns all keys matching pattern.
func (db *KeyDB) Search(pattern string) ([]string, error) {
	var keys []string
	err := db.View(func(tx *KeyTx) error {
		var err error
		keys, err = tx.Search(pattern)
		return err
	})
	return keys, err
}

// Scan iterates over keys matching pattern by returning
// the next page based on the current state of the cursor.
// Count regulates the number of keys returned (count = 0 for default).
func (db *KeyDB) Scan(cursor int, pattern string, count int) (ScanResult, error) {
	var out ScanResult
	err := db.View(func(tx *KeyTx) error {
		var err error
		out, err = tx.Scan(cursor, pattern, count)
		return err
	})
	return out, err
}

// Scanner returns an iterator for keys matching pattern.
// The scanner returns keys one by one, fetching a new page
// when the current one is exhausted. Set pageSize to 0 for default.
func (db *KeyDB) Scanner(pattern string, pageSize int) *keyScanner {
	return newKeyScanner(db, pattern, pageSize)
}

// Random returns a random key.
func (db *KeyDB) Random() (string, error) {
	var key string
	err := db.View(func(tx *KeyTx) error {
		var err error
		key, err = tx.Random()
		return err
	})
	return key, err
}

// Get returns a specific key with all associated details.
func (db *KeyDB) Get(key string) (Key, error) {
	var k Key
	err := db.View(func(tx *KeyTx) error {
		var err error
		k, err = tx.Get(key)
		return err
	})
	return k, err
}

// Expire sets a timeout on the key using a relative duration.
func (db *KeyDB) Expire(key string, ttl time.Duration) (bool, error) {
	var ok bool
	err := db.Update(func(tx *KeyTx) error {
		var err error
		ok, err = tx.Expire(key, ttl)
		return err
	})
	return ok, err
}

// ETime sets a timeout on the key using an absolute time.
func (db *KeyDB) ETime(key string, at time.Time) (bool, error) {
	var ok bool
	err := db.Update(func(tx *KeyTx) error {
		var err error
		ok, err = tx.ETime(key, at)
		return err
	})
	return ok, err
}

// Persist removes a timeout on the key.
func (db *KeyDB) Persist(key string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *KeyTx) error {
		var err error
		ok, err = tx.Persist(key)
		return err
	})
	return ok, err
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
func (db *KeyDB) Rename(key, newKey string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *KeyTx) error {
		var err error
		ok, err = tx.Rename(key, newKey)
		return err
	})
	return ok, err
}

// RenameIfNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
func (db *KeyDB) RenameNX(key, newKey string) (bool, error) {
	var ok bool
	err := db.Update(func(tx *KeyTx) error {
		var err error
		ok, err = tx.RenameNX(key, newKey)
		return err
	})
	return ok, err
}

// Delete deletes keys and their values.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (db *KeyDB) Delete(keys ...string) (int, error) {
	var count int
	err := db.Update(func(tx *KeyTx) error {
		var err error
		count, err = tx.Delete(keys...)
		return err
	})
	return count, err
}

// deleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (db *KeyDB) deleteExpired(n int) (count int, err error) {
	err = db.Update(func(tx *KeyTx) error {
		count, err = tx.deleteExpired(n)
		return err
	})
	return count, err
}
