// Package storage defines database backends for Redka data structures.
// SQL and dialect-specific fragments live only in storagesqlite and storagepostgres;
// domain packages (rkey, rstring, …) depend on these interfaces.
package storage

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// Storage groups every backend used by Redis-like repositories.
type Storage interface {
	Keys() KeyStorage
	Strings() StringStorage
	Hashes() HashStorage
	Lists() ListStorage
	Sets() SetStorage
	ZSets() ZSetStorage
}

// KeyStorage implements key metadata operations.
type KeyStorage interface {
	Count(tx sqlx.Tx, keys []string, now int64) (int, error)
	Delete(tx sqlx.Tx, keys []string, now int64) (int, error)
	DeleteAll(tx sqlx.Tx) error
	DeleteAllExpired(tx sqlx.Tx, now int64) (sql.Result, error)
	DeleteNExpired(tx sqlx.Tx, now int64, n int) (sql.Result, error)
	Expire(tx sqlx.Tx, atMillis int64, key string, now int64) (sql.Result, error)
	Get(tx sqlx.Tx, key string, now int64) (core.Key, error)
	Keys(tx sqlx.Tx, pattern string, now int64) ([]core.Key, error)
	Len(tx sqlx.Tx) (int, error)
	Persist(tx sqlx.Tx, key string, now int64) (sql.Result, error)
	Random(tx sqlx.Tx, now int64) (core.Key, error)
	RenameDeleteByID(tx sqlx.Tx, id int) error
	RenameUpdateKey(tx sqlx.Tx, newKey string, mtime int64, oldKey string, now int64) error
	Scan(tx sqlx.Tx, cursor int, pattern string, ktype int, ktypeAny bool, now int64, limit int) ([]core.Key, error)
}

// StringKV is one row from MGET-style queries.
type StringKV struct {
	Key string
	Val []byte
}

// StringStorage implements rstring persistence.
type StringStorage interface {
	Get(tx sqlx.Tx, key string, now int64) ([]byte, error)
	GetMany(tx sqlx.Tx, keys []string, now int64) ([]StringKV, error)
	SetKey(tx sqlx.Tx, key string, etime *int64, now int64) error
	SetValue(tx sqlx.Tx, key string, value []byte) error
	UpdateKey(tx sqlx.Tx, key string, now int64) error
	UpdateValue(tx sqlx.Tx, key string, value []byte) error
}

// HashField is a field/value pair.
type HashField struct {
	Field string
	Value []byte
}

// HashScanRow is one HSCAN row.
type HashScanRow struct {
	RowID int
	Field string
	Value []byte
}

// HashStorage implements rhash persistence.
type HashStorage interface {
	DeleteFields(tx sqlx.Tx, key string, now int64, fields []string) (int, error)
	Fields(tx sqlx.Tx, key string, now int64) ([]string, error)
	Get(tx sqlx.Tx, key, field string, now int64) ([]byte, error)
	GetMany(tx sqlx.Tx, key string, now int64, fields []string) ([]HashField, error)
	Items(tx sqlx.Tx, key string, now int64) ([]HashField, error)
	Len(tx sqlx.Tx, key string, now int64) (int, error)
	Scan(tx sqlx.Tx, key string, now int64, cursor int, pattern string, limit int) ([]HashScanRow, error)
	Values(tx sqlx.Tx, key string, now int64) ([][]byte, error)
	CountFields(tx sqlx.Tx, key string, now int64, fields []string) (int, error)
	SetKey(tx sqlx.Tx, key string, now int64) (int, error)
	SetField(tx sqlx.Tx, keyID int, field string, value []byte) error
}

// ListStorage implements rlist persistence.
type ListStorage interface {
	DeleteEqual(tx sqlx.Tx, key string, elem []byte, now int64) (int64, error)
	DeleteBack(tx sqlx.Tx, key string, elem []byte, now int64, count int) (int64, error)
	DeleteFront(tx sqlx.Tx, key string, elem []byte, now int64, count int) (int64, error)
	Get(tx sqlx.Tx, key string, now int64, idx int, indexDesc bool) ([]byte, error)
	InsertBump(tx sqlx.Tx, now int64, key string, now2 int64) (keyID int, newLen int, err error)
	InsertAfter(tx sqlx.Tx, keyID int, pivot, elem []byte) error
	InsertBefore(tx sqlx.Tx, keyID int, pivot, elem []byte) error
	Len(tx sqlx.Tx, key string, now int64) (int, error)
	PopBack(tx sqlx.Tx, key string, now int64) ([]byte, error)
	PopFront(tx sqlx.Tx, key string, now int64) ([]byte, error)
	PushUpsert(tx sqlx.Tx, key string, now int64) (keyID int, newLen int, err error)
	PushBack(tx sqlx.Tx, keyID int, elem []byte) error
	PushFront(tx sqlx.Tx, keyID int, elem []byte) error
	Range(tx sqlx.Tx, key string, now int64, start, stop int) ([][]byte, error)
	Set(tx sqlx.Tx, key string, now int64, elem []byte, idx int, indexDesc bool) (int64, error)
	Trim(tx sqlx.Tx, key string, now int64, start, stop int) (int64, error)
}

// SetScanRow is one SSCAN row.
type SetScanRow struct {
	RowID int
	Elem  []byte
}

// SetStorage implements rset persistence.
type SetStorage interface {
	EnsureSetKey(tx sqlx.Tx, key string, now int64) (keyID int, err error)
	InsertSetElem(tx sqlx.Tx, keyID int, elem []byte) (created bool, err error)
	DeleteElems(tx sqlx.Tx, key string, now int64, elems [][]byte) (int, error)
	DiffElems(tx sqlx.Tx, key string, now int64, others []string) ([][]byte, error)
	DiffStore(tx sqlx.Tx, destID int, firstKey string, otherKeys []string, now int64) (int, error)
	InterElems(tx sqlx.Tx, now int64, keys []string) ([][]byte, error)
	InterStore(tx sqlx.Tx, destID int, keys []string, now int64) (int, error)
	Exists(tx sqlx.Tx, key string, now int64, elem []byte) (bool, error)
	Items(tx sqlx.Tx, key string, now int64) ([][]byte, error)
	Len(tx sqlx.Tx, key string, now int64) (int, error)
	Pop(tx sqlx.Tx, key string, now int64) ([]byte, error)
	PopDecr(tx sqlx.Tx, key string, now int64) error
	Random(tx sqlx.Tx, key string, now int64) ([]byte, error)
	Scan(tx sqlx.Tx, key string, now int64, cursor int, pattern string, limit int) ([]SetScanRow, error)
	UnionElems(tx sqlx.Tx, now int64, keys []string) ([][]byte, error)
	UnionStore(tx sqlx.Tx, destID int, keys []string, now int64) (int, error)
	CloneElems(tx sqlx.Tx, destID int, srcKey string, now int64) (int, error)
	DeleteKeyPart1(tx sqlx.Tx, key string, now int64) error
	DeleteKeyPart2(tx sqlx.Tx, key string, now int64) error
	AddKey(tx sqlx.Tx, key string, now int64) (int, error)
}

// ZSetElem is an element with score (row id used as SCAN cursor).
type ZSetElem struct {
	RowID int
	Elem  []byte
	Score float64
}

// ZSetStorage implements rzset persistence.
type ZSetStorage interface {
	CountScore(tx sqlx.Tx, key string, now int64, min, max float64) (int, error)
	DeleteElems(tx sqlx.Tx, key string, now int64, elems [][]byte) (int, error)
	UpdateKeyLen(tx sqlx.Tx, now int64, delta int, key string, now2 int64) error
	DeleteAllPart1(tx sqlx.Tx, key string, now int64) error
	DeleteAllPart2(tx sqlx.Tx, key string, now int64) error
	DeleteByRank(tx sqlx.Tx, key string, now int64, limit, offset int) (int, error)
	DeleteByScore(tx sqlx.Tx, key string, now int64, min, max float64) (int, error)
	GetScore(tx sqlx.Tx, key string, now int64, elem []byte) (float64, error)
	AddKey(tx sqlx.Tx, key string, now int64) (int, error)
	AddScore(tx sqlx.Tx, keyID int, elem []byte, score float64) error
	IncrScore(tx sqlx.Tx, keyID int, elem []byte, delta float64) (float64, error)
	Len(tx sqlx.Tx, key string, now int64) (int, error)
	Scan(tx sqlx.Tx, key string, now int64, cursor int, pattern string, limit int) ([]ZSetElem, error)
	CountElems(tx sqlx.Tx, key string, now int64, elems [][]byte) (int, error)
	GetRank(tx sqlx.Tx, key string, now int64, elem []byte, sortAsc bool) (rank int, score float64, err error)
	RangeRank(tx sqlx.Tx, key string, now int64, start, stop int, sortAsc bool) ([]ZSetElem, error)
	RangeScore(tx sqlx.Tx, key string, now int64, min, max float64, sortAsc bool, offset, count int) ([]ZSetElem, error)
	UnionRead(tx sqlx.Tx, keys []string, now int64, aggregate string) ([]ZSetElem, error)
	UnionWrite(tx sqlx.Tx, dest string, keys []string, now int64, aggregate string) (int, error)
	InterRead(tx sqlx.Tx, keys []string, now int64, aggregate string) ([]ZSetElem, error)
	InterWrite(tx sqlx.Tx, dest string, keys []string, now int64, aggregate string) (int, error)
}
