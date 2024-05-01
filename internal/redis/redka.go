package redis

import (
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/rstring"
	"github.com/nalgeon/redka/internal/rzset"
)

// RHash is a hash repository.
type RHash interface {
	Delete(key string, fields ...string) (int, error)
	Exists(key, field string) (bool, error)
	Fields(key string) ([]string, error)
	Get(key, field string) (core.Value, error)
	GetMany(key string, fields ...string) (map[string]core.Value, error)
	Incr(key, field string, delta int) (int, error)
	IncrFloat(key, field string, delta float64) (float64, error)
	Items(key string) (map[string]core.Value, error)
	Len(key string) (int, error)
	Scan(key string, cursor int, pattern string, pageSize int) (rhash.ScanResult, error)
	Scanner(key, pattern string, pageSize int) *rhash.Scanner
	Set(key, field string, value any) (bool, error)
	SetMany(key string, items map[string]any) (int, error)
	SetNotExists(key, field string, value any) (bool, error)
	Values(key string) ([]core.Value, error)
}

// RKey is a key repository.
type RKey interface {
	Count(keys ...string) (int, error)
	Delete(keys ...string) (int, error)
	DeleteAll() error
	Exists(key string) (bool, error)
	Expire(key string, ttl time.Duration) error
	ExpireAt(key string, at time.Time) error
	Get(key string) (core.Key, error)
	Keys(pattern string) ([]core.Key, error)
	Persist(key string) error
	Random() (core.Key, error)
	Rename(key, newKey string) error
	RenameNotExists(key, newKey string) (bool, error)
	Scan(cursor int, pattern string, ktype core.TypeID, count int) (rkey.ScanResult, error)
	Scanner(pattern string, ktype core.TypeID, pageSize int) *rkey.Scanner
}

// RList is a list repository.
type RList interface {
	Delete(key string, elem any) (int, error)
	DeleteBack(key string, elem any, count int) (int, error)
	DeleteFront(key string, elem any, count int) (int, error)
	Get(key string, idx int) (core.Value, error)
	InsertAfter(key string, pivot, elem any) (int, error)
	InsertBefore(key string, pivot, elem any) (int, error)
	Len(key string) (int, error)
	PopBack(key string) (core.Value, error)
	PopBackPushFront(src, dest string) (core.Value, error)
	PopFront(key string) (core.Value, error)
	PushBack(key string, elem any) (int, error)
	PushFront(key string, elem any) (int, error)
	Range(key string, start, stop int) ([]core.Value, error)
	Set(key string, idx int, elem any) error
	Trim(key string, start, stop int) (int, error)
}

// RStr is a string repository.
type RStr interface {
	Get(key string) (core.Value, error)
	GetMany(keys ...string) (map[string]core.Value, error)
	Incr(key string, delta int) (int, error)
	IncrFloat(key string, delta float64) (float64, error)
	Set(key string, value any) error
	SetExpires(key string, value any, ttl time.Duration) error
	SetMany(items map[string]any) error
	SetWith(key string, value any) rstring.SetCmd
}

// RZSet is a sorted set repository.
type RZSet interface {
	Add(key string, elem any, score float64) (bool, error)
	AddMany(key string, items map[any]float64) (int, error)
	Count(key string, min, max float64) (int, error)
	Delete(key string, elems ...any) (int, error)
	DeleteWith(key string) rzset.DeleteCmd
	GetRank(key string, elem any) (rank int, score float64, err error)
	GetRankRev(key string, elem any) (rank int, score float64, err error)
	GetScore(key string, elem any) (float64, error)
	Incr(key string, elem any, delta float64) (float64, error)
	Inter(keys ...string) ([]rzset.SetItem, error)
	InterWith(keys ...string) rzset.InterCmd
	Len(key string) (int, error)
	Range(key string, start, stop int) ([]rzset.SetItem, error)
	RangeWith(key string) rzset.RangeCmd
	Scan(key string, cursor int, pattern string, count int) (rzset.ScanResult, error)
	Scanner(key, pattern string, pageSize int) *rzset.Scanner
	Union(keys ...string) ([]rzset.SetItem, error)
	UnionWith(keys ...string) rzset.UnionCmd
}

// Redka is an abstraction for *redka.DB and *redka.Tx.
// Used to execute commands in a unified way.
type Redka struct {
	hash RHash
	key  RKey
	list RList
	str  RStr
	zset RZSet
}

// RedkaDB creates a new Redka instance for a database.
func RedkaDB(db *redka.DB) Redka {
	return Redka{
		hash: db.Hash(),
		key:  db.Key(),
		list: db.List(),
		str:  db.Str(),
		zset: db.ZSet(),
	}
}

// RedkaTx creates a new Redka instance for a transaction.
func RedkaTx(tx *redka.Tx) Redka {
	return Redka{
		hash: tx.Hash(),
		key:  tx.Key(),
		list: tx.List(),
		str:  tx.Str(),
		zset: tx.ZSet(),
	}
}

// Hash returns the hash repository.
func (r Redka) Hash() RHash {
	return r.hash
}

// Key returns the key repository.
func (r Redka) Key() RKey {
	return r.key
}

func (r Redka) List() RList {
	return r.list
}

// Str returns the string repository.
func (r Redka) Str() RStr {
	return r.str
}

// ZSet returns the sorted set repository.
func (r Redka) ZSet() RZSet {
	return r.zset
}
