// Common types and functions.
package core

import (
	"errors"
	"strconv"
)

// type identifiers
type TypeID int

const (
	TypeString    = TypeID(1)
	TypeList      = TypeID(2)
	TypeSet       = TypeID(3)
	TypeHash      = TypeID(4)
	TypeSortedSet = TypeID(5)
)

// Initial version of the key
const InitialVersion = 1

// ErrNotFound is when the key is not found.
var ErrNotFound = errors.New("key not found")

// ErrKeyType is when the key already exists with a different type.
var ErrKeyType = errors.New("key type mismatch")

// ErrValueType is when the value does not have a valid type.
var ErrValueType = errors.New("invalid value type")

// ErrNotAllowed indicates that the operation is not allowed.
var ErrNotAllowed = errors.New("operation not allowed")

// Key represents a key data structure.
type Key struct {
	ID      int
	Key     string
	Type    TypeID
	Version int
	ETime   *int64
	MTime   int64
}

// Exists returns true if the key exists.
func (k Key) Exists() bool {
	return k.Key != ""
}

// TypeName returns the name of the key type.
func (k Key) TypeName() string {
	switch k.Type {
	case TypeString:
		return "string"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeHash:
		return "hash"
	case TypeSortedSet:
		return "zset"
	}
	return "unknown"
}

// Value represents a key value (a byte slice).
// It can be converted to other scalar types.
type Value []byte

func (v Value) String() string {
	return string(v)
}
func (v Value) Bytes() []byte {
	return v
}
func (v Value) Bool() (bool, error) {
	if !v.Exists() {
		return false, nil
	}
	return strconv.ParseBool(string(v))
}
func (v Value) MustBool() bool {
	b, err := v.Bool()
	if err != nil {
		panic(err)
	}
	return b
}
func (v Value) Int() (int, error) {
	if !v.Exists() {
		return 0, nil
	}
	return strconv.Atoi(string(v))
}
func (v Value) MustInt() int {
	i, err := v.Int()
	if err != nil {
		panic(err)
	}
	return i
}
func (v Value) Float() (float64, error) {
	if !v.Exists() {
		return 0, nil
	}
	return strconv.ParseFloat(string(v), 64)
}
func (v Value) MustFloat() float64 {
	f, err := v.Float()
	if err != nil {
		panic(err)
	}
	return f
}
func (v Value) Exists() bool {
	return len(v) != 0
}

// IsValueType returns true if the value has a valid type
// to be persisted in the database.
func IsValueType(v any) bool {
	switch v.(type) {
	case string, int, float64, bool, []byte:
		return true
	}
	return false
}
