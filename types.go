// Common types and functions.
package redka

import (
	"errors"
	"strconv"
)

// type identifiers
type typeID int

const (
	typeString    = typeID(1)
	typeList      = typeID(2)
	typeSet       = typeID(3)
	typeHash      = typeID(4)
	typeSortedSet = typeID(5)
)

// initial version of the key
const initialVersion = 1

// ErrInvalidInt is when the value is not a valid integer.
var ErrInvalidInt = errors.New("invalid int")

// ErrInvalidFloat is when the value is not a valid float.
var ErrInvalidFloat = errors.New("invalid float")

// ErrKeyNotFound is when the key is not found.
var ErrKeyNotFound = errors.New("key not found")

// ErrInvalidType is when the value does not have a valid type.
var ErrInvalidType = errors.New("invalid type")

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value any
}

// Key represents a key data structure.
type Key struct {
	ID      int
	Key     string
	Type    typeID
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
	case typeString:
		return "string"
	case typeList:
		return "list"
	case typeSet:
		return "set"
	case typeHash:
		return "hash"
	case typeSortedSet:
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
	return []byte(v)
}
func (v Value) Bool() (bool, error) {
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
	return strconv.ParseFloat(string(v), 64)
}
func (v Value) MustFloat() float64 {
	f, err := v.Float()
	if err != nil {
		panic(err)
	}
	return f
}
func (v Value) IsEmpty() bool {
	return len(v) == 0
}

// isValueType returns true if the value has a valid type
// to be persisted in the database.
func isValueType(v any) bool {
	switch v.(type) {
	case string, int, float64, bool, []byte:
		return true
	}
	return false
}
