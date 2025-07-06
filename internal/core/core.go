// Package core provides the core types used by other Redka packages.
package core

import (
	"errors"
	"strconv"
)

// A TypeID identifies the type of the key and thus
// the data structure of the value with that key.
type TypeID int

const (
	TypeAny    = TypeID(0)
	TypeString = TypeID(1)
	TypeList   = TypeID(2)
	TypeSet    = TypeID(3)
	TypeHash   = TypeID(4)
	TypeZSet   = TypeID(5)
)

// Common errors returned by data structure methods.
var (
	ErrArgument   = errors.New("invalid argument")
	ErrKeyType    = errors.New("key type mismatch") // the key already exists with a different type
	ErrNotAllowed = errors.New("operation not allowed")
	ErrNotFound   = errors.New("key or elem not found")
	ErrValueType  = errors.New("invalid value type")
)

// Key represents a key data structure.
// Each key uniquely identifies a data structure stored in the
// database (e.g. a string, a list, or a hash). There can be only one
// data structure with a given key, regardless of type. For example,
// you can't have a string and a hash map with the same key.
type Key struct {
	ID      int
	Key     string
	Type    TypeID
	Version int    // incremented on each update
	ETime   *int64 // expiration time in unix milliseconds
	MTime   int64  // last modification time in unix milliseconds
}

// Exists reports whether the key exists.
// Returns false for expired keys.
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
	case TypeZSet:
		return "zset"
	}
	return "unknown"
}

// Value represents a value stored in a database (a byte slice).
// It can be converted to other scalar types.
type Value []byte

// String returns the value as a string.
func (v Value) String() string {
	return string(v)
}

// Bytes returns the value as a byte slice.
func (v Value) Bytes() []byte {
	return v
}

// Bool returns the value as a boolean.
func (v Value) Bool() (bool, error) {
	if v.IsZero() {
		return false, nil
	}
	return strconv.ParseBool(string(v))
}

// MustBool returns the value as a boolean, and panics if the value
// is not a valid boolean. Use this method only if you are sure of
// the value type.
func (v Value) MustBool() bool {
	b, err := v.Bool()
	if err != nil {
		panic(err)
	}
	return b
}

// Int returns the value as an integer.
func (v Value) Int() (int, error) {
	if v.IsZero() {
		return 0, nil
	}
	return strconv.Atoi(string(v))
}

// MustInt returns the value as an integer, and panics if the value
// is not a valid integer. Use this method only if you are sure of
// the value type.
func (v Value) MustInt() int {
	i, err := v.Int()
	if err != nil {
		panic(err)
	}
	return i
}

// Float returns the value as a float64.
func (v Value) Float() (float64, error) {
	if v.IsZero() {
		return 0, nil
	}
	return strconv.ParseFloat(string(v), 64)
}

// MustFloat returns the value as a float64, and panics if the value
// is not a valid float. Use this method only if you are sure of
// the value type.
func (v Value) MustFloat() float64 {
	f, err := v.Float()
	if err != nil {
		panic(err)
	}
	return f
}

// IsZero reports whether the value is empty.
func (v Value) IsZero() bool {
	return len(v) == 0
}

// IsValueType reports if the value has a valid type to be persisted
// in the database. Only the following types are allowed:
//   - string
//   - integer
//   - float
//   - boolean
//   - byte slice
func IsValueType(v any) bool {
	switch v.(type) {
	case bool, float64, int, string, []byte:
		return true
	}
	return false
}

// ToBytesMany converts multiple values to byte slices.
func ToBytesMany(values ...any) ([][]byte, error) {
	blobs := make([][]byte, len(values))
	for i, v := range values {
		b, err := ToBytes(v)
		if err != nil {
			return nil, err
		}
		blobs[i] = b
	}
	return blobs, nil
}

// ToBytes converts a value to a byte slice.
func ToBytes(v any) ([]byte, error) {
	switch v := v.(type) {
	case bool:
		if v {
			return []byte{'1'}, nil
		} else {
			return []byte{'0'}, nil
		}
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case int:
		return []byte(strconv.Itoa(v)), nil
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	}
	return nil, ErrValueType
}
