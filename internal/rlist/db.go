// Package rlist is a database-backed list repository.
// It provides methods to interact with lists in the database.
package rlist

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed list repository.
// A list is a sequence of strings ordered by insertion order.
// Use the list repository to work with lists and their elements.
type DB struct {
	*sqlx.DB[*Tx]
}

// New connects to the list repository.
// Does not create the database schema.
func New(rw *sql.DB, ro *sql.DB) *DB {
	d := sqlx.New(rw, ro, NewTx)
	return &DB{d}
}

// Delete deletes all occurrences of an element from a list.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (d *DB) Delete(key string, elem any) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.Delete(key, elem)
		return err
	})
	return n, err
}

// DeleteBack deletes the first count occurrences of an element
// from a list, starting from the back. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (d *DB) DeleteBack(key string, elem any, count int) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.DeleteBack(key, elem, count)
		return err
	})
	return n, err
}

// DeleteFront deletes the first count occurrences of an element
// from a list, starting from the front. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (d *DB) DeleteFront(key string, elem any, count int) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.DeleteFront(key, elem, count)
		return err
	})
	return n, err
}

// Get returns an element from a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) Get(key string, idx int) (core.Value, error) {
	tx := NewTx(d.RO)
	return tx.Get(key, idx)
}

// InsertAfter inserts an element after another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (d *DB) InsertAfter(key string, pivot, elem any) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.InsertAfter(key, pivot, elem)
		return err
	})
	return n, err
}

// InsertBefore inserts an element before another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (d *DB) InsertBefore(key string, pivot, elem any) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.InsertBefore(key, pivot, elem)
		return err
	})
	return n, err
}

// Len returns the number of elements in a list.
// If the key does not exist or is not a list, returns 0.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.RO)
	return tx.Len(key)
}

// PopBack removes and returns the last element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) PopBack(key string) (core.Value, error) {
	var elem core.Value
	err := d.Update(func(tx *Tx) error {
		var err error
		elem, err = tx.PopBack(key)
		return err
	})
	return elem, err
}

// PopBackPushFront removes the last element of a list
// and prepends it to another list (or the same list).
// If the source key does not exist or is not a list, returns ErrNotFound.
func (d *DB) PopBackPushFront(src, dest string) (core.Value, error) {
	var elem core.Value
	err := d.Update(func(tx *Tx) error {
		var err error
		elem, err = tx.PopBackPushFront(src, dest)
		return err
	})
	return elem, err
}

// PopFront removes and returns the first element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) PopFront(key string) (core.Value, error) {
	var elem core.Value
	err := d.Update(func(tx *Tx) error {
		var err error
		elem, err = tx.PopFront(key)
		return err
	})
	return elem, err
}

// PushBack appends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
func (d *DB) PushBack(key string, elem any) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.PushBack(key, elem)
		return err
	})
	return n, err
}

// PushFront prepends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
func (d *DB) PushFront(key string, elem any) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.PushFront(key, elem)
		return err
	})
	return n, err
}

// Range returns a range of elements from a list.
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the key does not exist or is not a list, returns an empty slice.
func (d *DB) Range(key string, start, stop int) ([]core.Value, error) {
	tx := NewTx(d.RO)
	return tx.Range(key, start, stop)
}

// Set sets an element in a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) Set(key string, idx int, elem any) error {
	err := d.Update(func(tx *Tx) error {
		return tx.Set(key, idx, elem)
	})
	return err
}

// Trim removes elements from both ends of a list so that
// only the elements between start and stop indexes remain.
// Returns the number of elements removed.
//
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
//
// Does nothing if the key does not exist or is not a list.
func (d *DB) Trim(key string, start, stop int) (int, error) {
	var n int
	err := d.Update(func(tx *Tx) error {
		var err error
		n, err = tx.Trim(key, start, stop)
		return err
	})
	return n, err
}
