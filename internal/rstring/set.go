package rstring

import (
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

// SetOut is the output of the Set command.
type SetOut struct {
	Prev    core.Value
	Created bool
	Updated bool
}

// SetCmd sets the key value.
type SetCmd struct {
	db          *DB
	tx          *Tx
	key         string
	val         any
	ttl         time.Duration
	at          time.Time
	keepTTL     bool
	ifExists    bool
	ifNotExists bool
}

// IfExists instructs to set the value only if the key exists.
func (c SetCmd) IfExists() SetCmd {
	c.ifExists = true
	c.ifNotExists = false
	return c
}

// IfNotExists instructs to set the value only if the key does not exist.
func (c SetCmd) IfNotExists() SetCmd {
	c.ifExists = false
	c.ifNotExists = true
	return c
}

// TTL sets the time-to-live for the value.
func (c SetCmd) TTL(ttl time.Duration) SetCmd {
	c.ttl = ttl
	c.at = time.Time{}
	c.keepTTL = false
	return c
}

// At sets the expiration time for the value.
func (c SetCmd) At(at time.Time) SetCmd {
	c.ttl = 0
	c.at = at
	c.keepTTL = false
	return c
}

// KeepTTL instructs to keep the expiration time already set for the key.
func (c SetCmd) KeepTTL() SetCmd {
	c.ttl = 0
	c.at = time.Time{}
	c.keepTTL = true
	return c
}

// Run sets the key value according to the configured options.
// Returns the previous value (if any) and the operation result
// (if the key was created or updated).
//
// Expiration time handling:
//   - If called with TTL() > 0 or At(), sets the expiration time.
//   - If called with KeepTTL(), keeps the expiration time already set for the key.
//   - If called without TTL(), At() or KeepTTL(), sets the value that will not expire.
//
// Existence checks:
//   - If called with IfExists(), sets the value only if the key exists.
//   - If called with IfNotExists(), sets the value only if the key does not exist.
func (c SetCmd) Run() (out SetOut, err error) {
	if c.db != nil {
		var out SetOut
		err := c.db.Update(func(tx *Tx) error {
			var err error
			out, err = c.run(tx.tx)
			return err
		})
		return out, err
	}
	if c.tx != nil {
		return c.run(c.tx.tx)
	}
	return SetOut{}, nil
}

func (c SetCmd) run(tx sqlx.Tx) (out SetOut, err error) {
	if !core.IsValueType(c.val) {
		return SetOut{}, core.ErrValueType
	}

	// Get the previous value.
	prev, err := get(tx, c.key)
	if err != nil && err != core.ErrNotFound {
		return SetOut{}, err
	}
	exists := err != core.ErrNotFound

	// Set the expiration time.
	if c.ttl > 0 {
		c.at = time.Now().Add(c.ttl)
	}

	// Special cases for exists / not exists checks.
	if c.ifExists && !exists {
		// only set if the key exists
		return SetOut{Prev: prev}, nil
	}
	if c.ifNotExists && exists {
		// only set if the key does not exist
		return SetOut{Prev: prev}, nil
	}

	// Set the value.
	if c.keepTTL {
		err = update(tx, c.key, c.val)
	} else {
		err = set(tx, c.key, c.val, c.at)
	}

	if err != nil {
		return SetOut{Prev: prev}, err
	}
	return SetOut{Prev: prev, Created: !exists, Updated: exists}, nil
}
