package sqlx

import (
	"context"
	"database/sql"
)

// Transactor is a domain transaction manager.
// T is the type of the domain transaction.
type Transactor[T any] struct {
	db    *DB                 // Database handle.
	newTx func(Dialect, Tx) T // Domain transaction constructor.
}

// NewTransactor creates a new transaction manager.
func NewTransactor[T any](db *DB, newTx func(Dialect, Tx) T) *Transactor[T] {
	return &Transactor[T]{db: db, newTx: newTx}
}

// Update executes a function within a writable transaction.
func (t *Transactor[T]) Update(f func(tx T) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.db.Timeout)
	defer cancel()
	return t.UpdateContext(ctx, f)
}

// UpdateContext executes a function within a writable transaction.
func (t *Transactor[T]) UpdateContext(ctx context.Context, f func(tx T) error) error {
	return t.execTx(ctx, true, f)
}

// View executes a function within a read-only transaction.
func (t *Transactor[T]) View(f func(tx T) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.db.Timeout)
	defer cancel()
	return t.ViewContext(ctx, f)
}

// ViewContext executes a function within a read-only transaction.
func (t *Transactor[T]) ViewContext(ctx context.Context, f func(tx T) error) error {
	return t.execTx(ctx, false, f)
}

// execTx executes a function within a transaction.
func (t *Transactor[T]) execTx(ctx context.Context, writable bool, f func(tx T) error) error {
	var sqlTx *sql.Tx
	var err error
	if writable {
		sqlTx, err = t.db.RW.BeginTx(ctx, nil)
	} else {
		sqlTx, err = t.db.RO.BeginTx(ctx, nil)
	}

	if err != nil {
		return err
	}
	defer func() { _ = sqlTx.Rollback() }()

	// Create a domain transaction from the database transaction,
	// then execute the function with it.
	tx := t.newTx(t.db.Dialect, sqlTx)
	err = f(tx)
	if err != nil {
		return err
	}
	return sqlTx.Commit()
}
