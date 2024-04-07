package redka

import (
	"context"
	"database/sql"
	"sync"
)

// sqlDB is a generic database-backed repository
// with a domain-specific transaction of type T.
type sqlDB[T any] struct {
	db *sql.DB
	// newT creates a new domain-specific transaction.
	newT func(sqlTx) T
	mu   sync.Mutex
}

// openSQL creates a new database-backed repository.
// Creates the database schema if necessary.
func openSQL[T any](db *sql.DB, newT func(sqlTx) T) (*sqlDB[T], error) {
	d := newSqlDB(db, newT)
	err := d.init()
	return d, err
}

// newSqlDB creates a new database-backed repository.
// Like openSQL, but does not create the database schema.
func newSqlDB[T any](db *sql.DB, newT func(sqlTx) T) *sqlDB[T] {
	d := &sqlDB[T]{db: db, newT: newT}
	return d
}

// Update executes a function within a writable transaction.
func (d *sqlDB[T]) Update(f func(tx T) error) error {
	return d.UpdateContext(context.Background(), f)
}

// UpdateContext executes a function within a writable transaction.
func (d *sqlDB[T]) UpdateContext(ctx context.Context, f func(tx T) error) error {
	return d.execTx(ctx, true, f)
}

// View executes a function within a read-only transaction.
func (d *sqlDB[T]) View(f func(tx T) error) error {
	return d.ViewContext(context.Background(), f)
}

// ViewContext executes a function within a read-only transaction.
func (d *sqlDB[T]) ViewContext(ctx context.Context, f func(tx T) error) error {
	return d.execTx(ctx, false, f)
}

// init creates the necessary tables.
func (d *sqlDB[T]) init() error {
	if _, err := d.db.Exec(sqlSettings); err != nil {
		return err
	}
	if _, err := d.db.Exec(sqlSchema); err != nil {
		return err
	}
	return nil
}

// execTx executes a function within a transaction.
func (d *sqlDB[T]) execTx(ctx context.Context, writable bool, f func(tx T) error) error {
	if writable {
		// only one writable transaction at a time
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	dtx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = dtx.Rollback() }()

	tx := d.newT(dtx)
	err = f(tx)
	if err != nil {
		return err
	}
	return dtx.Commit()
}
