package rset

import (
	"database/sql"
	"github.com/nalgeon/redka/internal/sqlx"
)

// DB is a database-backed set repository.
// A set is a field-value map associated with a key.
// Use the set repository to work with individual hashmaps
// and their fields.
type DB struct {
	*sqlx.DB[*Tx]
}

// New connects to the set repository.
// Does not create the database schema.
func New(db *sql.DB) *DB {
	d := sqlx.New(db, NewTx)
	return &DB{d}
}

func (d *DB) Add(key string, elems ...any) (int, error) {
	tx := NewTx(d.SQL)
	return tx.Add(key, elems...)
}
