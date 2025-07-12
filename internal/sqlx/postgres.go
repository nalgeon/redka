package sqlx

import (
	"database/sql"
	_ "embed"
	"net/url"
	"strings"
)

//go:embed postgres.sql
var postgresSchema string

// A Postgres database handle.
type postgres DB

// openPostgres creates a new Postgres database handle.
// Creates the database schema if necessary.
func openPostgres(rw *sql.DB, ro *sql.DB, opts *Options) (*postgres, error) {
	d, err := newPostgres(rw, ro, opts)
	if err != nil {
		return nil, err
	}
	err = d.createSchema()
	return d, err
}

// newPostgres creates a new Postgres database handle.
// Like openPostgres, but does not create the database schema.
func newPostgres(rw *sql.DB, ro *sql.DB, opts *Options) (*postgres, error) {
	d := &postgres{Dialect: DialectPostgres, RW: rw, RO: ro, Timeout: opts.Timeout}
	d.setNumConns(opts.ReadOnly)
	return d, nil
}

// setNumConns sets the number of connections.
func (d *postgres) setNumConns(readOnly bool) {
	// The number of open connections
	// should be equal to the number of idle connections. Otherwise,
	// the handle will keep opening and closing connections, severely
	// impacting the througput.
	nConns := suggestNumConns()
	d.RO.SetMaxOpenConns(nConns)
	d.RO.SetMaxIdleConns(nConns)
	if !readOnly {
		d.RW.SetMaxOpenConns(nConns)
		d.RW.SetMaxIdleConns(nConns)
	}
}

// createSchema creates the database schema.
func (d *postgres) createSchema() error {
	_, err := d.RW.Exec(postgresSchema)
	return err
}

// postgresDataSource returns a Postgres connection string
// for a read-only or read-write mode.
func postgresDataSource(path string, readOnly bool, pragma map[string]string) string {
	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	// Set the connection mode (writable or read-only).
	if readOnly {
		params.Set("default_transaction_read_only", "on")
	}

	// Apply the pragma settings.
	for name, val := range pragma {
		params.Add(name, val)
	}

	// Return the connection string.
	if len(params) == 0 {
		return source
	}
	return source + "?" + params.Encode()
}
