package sqlx

import (
	"database/sql"
	_ "embed"
	"net/url"
	"strings"
)

//go:embed sqlite.sql
var sqliteSchema string

// sqlitePragma is a set of default SQLite settings.
var sqlitePragma = map[string]string{
	"journal_mode": "wal",
	"synchronous":  "normal",
	"temp_store":   "memory",
	"mmap_size":    "268435456",
	"foreign_keys": "on",
}

// An SQLite database handle.
type sqlite DB

// openSqlite creates a new SQLite database handle.
// Creates the database schema if necessary.
func openSqlite(rw *sql.DB, ro *sql.DB, opts *Options) (*sqlite, error) {
	d, err := newSqlite(rw, ro, opts)
	if err != nil {
		return nil, err
	}
	err = d.createSchema()
	return d, err
}

// newSqlite creates a new SQLite database handle.
// Like openSqlite, but does not create the database schema.
func newSqlite(rw *sql.DB, ro *sql.DB, opts *Options) (*sqlite, error) {
	d := &sqlite{Dialect: DialectSqlite, RW: rw, RO: ro, Timeout: opts.Timeout}
	d.setNumConns(opts.ReadOnly)
	err := d.applySettings(opts.Pragma)
	return d, err
}

// setNumConns sets the number of connections.
func (d *sqlite) setNumConns(readOnly bool) {
	// For the read-only DB handle the number of open connections
	// should be equal to the number of idle connections. Otherwise,
	// the handle will keep opening and closing connections, severely
	// impacting the througput.
	//
	// Benchmarks show that setting nConns>2 does not significantly
	// improve throughput, so I'm not sure what the best value is.
	// For now, I'm setting it to 2-8 depending on the number of CPUs.
	nConns := suggestNumConns()
	d.RO.SetMaxOpenConns(nConns)
	d.RO.SetMaxIdleConns(nConns)

	if !readOnly {
		// SQLite allows only one writer at a time. Setting the maximum
		// number of DB connections to 1 for the read-write DB handle
		// is the best and fastest way to enforce this.
		d.RW.SetMaxOpenConns(1)
	}
}

// applySettings applies the database settings.
func (d *sqlite) applySettings(pragma map[string]string) error {
	// Ideally, we'd only set the pragmas in the connection string
	// (see [DataSource]), so we wouldn't need this function.
	// But since the mattn driver does not support setting pragmas
	// in the connection string, we also set them here.
	//
	// The correct way to set pragmas for the mattn driver is to
	// use the connection hook (see cmd/redka/main.go on how to do this).
	// But since we can't be sure the user does that, we also set them here.
	//
	// Unfortunately, setting pragmas using Exec only sets them for
	// a single connection. It's not a problem for d.RW (which has only
	// one connection), but it is for d.RO (which has multiple connections).
	// Still, it's better than nothing.
	//
	// See https://github.com/nalgeon/redka/issues/28 for more details.
	if pragma == nil {
		// If no pragmas are specified, use the default ones.
		pragma = sqlitePragma
	}

	if len(pragma) == 0 {
		// If there are no pragmas on purpose (empty map), don't do anything.
		return nil
	}

	var query strings.Builder
	for name, val := range pragma {
		query.WriteString("pragma ")
		query.WriteString(name)
		query.WriteString("=")
		query.WriteString(val)
		query.WriteString(";")
	}
	if _, err := d.RW.Exec(query.String()); err != nil {
		return err
	}
	if _, err := d.RO.Exec(query.String()); err != nil {
		return err
	}
	return nil
}

// createSchema creates the database schema.
func (d *sqlite) createSchema() error {
	_, err := d.RW.Exec(sqliteSchema)
	return err
}

// sqliteDataSource returns an SQLite connection string
// for a read-only or read-write mode.
func sqliteDataSource(path string, readOnly bool, pragma map[string]string) string {
	var ds string

	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	if source == ":memory:" {
		// This is an in-memory database, so we must either enable shared cache
		// (https://sqlite.org/sharedcache.html), which is discouraged,
		// or use the memdb VFS (https://sqlite.org/src/file?name=src/memdb.c).
		// https://github.com/ncruces/go-sqlite3/issues/94#issuecomment-2157679766
		ds = "file:/redka.db"
		params.Set("vfs", "memdb")
	} else {
		// This is a file-based database, it must have a "file:" prefix
		// for setting parameters (https://www.sqlite.org/c3ref/open.html).
		ds = source
		if !strings.HasPrefix(ds, "file:") {
			ds = "file:" + ds
		}
	}

	// sql.DB is concurrent-safe, so we don't need SQLite mutexes.
	params.Set("_mutex", "no")

	// Set the connection mode (writable or read-only).
	if readOnly {
		if params.Get("mode") != "memory" {
			// Enable read-only mode for read-only databases
			// (except for in-memory databases, which are always writable).
			// https://www.sqlite.org/c3ref/open.html
			params.Set("mode", "ro")
		}
	} else {
		// Enable IMMEDIATE transactions for writable databases.
		// https://www.sqlite.org/lang_transaction.html
		params.Set("_txlock", "immediate")
	}

	// Apply the pragma settings.
	// Some drivers (modernc and ncruces) setting passing pragmas
	// in the connection string, so we add them here.
	// The mattn driver does not support this, so it'll just ignore them.
	// For mattn driver, we have to set the pragmas in the connection hook.
	// (see cmd/redka/main.go on how to do this).
	for name, val := range pragma {
		params.Add("_pragma", name+"="+val)
	}

	return ds + "?" + params.Encode()
}
