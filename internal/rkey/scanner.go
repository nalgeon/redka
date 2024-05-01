package rkey

import "github.com/nalgeon/redka/internal/core"

// Scanner is the iterator for keys.
// Stops when there are no more keys or an error occurs.
type Scanner struct {
	db       *Tx
	cursor   int
	pattern  string
	ktype    core.TypeID
	pageSize int
	index    int
	cur      core.Key
	keys     []core.Key
	err      error
}

func newScanner(db *Tx, pattern string, ktype core.TypeID, pageSize int) *Scanner {
	if pageSize == 0 {
		pageSize = scanPageSize
	}
	return &Scanner{
		db:       db,
		cursor:   0,
		pattern:  pattern,
		ktype:    ktype,
		pageSize: pageSize,
		index:    0,
		keys:     []core.Key{},
	}
}

// Scan advances to the next key, fetching keys from db as necessary.
// Returns false when there are no more keys or an error occurs.
func (sc *Scanner) Scan() bool {
	if sc.index >= len(sc.keys) {
		// Fetch a new page of keys.
		out, err := sc.db.Scan(sc.cursor, sc.pattern, sc.ktype, sc.pageSize)
		if err != nil {
			sc.err = err
			return false
		}
		sc.cursor = out.Cursor
		sc.keys = out.Keys
		sc.index = 0
		if len(sc.keys) == 0 {
			return false
		}
	}
	// Advance to the next key from the current page.
	sc.cur = sc.keys[sc.index]
	sc.index++
	return true
}

// Key returns the current key.
func (sc *Scanner) Key() core.Key {
	return sc.cur
}

// Err returns the first error encountered during iteration.
func (sc *Scanner) Err() error {
	return sc.err
}
