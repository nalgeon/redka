package rhash

// Scanner is the iterator for hash items.
// Stops when there are no more items or an error occurs.
type Scanner struct {
	db       *Tx
	key      string
	cursor   int
	pattern  string
	pageSize int
	index    int
	cur      HashItem
	items    []HashItem
	err      error
}

func newScanner(db *Tx, key string, pattern string, pageSize int) *Scanner {
	if pageSize == 0 {
		pageSize = scanPageSize
	}
	return &Scanner{
		db:       db,
		key:      key,
		cursor:   0,
		pattern:  pattern,
		pageSize: pageSize,
		index:    0,
		items:    []HashItem{},
	}
}

// Scan advances to the next item, fetching items from db as necessary.
// Returns false when there are no more items or an error occurs.
// Returns false if the key does not exist or is not a hash.
func (sc *Scanner) Scan() bool {
	if sc.index >= len(sc.items) {
		// Fetch a new page of items.
		out, err := sc.db.Scan(sc.key, sc.cursor, sc.pattern, sc.pageSize)
		if err != nil {
			sc.err = err
			return false
		}
		sc.cursor = out.Cursor
		sc.items = out.Items
		sc.index = 0
		if len(sc.items) == 0 {
			return false
		}
	}
	// Advance to the next item from the current page.
	sc.cur = sc.items[sc.index]
	sc.index++
	return true
}

// Item returns the current hash item.
func (sc *Scanner) Item() HashItem {
	return sc.cur
}

// Err returns the first error encountered during iteration.
func (sc *Scanner) Err() error {
	return sc.err
}
