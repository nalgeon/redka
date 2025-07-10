package sqlx

import (
	"errors"
	"strconv"
	"strings"
)

// SQL dialect.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSqlite   Dialect = "sqlite"
	DialectUnknown  Dialect = "unknown"
)

var ErrDialect = errors.New("unknown SQL dialect")

// Enumerate replaces ? placeholders with $1, $2, ... $n.
func (d Dialect) Enumerate(query string) string {
	if d == DialectSqlite {
		// SQLite supports ? placeholders.
		return query
	}
	// Replace ? with $1, $2, ... $n placeholders.
	var b strings.Builder
	var phIdx int
	for _, char := range query {
		if char == '?' {
			phIdx++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(phIdx))
		} else {
			b.WriteRune(char)
		}
	}
	return b.String()
}

// GlobToLike creates a like-style pattern from a glob-style pattern.
// Only supports * and ? wildcards, not [abc] and [!abc].
// Escapes % and _ special characters with a backslash.
func (d Dialect) GlobToLike(pattern string) string {
	if d == DialectSqlite {
		// SQLite supports glob-style patterns.
		return pattern
	}
	var b strings.Builder
	for _, char := range pattern {
		switch char {
		case '*':
			b.WriteByte('%')
		case '?':
			b.WriteByte('_')
		case '%', '_', '\\':
			b.WriteByte('\\')
			b.WriteRune(char)
		default:
			b.WriteRune(char)
		}
	}
	return b.String()
}

// LimitAll returns a SQL query fragment to limit the result to all rows.
func (d Dialect) LimitAll() string {
	if d == DialectSqlite {
		return "limit -1"
	}
	return "limit all"
}
