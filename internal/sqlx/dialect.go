package sqlx

import (
	"errors"
	"strconv"
	"strings"

	"github.com/nalgeon/redka/internal/core"
)

// SQL dialect.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSqlite   Dialect = "sqlite"
	DialectUnknown  Dialect = "unknown"
)

var ErrDialect = errors.New("unknown SQL dialect")

// ConstraintFailed checks if the error is due to
// a constraint violation on a column.
// Error examples:
//   - sqlite3.Error (NOT NULL constraint failed: rkey.type)
//   - *pq.Error (pq: null value in column "type" of relation "rkey" violates not-null constraint)
func (d Dialect) ConstraintFailed(err error, constraint, table string, column string) bool {
	var message string
	switch d {
	case DialectPostgres:
		message = `"` + column + `" of relation "` + table +
			`" violates ` + strings.ReplaceAll(constraint, " ", "-") + ` constraint`
	case DialectSqlite:
		message = constraint + " constraint failed: " + table + "." + column
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, message)
}

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

// Returns ErrKeyType if the error is due to a not-null
// constraint violation on rkey.type.
// Otherwise, returns the original error.
func (d Dialect) TypedError(err error) error {
	if err == nil {
		return nil
	}
	if d.ConstraintFailed(err, "not null", "rkey", "type") {
		return core.ErrKeyType
	}
	return err
}
