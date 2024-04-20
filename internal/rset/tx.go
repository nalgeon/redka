package rset

import (
	"database/sql"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"time"
)

var sqlAdd = []string{`
insert into rkey (key, type, version, etime, mtime)
values (:key, :type, :version, :etime, :mtime)
on conflict (key) do update set 
    version = version + 1,
    type = excluded.type,
    etime = excluded.etime,
    mtime = excluded.mtime
;`,

	`insert into rset (key_id, elem)
values ((select id from rkey where key = :key), :value)`,
}

// Tx is a set repository transaction.
type Tx struct {
	tx sqlx.Tx
}

func NewTx(tx sqlx.Tx) *Tx {
	return &Tx{tx}
}

// Add adds key elems to set.
func (t *Tx) Add(key string, elems ...any) (int, error) {
	return t.add(key, elems...)

}

func (t *Tx) add(key string, elems ...any) (int, error) {
	now := time.Now()

	var args [][]any
	for _, elem := range elems {
		args = append(args, []any{
			sql.Named("key", key),
			sql.Named("type", core.TypeSet),
			sql.Named("version", core.InitialVersion),
			sql.Named("elem", elem),
			sql.Named("etime", now),
			sql.Named("mtime", now.UnixMilli()),
		})
	}

	for _, arg := range args {
		_, err := t.tx.Exec(sqlAdd[0], arg)
		if err != nil {
			return 0, sqlx.TypedError(err)
		}
		_, err = t.tx.Exec(sqlAdd[1], arg)
		if err != nil {
			return 0, err
		}
	}

	return len(args), nil
}
