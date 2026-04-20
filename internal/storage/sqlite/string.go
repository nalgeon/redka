package sqlite

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

type stringStore struct{}

func (stringStore) Get(tx sqlx.Tx, key string, now int64) ([]byte, error) {
	var val []byte
	err := tx.QueryRow(strSQL.get, key, now).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	}
	return val, err
}

func (stringStore) GetMany(tx sqlx.Tx, keys []string, now int64) ([]storage.StringKV, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	query, keyArgs := sqlx.ExpandIn(strSQL.getMany, ":keys", keys)
	args := append(keyArgs, now)
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []storage.StringKV
	for rows.Next() {
		var kv storage.StringKV
		if err := rows.Scan(&kv.Key, &kv.Val); err != nil {
			return nil, err
		}
		out = append(out, kv)
	}
	return out, rows.Err()
}

func (stringStore) SetKey(tx sqlx.Tx, key string, etime *int64, now int64) error {
	_, err := tx.Exec(strSQL.set1, key, etime, now)
	return err
}

func (stringStore) SetValue(tx sqlx.Tx, key string, value []byte) error {
	_, err := tx.Exec(strSQL.set2, key, value)
	return err
}

func (stringStore) UpdateKey(tx sqlx.Tx, key string, now int64) error {
	_, err := tx.Exec(strSQL.update1, key, now)
	return err
}

func (stringStore) UpdateValue(tx sqlx.Tx, key string, value []byte) error {
	_, err := tx.Exec(strSQL.update2, key, value)
	return err
}

var strSQL = struct{ get, getMany, set1, set2, update1, update2 string }{
	get: `
	select value
	from rstring join rkey on kid = rkey.id and type = 1
	where key = $1 and (etime is null or etime > $2)`,

	getMany: `
	select key, value
	from rstring
	join rkey on kid = rkey.id and type = 1
	where key in (:keys) and (etime is null or etime > ?)`,

	set1: `
	insert into
	rkey   (key, type, version, etime, mtime)
	values ( $1,    1,       1,    $2,    $3)
	on conflict (key) do update set
		type = case when rkey.type = excluded.type then rkey.type else null end,
		version = rkey.version + 1,
		etime = excluded.etime,
		mtime = excluded.mtime`,

	set2: `
	insert into rstring (kid, value)
	values ((select id from rkey where key = $1), $2)
	on conflict (kid) do update
	set value = excluded.value`,

	update1: `
	insert into
	rkey   (key, type, version, etime, mtime)
	values ( $1,    1,       1,  null,    $2)
	on conflict (key) do update set
		type = case when rkey.type = excluded.type then rkey.type else null end,
		version = rkey.version + 1,
		mtime = excluded.mtime`,

	update2: `
	insert into rstring (kid, value)
	values ((select id from rkey where key = $1), $2)
	on conflict (kid) do update
	set value = excluded.value`,
}

var _ storage.StringStorage = stringStore{}
