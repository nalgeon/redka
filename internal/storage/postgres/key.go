package postgres

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

type keyStore struct{}

func (keyStore) Count(tx sqlx.Tx, keys []string, now int64) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	query, keyArgs := sqlx.ExpandIn(keySQL.count, ":keys", keys)
	query = sqlx.DialectPostgres.Enumerate(query)
	args := append(keyArgs, now)
	var count int
	err := tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

func (keyStore) Delete(tx sqlx.Tx, keys []string, now int64) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	query, keyArgs := sqlx.ExpandIn(keySQL.delete, ":keys", keys)
	query = sqlx.DialectPostgres.Enumerate(query)
	args := append(keyArgs, now)
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

func (keyStore) DeleteAll(tx sqlx.Tx) error {
	_, err := tx.Exec(keySQL.deleteAll)
	return err
}

func (keyStore) DeleteAllExpired(tx sqlx.Tx, now int64) (sql.Result, error) {
	return tx.Exec(keySQL.deleteAllExpired, now)
}

func (keyStore) DeleteNExpired(tx sqlx.Tx, now int64, n int) (sql.Result, error) {
	return tx.Exec(keySQL.deleteNExpired, now, n)
}

func (keyStore) Expire(tx sqlx.Tx, atMillis int64, key string, now int64) (sql.Result, error) {
	return tx.Exec(keySQL.expire, atMillis, key, now)
}

func (keyStore) Get(tx sqlx.Tx, key string, now int64) (core.Key, error) {
	args := []any{key, now}
	var k core.Key
	err := tx.QueryRow(keySQL.get, args...).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return core.Key{}, core.ErrNotFound
	}
	return k, err
}

func (keyStore) Keys(tx sqlx.Tx, pattern string, now int64) ([]core.Key, error) {
	pattern = sqlx.DialectPostgres.GlobToLike(pattern)
	args := []any{pattern, now}
	scan := func(rows *sql.Rows) (core.Key, error) {
		var k core.Key
		err := rows.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
		return k, err
	}
	return sqlx.Select(tx, keySQL.keys, args, scan)
}

func (keyStore) Len(tx sqlx.Tx) (int, error) {
	var n int
	err := tx.QueryRow(keySQL.len).Scan(&n)
	return n, err
}

func (keyStore) Persist(tx sqlx.Tx, key string, now int64) (sql.Result, error) {
	return tx.Exec(keySQL.persist, key, now)
}

func (keyStore) Random(tx sqlx.Tx, now int64) (core.Key, error) {
	var k core.Key
	err := tx.QueryRow(keySQL.random, now).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return core.Key{}, core.ErrNotFound
	}
	return k, err
}

func (keyStore) RenameDeleteByID(tx sqlx.Tx, id int) error {
	_, err := tx.Exec(keySQL.rename1, id)
	return err
}

func (keyStore) RenameUpdateKey(tx sqlx.Tx, newKey string, mtime int64, oldKey string, now int64) error {
	_, err := tx.Exec(keySQL.rename2, newKey, mtime, oldKey, now)
	return err
}

func (keyStore) Scan(tx sqlx.Tx, cursor int, pattern string, ktype int, ktypeAny bool, now int64, limit int) ([]core.Key, error) {
	pattern = sqlx.DialectPostgres.GlobToLike(pattern)
	args := []any{cursor, pattern, ktype, ktypeAny, now, limit}
	scan := func(rows *sql.Rows) (core.Key, error) {
		var k core.Key
		err := rows.Scan(&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime)
		return k, err
	}
	return sqlx.Select(tx, keySQL.scan, args, scan)
}

// keySQL holds SQLite statements for keys (same placeholders as original rkey/sqlite.go).
var keySQL = struct {
	count, delete, deleteAll, deleteAllExpired, deleteNExpired string
	expire, get, keys, len, persist, random                    string
	rename1, rename2, scan                                     string
}{
	count: `
	select count(id) from rkey
	where key in (:keys) and (etime is null or etime > ?)`,

	delete: `
	delete from rkey
	where key in (:keys) and (etime is null or etime > ?)`,

	deleteAll: `truncate table rkey cascade`,

	deleteAllExpired: `
	delete from rkey
	where etime <= $1`,

	deleteNExpired: `
	delete from rkey
	where ctid in (
		select ctid from rkey
		where etime <= $1
		limit $2
	)`,

	expire: `
	update rkey set
		version = version + 1,
		etime = $1
	where key = $2 and (etime is null or etime > $3)`,

	get: `
	select id, key, type, version, etime, mtime
	from rkey
	where key = $1 and (etime is null or etime > $2)`,

	keys: `
	select id, key, type, version, etime, mtime from rkey
	where key like $1 and (etime is null or etime > $2)`,

	len: `select count(*) from rkey`,

	persist: `
	update rkey set
		version = version + 1,
		etime = null
	where key = $1 and (etime is null or etime > $2)`,

	random: `
	select id, key, type, version, etime, mtime from rkey
	where etime is null or etime > $1
	order by random() limit 1`,

	rename1: `delete from rkey where id = $1`,

	rename2: `
	update rkey set
		key = $1,
		version = version + 1,
		mtime = $2
	where key = $3 and (etime is null or etime > $4)`,

	scan: `
	select id, key, type, version, etime, mtime from rkey
	where
		id > $1 and key like $2 and (type = $3 or $4)
		and (etime is null or etime > $5)
	order by id asc
	limit $6`,
}

var _ storage.KeyStorage = keyStore{}
