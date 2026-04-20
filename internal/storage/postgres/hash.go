package postgres

import (
	"database/sql"

	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

type hashStore struct{}

func (hashStore) DeleteFields(tx sqlx.Tx, key string, now int64, fields []string) (int, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	query, fieldArgs := sqlx.ExpandIn(hashSQL.delete1, ":fields", fields)
	query = sqlx.DialectPostgres.Enumerate(query)
	args := append([]any{key, now}, fieldArgs...)
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, nil
	}
	_, err = tx.Exec(hashSQL.delete2, now, n, key, now)
	return int(n), err
}

func (hashStore) Fields(tx sqlx.Tx, key string, now int64) ([]string, error) {
	rows, err := tx.Query(hashSQL.fields, key, now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var fields []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return fields, rows.Err()
}

func (hashStore) Get(tx sqlx.Tx, key, field string, now int64) ([]byte, error) {
	var val []byte
	err := tx.QueryRow(hashSQL.get, key, now, field).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	}
	return val, err
}

func (hashStore) GetMany(tx sqlx.Tx, key string, now int64, fields []string) ([]storage.HashField, error) {
	query, fieldArgs := sqlx.ExpandIn(hashSQL.getMany, ":fields", fields)
	query = sqlx.DialectPostgres.Enumerate(query)
	args := append([]any{key, now}, fieldArgs...)
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []storage.HashField
	for rows.Next() {
		var hf storage.HashField
		if err := rows.Scan(&hf.Field, &hf.Value); err != nil {
			return nil, err
		}
		out = append(out, hf)
	}
	return out, rows.Err()
}

func (hashStore) Items(tx sqlx.Tx, key string, now int64) ([]storage.HashField, error) {
	rows, err := tx.Query(hashSQL.items, key, now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []storage.HashField
	for rows.Next() {
		var hf storage.HashField
		if err := rows.Scan(&hf.Field, &hf.Value); err != nil {
			return nil, err
		}
		out = append(out, hf)
	}
	return out, rows.Err()
}

func (hashStore) Len(tx sqlx.Tx, key string, now int64) (int, error) {
	var n int
	err := tx.QueryRow(hashSQL.len, key, now).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}

func (hashStore) Scan(tx sqlx.Tx, key string, now int64, cursor int, pattern string, limit int) ([]storage.HashScanRow, error) {
	pattern = sqlx.DialectPostgres.GlobToLike(pattern)
	args := []any{key, now, cursor, pattern, limit}
	scan := func(rows *sql.Rows) (storage.HashScanRow, error) {
		var r storage.HashScanRow
		err := rows.Scan(&r.RowID, &r.Field, &r.Value)
		return r, err
	}
	return sqlx.Select(tx, hashSQL.scan, args, scan)
}

func (hashStore) Values(tx sqlx.Tx, key string, now int64) ([][]byte, error) {
	rows, err := tx.Query(hashSQL.values, key, now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out [][]byte
	for rows.Next() {
		var v []byte
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (hashStore) CountFields(tx sqlx.Tx, key string, now int64, fields []string) (int, error) {
	query, fieldArgs := sqlx.ExpandIn(hashSQL.count, ":fields", fields)
	query = sqlx.DialectPostgres.Enumerate(query)
	args := append([]any{key, now}, fieldArgs...)
	var count int
	err := tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

func (hashStore) SetKey(tx sqlx.Tx, key string, now int64) (int, error) {
	var keyID int
	err := tx.QueryRow(hashSQL.set1, key, now).Scan(&keyID)
	return keyID, err
}

func (hashStore) SetField(tx sqlx.Tx, keyID int, field string, value []byte) error {
	_, err := tx.Exec(hashSQL.set2, keyID, field, value)
	return err
}

var hashSQL = struct {
	count, delete1, delete2, fields, get, getMany, items, len, scan, set1, set2, values string
}{
	count: `
	select count(field)
	from rhash join rkey on kid = rkey.id and type = 4
	where key = ? and (etime is null or etime > ?) and field in (:fields)`,

	delete1: `
	delete from rhash
	where kid = (
			select id from rkey
			where key = ? and type = 4 and (etime is null or etime > ?)
		) and field in (:fields)`,

	delete2: `
	update rkey set
		version = version + 1,
		mtime = $1,
		len = len - $2
	where key = $3 and type = 4 and (etime is null or etime > $4)`,

	fields: `
	select field
	from rhash join rkey on kid = rkey.id and type = 4
	where key = $1 and (etime is null or etime > $2)`,

	get: `
	select value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = $1 and (etime is null or etime > $2) and field = $3`,

	getMany: `
	select field, value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = ? and (etime is null or etime > ?) and field in (:fields)`,

	items: `
	select field, value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = $1 and (etime is null or etime > $2)`,

	len: `
	select len from rkey
	where key = $1 and type = 4 and (etime is null or etime > $2)`,

	scan: `
	select rhash.rowid, field, value
	from rhash join rkey on kid = rkey.id and type = 4
	where
		key = $1 and (etime is null or etime > $2)
		and rhash.rowid > $3 and field like $4
	order by rhash.rowid asc
	limit $5`,

	set1: `
	insert into
	rkey   (key, type, version, mtime, len)
	values ( $1,    4,       1,    $2,   0)
	on conflict (key) do update set
		type = case when rkey.type = excluded.type then rkey.type else null end,
		version = rkey.version + 1,
		mtime = excluded.mtime
	returning id`,

	set2: `
	insert into rhash (kid, field, value)
	values ($1, $2, $3)
	on conflict (kid, field) do update
	set value = excluded.value`,

	values: `
	select value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = $1 and (etime is null or etime > $2)`,
}

var _ storage.HashStorage = hashStore{}
