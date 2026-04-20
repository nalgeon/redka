package sqlite

import (
	"database/sql"
	"slices"

	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

type setStore struct{}

func (setStore) EnsureSetKey(tx sqlx.Tx, key string, now int64) (int, error) {
	var keyID int
	err := tx.QueryRow(setSQL.add1, key, now).Scan(&keyID)
	return keyID, err
}

func (setStore) InsertSetElem(tx sqlx.Tx, keyID int, elem []byte) (bool, error) {
	var created int
	err := tx.QueryRow(setSQL.add2, keyID, elem).Scan(&created)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return created == 1, err
}

func (setStore) DeleteElems(tx sqlx.Tx, key string, now int64, elems [][]byte) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}
	query, elemArgs := sqlx.ExpandIn(setSQL.delete1, ":elems", elems)
	args := append([]any{key, now}, elemArgs...)
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, nil
	}
	_, err = tx.Exec(setSQL.delete2, now, n, key, now)
	return int(n), err
}

func (setStore) DiffElems(tx sqlx.Tx, key string, now int64, others []string) ([][]byte, error) {
	query, keyArgs := sqlx.ExpandIn(setSQL.diff, ":keys", others)
	args := append(keyArgs, now, key, now)
	return selectSetBytes(tx, query, args)
}

func (setStore) DiffStore(tx sqlx.Tx, destID int, firstKey string, otherKeys []string, now int64) (int, error) {
	if len(otherKeys) == 0 {
		res, err := tx.Exec(setSQL.clone, destID, firstKey, now)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		return int(n), nil
	}
	query, keyArgs := sqlx.ExpandIn(setSQL.diffStore, ":keys", otherKeys)
	args := append(keyArgs, now, destID, firstKey, now)
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (setStore) InterElems(tx sqlx.Tx, now int64, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	query, keyArgs := sqlx.ExpandIn(setSQL.inter, ":keys", keys)
	args := append(keyArgs, now, len(keys))
	return selectSetBytes(tx, query, args)
}

func (setStore) InterStore(tx sqlx.Tx, destID int, keys []string, now int64) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	query, keyArgs := sqlx.ExpandIn(setSQL.interStore, ":keys", keys)
	args := slices.Concat([]any{destID}, keyArgs, []any{now, len(keys)})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (setStore) Exists(tx sqlx.Tx, key string, now int64, elem []byte) (bool, error) {
	var exists bool
	err := tx.QueryRow(setSQL.exists, key, now, elem).Scan(&exists)
	return exists, err
}

func (setStore) Items(tx sqlx.Tx, key string, now int64) ([][]byte, error) {
	return selectSetBytes(tx, setSQL.items, []any{key, now})
}

func (setStore) Len(tx sqlx.Tx, key string, now int64) (int, error) {
	var n int
	err := tx.QueryRow(setSQL.len, key, now).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}

func (setStore) Pop(tx sqlx.Tx, key string, now int64) ([]byte, error) {
	var val []byte
	err := tx.QueryRow(setSQL.pop1, key, now).Scan(&val)
	return val, err
}

func (setStore) PopDecr(tx sqlx.Tx, key string, now int64) error {
	_, err := tx.Exec(setSQL.pop2, now, 1, key, now)
	return err
}

func (setStore) Random(tx sqlx.Tx, key string, now int64) ([]byte, error) {
	var val []byte
	err := tx.QueryRow(setSQL.random, key, now).Scan(&val)
	return val, err
}

func (setStore) Scan(tx sqlx.Tx, key string, now int64, cursor int, pattern string, limit int) ([]storage.SetScanRow, error) {
	args := []any{key, now, cursor, pattern, limit}
	scan := func(rows *sql.Rows) (storage.SetScanRow, error) {
		var r storage.SetScanRow
		err := rows.Scan(&r.RowID, &r.Elem)
		return r, err
	}
	return sqlx.Select(tx, setSQL.scan, args, scan)
}

func (setStore) UnionElems(tx sqlx.Tx, now int64, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	query, keyArgs := sqlx.ExpandIn(setSQL.union, ":keys", keys)
	args := append(keyArgs, now)
	return selectSetBytes(tx, query, args)
}

func (setStore) UnionStore(tx sqlx.Tx, destID int, keys []string, now int64) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	query, keyArgs := sqlx.ExpandIn(setSQL.unionStore, ":keys", keys)
	args := slices.Concat([]any{destID}, keyArgs, []any{now})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (setStore) CloneElems(tx sqlx.Tx, destID int, srcKey string, now int64) (int, error) {
	res, err := tx.Exec(setSQL.clone, destID, srcKey, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (setStore) DeleteKeyPart1(tx sqlx.Tx, key string, now int64) error {
	_, err := tx.Exec(setSQL.deleteKey1, key, now)
	return err
}

func (setStore) DeleteKeyPart2(tx sqlx.Tx, key string, now int64) error {
	_, err := tx.Exec(setSQL.deleteKey2, key, now)
	return err
}

func (setStore) AddKey(tx sqlx.Tx, key string, now int64) (int, error) {
	var keyID int
	err := tx.QueryRow(setSQL.add1, key, now).Scan(&keyID)
	return keyID, err
}

func selectSetBytes(tx sqlx.Tx, query string, args []any) ([][]byte, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out [][]byte
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

var setSQL = struct {
	add1, add2, clone, delete1, delete2, deleteKey1, deleteKey2 string
	diff, diffStore, exists, inter, interStore, items, len      string
	pop1, pop2, random, scan, union, unionStore                 string
}{
	add1: `
	insert into
	rkey   (key, type, version, mtime, len)
	values ( $1,    3,       1,    $2,   0)
	on conflict (key) do update set
		type = case when rkey.type = excluded.type then rkey.type else null end,
		version = rkey.version + 1,
		mtime = excluded.mtime
	returning id`,

	add2: `
	insert into
	rset   (kid, elem)
	values ( $1,   $2)
	on conflict (kid, elem) do nothing
	returning 1`,

	clone: `
	insert into rset (kid, elem)
	select $1, elem
	from rset join rkey on kid = rkey.id and type = 3
	where key = $2 and (etime is null or etime > $3)`,

	delete1: `
	delete from rset
	where kid = (
			select id from rkey
			where key = ? and type = 3 and (etime is null or etime > ?)
		) and elem in (:elems)`,

	delete2: `
	update rkey set
		version = version + 1,
		mtime = $1,
		len = len - $2
	where key = $3 and type = 3 and (etime is null or etime > $4)`,

	deleteKey1: `
	delete from rset
	where kid = (
		select id from rkey
		where key = $1 and type = 3 and (etime is null or etime > $2)
	)`,

	deleteKey2: `
	update rkey set
		version = 0,
		mtime = 0,
		len = 0
	where key = $1 and type = 3 and (etime is null or etime > $2)`,

	diff: `
	with others as (
		select elem
		from rset
		where kid in (
			select id from rkey
			where key in (:keys) and type = 3 and (etime is null or etime > ?)
		)
	)
	select elem
	from rset
	where kid = (
		select id from rkey
		where key = ? and type = 3 and (etime is null or etime > ?)
	)
	and elem not in (select elem from others)`,

	diffStore: `
	with others as (
		select elem
		from rset
		where kid in (
			select id from rkey
			where key in (:keys) and type = 3 and (etime is null or etime > ?)
		)
	)
	insert into rset (kid, elem)
	select ?, elem
	from rset
	where kid = (
		select id from rkey
		where key = ? and type = 3 and (etime is null or etime > ?)
	)
	and elem not in (select elem from others)`,

	exists: `
	select count(*)
	from rset join rkey on kid = rkey.id and type = 3
	where key = $1 and (etime is null or etime > $2) and elem = $3`,

	inter: `
	select elem
	from rset join rkey on kid = rkey.id and type = 3
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	having count(distinct kid) = ?`,

	interStore: `
	insert into rset (kid, elem)
	select ?, elem
	from rset join rkey on kid = rkey.id and type = 3
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	having count(distinct kid) = ?`,

	items: `
	select elem
	from rset join rkey on kid = rkey.id and type = 3
	where key = $1 and (etime is null or etime > $2)`,

	len: `
	select len from rkey
	where key = $1 and type = 3 and (etime is null or etime > $2)`,

	pop1: `
	with chosen as (
		select rset.rowid
		from rset join rkey on kid = rkey.id and type = 3
		where key = $1 and (etime is null or etime > $2)
		order by random() limit 1
	)
	delete from rset
	where rowid in (select rowid from chosen)
	returning elem`,

	pop2: `
	update rkey set
		version = version + 1,
		mtime = $1,
		len = len - $2
	where key = $3 and type = 3 and (etime is null or etime > $4)`,

	random: `
	select elem
	from rset join rkey on kid = rkey.id and type = 3
	where key = $1 and (etime is null or etime > $2)
	order by random() limit 1`,

	scan: `
	select rset.rowid, elem
	from rset join rkey on kid = rkey.id and type = 3
	where
		key = $1 and (etime is null or etime > $2)
		and rset.rowid > $3 and elem glob $4
	limit $5`,

	union: `
	select elem
	from rset join rkey on kid = rkey.id and type = 3
	where key in (:keys) and (etime is null or etime > ?)
	group by elem`,

	unionStore: `
	insert into rset (kid, elem)
	select ?, elem
	from rset join rkey on kid = rkey.id and type = 3
	where key in (:keys) and (etime is null or etime > ?)
	group by elem`,
}

var _ storage.SetStorage = setStore{}
