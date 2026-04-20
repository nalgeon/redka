package sqlite

import (
	"database/sql"
	"strings"

	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

type listStore struct{}

func (listStore) DeleteEqual(tx sqlx.Tx, key string, elem []byte, now int64) (int64, error) {
	res, err := tx.Exec(listSQL.delete, key, now, elem)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (listStore) DeleteBack(tx sqlx.Tx, key string, elem []byte, now int64, count int) (int64, error) {
	res, err := tx.Exec(listSQL.deleteBack, key, now, elem, count)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (listStore) DeleteFront(tx sqlx.Tx, key string, elem []byte, now int64, count int) (int64, error) {
	res, err := tx.Exec(listSQL.deleteFront, key, now, elem, count)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (listStore) Get(tx sqlx.Tx, key string, now int64, idx int, indexDesc bool) ([]byte, error) {
	q := listSQL.get
	if indexDesc {
		q = strings.Replace(q, sqlx.Asc, sqlx.Desc, 1)
	}
	var val []byte
	err := tx.QueryRow(q, key, now, idx).Scan(&val)
	return val, err
}

func (listStore) InsertBump(tx sqlx.Tx, now int64, key string, now2 int64) (int, int, error) {
	var keyID, n int
	err := tx.QueryRow(listSQL.insert, now, key, now2).Scan(&keyID, &n)
	return keyID, n, err
}

func (listStore) InsertAfter(tx sqlx.Tx, keyID int, pivot, elem []byte) error {
	_, err := tx.Exec(listSQL.insertAfter, keyID, pivot, keyID, keyID, elem, keyID)
	return err
}

func (listStore) InsertBefore(tx sqlx.Tx, keyID int, pivot, elem []byte) error {
	_, err := tx.Exec(listSQL.insertBefore, keyID, pivot, keyID, keyID, elem, keyID)
	return err
}

func (listStore) Len(tx sqlx.Tx, key string, now int64) (int, error) {
	var n int
	err := tx.QueryRow(listSQL.len, key, now).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}

func (listStore) PopBack(tx sqlx.Tx, key string, now int64) ([]byte, error) {
	var val []byte
	err := tx.QueryRow(listSQL.popBack, key, now).Scan(&val)
	return val, err
}

func (listStore) PopFront(tx sqlx.Tx, key string, now int64) ([]byte, error) {
	var val []byte
	err := tx.QueryRow(listSQL.popFront, key, now).Scan(&val)
	return val, err
}

func (listStore) PushUpsert(tx sqlx.Tx, key string, now int64) (int, int, error) {
	var keyID, n int
	err := tx.QueryRow(listSQL.push, key, now).Scan(&keyID, &n)
	return keyID, n, err
}

func (listStore) PushBack(tx sqlx.Tx, keyID int, elem []byte) error {
	_, err := tx.Exec(listSQL.pushBack, keyID, elem, keyID)
	return err
}

func (listStore) PushFront(tx sqlx.Tx, keyID int, elem []byte) error {
	_, err := tx.Exec(listSQL.pushFront, keyID, elem, keyID)
	return err
}

func (listStore) Range(tx sqlx.Tx, key string, now int64, start, stop int) ([][]byte, error) {
	args := []any{key, now, start, start, start, stop, stop, stop}
	rows, err := tx.Query(listSQL.lrange, args...)
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

func (listStore) Set(tx sqlx.Tx, key string, now int64, elem []byte, idx int, indexDesc bool) (int64, error) {
	q := listSQL.set
	if indexDesc {
		q = strings.Replace(q, sqlx.Asc, sqlx.Desc, 1)
	}
	res, err := tx.Exec(q, key, now, elem, idx)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (listStore) Trim(tx sqlx.Tx, key string, now int64, start, stop int) (int64, error) {
	args := []any{key, now, start, start, start, stop, stop, stop}
	res, err := tx.Exec(listSQL.trim, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

var listSQL = struct {
	delete, deleteBack, deleteFront, get, insert, insertAfter, insertBefore string
	len, popBack, popFront, push, pushBack, pushFront, lrange, set, trim    string
}{
	delete: `
	delete from rlist
	where kid = (
			select id from rkey
			where key = $1 and type = 2 and (etime is null or etime > $2)
		) and elem = $3`,

	deleteBack: `
	with ids as (
		select rlist.rowid
		from rlist join rkey on kid = rkey.id and type = 2
		where key = $1 and (etime is null or etime > $2) and elem = $3
		order by pos desc
		limit $4
	)
	delete from rlist
	where rowid in (select rowid from ids)`,

	deleteFront: `
	with ids as (
		select rlist.rowid
		from rlist join rkey on kid = rkey.id and type = 2
		where key = $1 and (etime is null or etime > $2) and elem = $3
		order by pos
		limit $4
	)
	delete from rlist
	where rowid in (select rowid from ids)`,

	get: `
	with elems as (
		select elem, row_number() over (order by pos asc) as rownum
		from rlist join rkey on kid = rkey.id and type = 2
		where key = $1 and (etime is null or etime > $2)
	)
	select elem
	from elems
	where rownum = $3 + 1`,

	insert: `
	update rkey set
		version = version + 1,
		mtime = $1,
		len = len + 1
	where key = $2 and type = 2 and (etime is null or etime > $3)
	returning id, len`,

	insertAfter: `
	with elprev as (
		select min(pos) as pos from rlist
		where kid = $1 and elem = $2
	),
	elnext as (
		select min(pos) as pos from rlist
		where kid = $3 and pos > (select pos from elprev)
	),
	newpos as (
		select
			case
				when elnext.pos is null then elprev.pos + 1
				else (elprev.pos + elnext.pos) / 2
			end as pos
		from elprev, elnext
	)
	insert into rlist (kid, pos, elem)
	select $4, (select pos from newpos), $5
	from rlist
	where kid = $6
	limit 1`,

	insertBefore: `
	with elnext as (
		select min(pos) as pos from rlist
		where kid = $1 and elem = $2
	),
	elprev as (
		select max(pos) as pos from rlist
		where kid = $3 and pos < (select pos from elnext)
	),
	newpos as (
		select
			case
				when elprev.pos is null then elnext.pos - 1
				else (elprev.pos + elnext.pos) / 2
			end as pos
		from elprev, elnext
	)
	insert into rlist (kid, pos, elem)
	select $4, (select pos from newpos), $5
	from rlist
	where kid = $6
	limit 1`,

	len: `
	select len from rkey
	where key = $1 and type = 2 and (etime is null or etime > $2)`,

	popBack: `
	with curkey as (
		select id from rkey
		where key = $1 and type = 2 and (etime is null or etime > $2)
	)
	delete from rlist
	where
		kid = (select id from curkey)
		and pos = (
			select max(pos) from rlist
			where kid = (select id from curkey)
		)
	returning elem`,

	popFront: `
	with curkey as (
		select id from rkey
		where key = $1 and type = 2 and (etime is null or etime > $2)
	)
	delete from rlist
	where
		kid = (select id from curkey)
		and pos = (
			select min(pos) from rlist
			where kid = (select id from curkey)
		)
	returning elem`,

	push: `
	insert into
	rkey   (key, type, version, mtime, len)
	values ( $1,    2,       1,    $2,   1)
	on conflict (key) do update set
		type = case when rkey.type = excluded.type then rkey.type else null end,
		version = rkey.version + 1,
		mtime = excluded.mtime,
		len = rkey.len + 1
	returning id, len`,

	pushBack: `
	insert into rlist (kid, pos, elem)
	select $1, coalesce(max(pos)+1, 0), $2
	from rlist
	where kid = $3`,

	pushFront: `
	insert into rlist (kid, pos, elem)
	select $1, coalesce(min(pos)-1, 0), $2
	from rlist
	where kid = $3`,

	lrange: `
	with curkey as (
		select id from rkey
		where key = $1 and type = 2 and (etime is null or etime > $2)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when $3 < 0
				then (select len from counts) + $4
				else $5
			end as start,
			case when $6 < 0
				then (select len from counts) + $7
				else $8
			end as stop
	)
	select elem
	from rlist
	where kid = (select id from curkey)
	order by pos
	limit ((select stop from bounds) - (select start from bounds) + 1)
	offset (select start from bounds)`,

	set: `
	with curkey as (
		select id from rkey
		where key = $1 and type = 2 and (etime is null or etime > $2)
    ),
    elems as (
		select pos, row_number() over (order by pos asc) as rownum
		from rlist
		where kid = (select id from curkey)
    )
    update rlist set elem = $3
    where kid = (select id from curkey)
		and pos = (select pos from elems where rownum = $4 + 1)`,

	trim: `
	with curkey as (
		select id from rkey
		where key = $1 and type = 2 and (etime is null or etime > $2)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when $3 < 0
				then (select len from counts) + $4
				else $5
			end as start,
			case when $6 < 0
				then (select len from counts) + $7
				else $8
			end as stop
	),
	remain as (
		select rowid from rlist
		where kid = (select id from curkey)
		order by pos
		limit ((select stop from bounds) - (select start from bounds) + 1)
		offset (select start from bounds)
	)
	delete from rlist
	where
		kid = (select id from curkey)
		and rowid not in (select rowid from remain)`,
}

var _ storage.ListStorage = listStore{}
