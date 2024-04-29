package rlist

import (
	"database/sql"
	"strings"
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/sqlx"
)

const (
	sqlDelete = `
	delete from rlist
	where key_id = (
		select id from rkey where key = :key
		and (etime is null or etime > :now)
	) and elem = :elem`

	sqlDeleteBack = `
	with ids as (
		select rlist.rowid
		from rlist
			join rkey on key_id = rkey.id and (etime is null or etime > :now)
		where key = :key and elem = :elem
		order by pos desc
		limit :count
	)
	delete from rlist
	where rowid in (select rowid from ids)`

	sqlDeleteFront = `
	with ids as (
		select rlist.rowid
		from rlist
			join rkey on key_id = rkey.id and (etime is null or etime > :now)
		where key = :key and elem = :elem
		order by pos
		limit :count
	)
	delete from rlist
	where rowid in (select rowid from ids)`

	sqlGet = `
	with elems as (
		select elem, row_number() over (order by pos asc) as rownum
		from rlist
			join rkey on key_id = rkey.id and (etime is null or etime > :now)
		where key = :key
	)
	select elem
	from elems
	where rownum = :idx + 1`

	sqlInsertAfter = `
	with keyid as (
		select id from rkey
		where key = :key and (etime is null or etime > :now)
	),
	elprev as (
		select min(pos) as pos from rlist
		where key_id = (select id from keyid) and elem = :pivot
	),
	elnext as (
		select min(pos) as pos from rlist
		where key_id = (select id from keyid) and pos > (select pos from elprev)
	),
	newpos as (
		select
			case
				when elnext.pos is null then elprev.pos + 1
				else (elprev.pos + elnext.pos) / 2
			end as pos
		from elprev, elnext
	)
	insert into rlist (key_id, pos, elem)
	select (select id from keyid), (select pos from newpos), :elem
	from rlist
	where key_id = (select id from keyid)
	limit 1
	returning (
		select count(*) from rlist
		where key_id = (select id from keyid)
	)`

	sqlInsertBefore = `
	with keyid as (
		select id from rkey
		where key = :key and (etime is null or etime > :now)
	),
	elnext as (
		select min(pos) as pos from rlist
		where key_id = (select id from keyid) and elem = :pivot
	),
	elprev as (
		select max(pos) as pos from rlist
		where key_id = (select id from keyid) and pos < (select pos from elnext)
	),
	newpos as (
		select
			case
				when elprev.pos is null then elnext.pos - 1
				else (elprev.pos + elnext.pos) / 2
			end as pos
		from elprev, elnext
	)
	insert into rlist (key_id, pos, elem)
	select (select id from keyid), (select pos from newpos), :elem
	from rlist
	where key_id = (select id from keyid)
	limit 1
	returning (
		select count(*) from rlist
		where key_id = (select id from keyid)
	)`

	sqlLen = `
	select count(*)
	from rlist
		join rkey on key_id = rkey.id and (etime is null or etime > :now)
	where key = :key`

	sqlPopBack = `
	with keyid as (
		select id from rkey
		where key = :key and (etime is null or etime > :now)
	)
	delete from rlist
	where
		key_id = (select id from keyid)
		and pos = (
			select max(pos) from rlist
			where key_id = (select id from keyid)
		)
	returning elem`

	sqlPopFront = `
	with keyid as (
		select id from rkey
		where key = :key and (etime is null or etime > :now)
	)
	delete from rlist
	where
		key_id = (select id from keyid)
		and pos = (
			select min(pos) from rlist
			where key_id = (select id from keyid)
		)
	returning elem`

	sqlPushBack = `
	insert into rlist (key_id, pos, elem)
	select :key_id, coalesce(max(pos)+1, 0), :elem
	from rlist
	where key_id = :key_id
	returning (
		select count(*) from rlist
		where key_id = :key_id
	)`

	sqlPushFront = `
	insert into rlist (key_id, pos, elem)
	select :key_id, coalesce(min(pos)-1, 0), :elem
	from rlist
	where key_id = :key_id
	returning (
		select count(*) from rlist
		where key_id = :key_id
	)`

	sqlRange = `
	with keyid as (
		select id from rkey
		where key = :key and (etime is null or etime > :now)
	),
	counts as (
		select count(*) as n_elem from rlist
		where key_id = (select id from keyid)
	),
	bounds as (
		select
			case when :start < 0
				then (select n_elem from counts) + :start
				else :start
			end as start,
			case when :stop < 0
				then (select n_elem from counts) + :stop
				else :stop
			end as stop
	)
	select elem
	from rlist
	where key_id = (select id from keyid)
	order by pos
	limit
		(select start from bounds),
		((select stop from bounds) - (select start from bounds) + 1)`

	sqlSet = `
	with keyid as (
		select id from rkey
		where key = :key and (etime is null or etime > :now)
    ),
    elems as (
		select pos, row_number() over (order by pos asc) as rownum
		from rlist
		where key_id = (select id from keyid)
    )
    update rlist set elem = :elem
    where key_id = (select id from keyid)
		and pos = (select pos from elems where rownum = :idx + 1)`

	sqlSetKey = `
	insert into rkey (key, type, version, mtime)
	values (:key, :type, :version, :mtime)
	on conflict (key) do update set
		version = version+1,
		type = excluded.type,
		mtime = excluded.mtime
	returning id`

	sqlTrim = `
	with keyid as (
		select id from rkey
		where key = :key and (etime is null or etime > :now)
	),
	counts as (
		select count(*) as n_elem from rlist
		where key_id = (select id from keyid)
	),
	bounds as (
		select
			case when :start < 0
				then (select n_elem from counts) + :start
				else :start
			end as start,
			case when :stop < 0
				then (select n_elem from counts) + :stop
				else :stop
			end as stop
	),
	remain as (
		select rowid from rlist
		where key_id = (select id from keyid)
		order by pos
		limit
			(select start from bounds),
			((select stop from bounds) - (select start from bounds) + 1)
	)
	delete from rlist
	where
		key_id = (select id from keyid)
		and rowid not in (select rowid from remain)`
)

// Tx is a list repository transaction.
type Tx struct {
	tx sqlx.Tx
}

// NewTx creates a list repository transaction
// from a generic database transaction.
func NewTx(tx sqlx.Tx) *Tx {
	return &Tx{tx}
}

// Delete deletes all occurrences of an element from a list.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) Delete(key string, elem any) (int, error) {
	args := []any{key, time.Now().UnixMilli(), elem}
	res, err := tx.tx.Exec(sqlDelete, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// DeleteBack deletes the first count occurrences of an element
// from a list, starting from the back. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteBack(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, sqlDeleteBack)
}

// DeleteFront deletes the first count occurrences of an element
// from a list, starting from the front. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteFront(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, sqlDeleteFront)
}

// Get returns an element from a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) Get(key string, idx int) (core.Value, error) {
	var query = sqlGet
	if idx < 0 {
		// Reverse the query ordering and index, e.g.:
		//  - [11 12 13 14], idx = -1 <-> [14 13 12 11], idx = 0 (14)
		//  - [11 12 13 14], idx = -2 <-> [14 13 12 11], idx = 1 (13)
		//  - [11 12 13 14], idx = -3 <-> [14 13 12 11], idx = 2 (12)
		//  - etc.
		query = strings.Replace(query, sqlx.Asc, sqlx.Desc, 1)
		idx = -idx - 1
	}

	var val []byte
	args := []any{time.Now().UnixMilli(), key, idx}
	err := tx.tx.QueryRow(query, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// InsertAfter inserts an element after another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (tx *Tx) InsertAfter(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, sqlInsertAfter)
}

// InsertBefore inserts an element before another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (tx *Tx) InsertBefore(key string, pivot, elem any) (int, error) {
	return tx.insert(key, pivot, elem, sqlInsertBefore)
}

// Len returns the number of elements in a list.
// If the key does not exist or is not a list, returns 0.
func (tx *Tx) Len(key string) (int, error) {
	var count int
	args := []any{time.Now().UnixMilli(), key}
	err := tx.tx.QueryRow(sqlLen, args...).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// PopBack removes and returns the last element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBack(key string) (core.Value, error) {
	return tx.pop(key, sqlPopBack)
}

// PopBackPushFront removes the last element of a list
// and prepends it to another list (or the same list).
// If the source key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBackPushFront(src, dest string) (core.Value, error) {
	// Pop the last element from the source list.
	elem, err := tx.PopBack(src)
	if err == core.ErrNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Prepend the element to the destination list.
	_, err = tx.PushFront(dest, elem.Bytes())
	return elem, err
}

// PopFront removes and returns the first element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopFront(key string) (core.Value, error) {
	return tx.pop(key, sqlPopFront)
}

// PushBack appends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
func (tx *Tx) PushBack(key string, elem any) (int, error) {
	return tx.push(key, elem, sqlPushBack)
}

// PushFront prepends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
func (tx *Tx) PushFront(key string, elem any) (int, error) {
	return tx.push(key, elem, sqlPushFront)
}

// Range returns a range of elements from a list.
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the key does not exist or is not a list, returns an empty slice.
func (tx *Tx) Range(key string, start, stop int) ([]core.Value, error) {
	if (start > stop) && (start > 0 && stop > 0 || start < 0 && stop < 0) {
		return nil, nil
	}

	args := []any{key, time.Now().UnixMilli(), start, stop}
	rows, err := tx.tx.Query(sqlRange, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []core.Value
	for rows.Next() {
		var val []byte
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		values = append(values, core.Value(val))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

// Set sets an element in a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) Set(key string, idx int, elem any) error {
	if !core.IsValueType(elem) {
		return core.ErrValueType
	}

	var query = sqlSet
	if idx < 0 {
		// Reverse the query ordering and index.
		query = strings.Replace(query, sqlx.Asc, sqlx.Desc, 1)
		idx = -idx - 1
	}

	args := []any{key, time.Now().UnixMilli(), elem, idx}
	out, err := tx.tx.Exec(query, args...)
	if err != nil {
		return err
	}
	n, _ := out.RowsAffected()
	if n == 0 {
		return core.ErrNotFound
	}
	return err
}

// Trim removes elements from both ends of a list so that
// only the elements between start and stop indexes remain.
// Returns the number of elements removed.
//
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
//
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) Trim(key string, start, stop int) (int, error) {
	args := []any{key, time.Now().UnixMilli(), start, stop}
	out, err := tx.tx.Exec(sqlTrim, args...)
	if err != nil {
		return 0, err
	}
	n, _ := out.RowsAffected()
	return int(n), nil
}

// delete removes the first count occurrences of an element
// from a list, starting from the front or back.
func (tx *Tx) delete(key string, elem any, count int, query string) (int, error) {
	if count <= 0 {
		return 0, nil
	}

	args := []any{time.Now().UnixMilli(), key, elem, count}
	res, err := tx.tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// insert inserts an element before or after a pivot in a list.
func (tx *Tx) insert(key string, pivot, elem any, query string) (int, error) {
	if !core.IsValueType(elem) {
		return 0, core.ErrValueType
	}

	args := []any{key, time.Now().UnixMilli(), pivot, elem}
	var count int
	err := tx.tx.QueryRow(query, args...).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, core.ErrNotFound
	}
	if err != nil {
		if err.Error() == "NOT NULL constraint failed: rlist.pos" {
			return -1, core.ErrNotFound
		}
		return 0, err
	}
	return count, nil
}

// pop removes and returns an element from the front or back of a list.
func (tx *Tx) pop(key string, query string) (core.Value, error) {
	var val []byte
	args := []any{key, time.Now().UnixMilli()}
	err := tx.tx.QueryRow(query, args...).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(val), nil
}

// push inserts an element to the front or back of a list.
func (tx *Tx) push(key string, elem any, query string) (int, error) {
	if !core.IsValueType(elem) {
		return 0, core.ErrValueType
	}

	// Set the key if it does not exist.
	args := []any{
		key,                    // key
		core.TypeList,          // type
		core.InitialVersion,    // version
		time.Now().UnixMilli(), // mtime
	}
	var keyID int
	err := tx.tx.QueryRow(sqlSetKey, args...).Scan(&keyID)
	if err != nil {
		return 0, err
	}

	// Insert the element.
	var count int
	args = []any{keyID, elem, keyID, keyID}
	err = tx.tx.QueryRow(query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
