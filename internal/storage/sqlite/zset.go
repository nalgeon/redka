package sqlite

import (
	"database/sql"
	"slices"
	"strings"

	"github.com/nalgeon/redka/internal/sqlx"
	"github.com/nalgeon/redka/internal/storage"
)

type zsetStore struct{}

func (zsetStore) CountScore(tx sqlx.Tx, key string, now int64, min, max float64) (int, error) {
	var n int
	err := tx.QueryRow(zsql.countScore, key, now, min, max).Scan(&n)
	return n, err
}

func (zsetStore) DeleteElems(tx sqlx.Tx, key string, now int64, elems [][]byte) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}
	query, elemArgs := sqlx.ExpandIn(zsql.delete1, ":elems", elems)
	args := append([]any{key, now}, elemArgs...)
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, nil
	}
	_, err = tx.Exec(zsql.delete2, now, n, key, now)
	return int(n), err
}

func (zsetStore) UpdateKeyLen(tx sqlx.Tx, now int64, delta int, key string, now2 int64) error {
	_, err := tx.Exec(zsql.delete2, now, delta, key, now2)
	return err
}

func (zsetStore) DeleteAllPart1(tx sqlx.Tx, key string, now int64) error {
	_, err := tx.Exec(zsql.deleteAll1, key, now)
	return err
}

func (zsetStore) DeleteAllPart2(tx sqlx.Tx, key string, now int64) error {
	_, err := tx.Exec(zsql.deleteAll2, key, now)
	return err
}

func (zsetStore) DeleteByRank(tx sqlx.Tx, key string, now int64, limit, offset int) (int, error) {
	args := []any{key, now, limit, offset}
	res, err := tx.Exec(zsql.deleteRank, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (zsetStore) DeleteByScore(tx sqlx.Tx, key string, now int64, min, max float64) (int, error) {
	args := []any{key, now, min, max}
	res, err := tx.Exec(zsql.deleteScore, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (zsetStore) GetScore(tx sqlx.Tx, key string, now int64, elem []byte) (float64, error) {
	var score float64
	err := tx.QueryRow(zsql.getScore, key, now, elem).Scan(&score)
	return score, err
}

func (zsetStore) AddKey(tx sqlx.Tx, key string, now int64) (int, error) {
	var keyID int
	err := tx.QueryRow(zsql.add1, key, now).Scan(&keyID)
	return keyID, err
}

func (zsetStore) AddScore(tx sqlx.Tx, keyID int, elem []byte, score float64) error {
	_, err := tx.Exec(zsql.add2, keyID, elem, score)
	return err
}

func (zsetStore) IncrScore(tx sqlx.Tx, keyID int, elem []byte, delta float64) (float64, error) {
	var score float64
	err := tx.QueryRow(zsql.incr, keyID, elem, delta).Scan(&score)
	return score, err
}

func (zsetStore) Len(tx sqlx.Tx, key string, now int64) (int, error) {
	var n int
	err := tx.QueryRow(zsql.len, key, now).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}

func (zsetStore) Scan(tx sqlx.Tx, key string, now int64, cursor int, pattern string, limit int) ([]storage.ZSetElem, error) {
	args := []any{key, now, cursor, pattern, limit}
	scan := func(rows *sql.Rows) (storage.ZSetElem, error) {
		var z storage.ZSetElem
		var elem []byte
		err := rows.Scan(&z.RowID, &elem, &z.Score)
		z.Elem = elem
		return z, err
	}
	return sqlx.Select(tx, zsql.scan, args, scan)
}

func (zsetStore) CountElems(tx sqlx.Tx, key string, now int64, elems [][]byte) (int, error) {
	query, elemArgs := sqlx.ExpandIn(zsql.count, ":elems", elems)
	args := append([]any{key, now}, elemArgs...)
	var count int
	err := tx.QueryRow(query, args...).Scan(&count)
	return count, err
}

func (zsetStore) GetRank(tx sqlx.Tx, key string, now int64, elem []byte, sortAsc bool) (int, float64, error) {
	q := zsql.getRank
	sortDir := sqlx.Desc
	if sortAsc {
		sortDir = sqlx.Asc
	}
	if sortDir != sqlx.Asc {
		q = strings.Replace(q, sqlx.Asc, sortDir, 2)
	}
	var rank int
	var score float64
	err := tx.QueryRow(q, key, now, elem).Scan(&rank, &score)
	return rank, score, err
}

func (zsetStore) RangeRank(tx sqlx.Tx, key string, now int64, start, stop int, sortAsc bool) ([]storage.ZSetElem, error) {
	q := zsql.rangeRank
	sortDir := sqlx.Desc
	if sortAsc {
		sortDir = sqlx.Asc
	}
	if sortDir != sqlx.Asc {
		q = strings.Replace(q, sqlx.Asc, sortDir, 2)
	}
	rows, err := tx.Query(q, key, now, start, stop)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanZElems(rows)
}

func (zsetStore) RangeScore(tx sqlx.Tx, key string, now int64, min, max float64, sortAsc bool, offset, count int) ([]storage.ZSetElem, error) {
	q := zsql.rangeScore
	sortDir := sqlx.Desc
	if sortAsc {
		sortDir = sqlx.Asc
	}
	if sortDir != sqlx.Asc {
		q = strings.Replace(q, sqlx.Asc, sortDir, 2)
	}
	args := []any{key, now, min, max}
	if offset > 0 && count > 0 {
		q += " limit $5 offset $6"
		args = append(args, count, offset)
	} else if count > 0 {
		q += " limit $5"
		args = append(args, count)
	} else if offset > 0 {
		q += " limit -1 offset $5"
		args = append(args, offset)
	}
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanZElems(rows)
}

func (zsetStore) UnionRead(tx sqlx.Tx, keys []string, now int64, aggregate string) ([]storage.ZSetElem, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	q := zsql.union
	if aggregate != sqlx.Sum {
		q = strings.Replace(q, sqlx.Sum, aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(q, ":keys", keys)
	args := append(keyArgs, now)
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanZElems(rows)
}

func (zsetStore) UnionWrite(tx sqlx.Tx, dest string, keys []string, now int64, aggregate string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(zsql.deleteAll1, dest, now); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(zsql.deleteAll2, dest, now); err != nil {
		return 0, err
	}
	var destID int
	if err := tx.QueryRow(zsql.add1, dest, now).Scan(&destID); err != nil {
		return 0, err
	}
	q := zsql.unionStore
	if aggregate != sqlx.Sum {
		q = strings.Replace(q, sqlx.Sum, aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(q, ":keys", keys)
	args := slices.Concat([]any{destID}, keyArgs, []any{now})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (zsetStore) InterRead(tx sqlx.Tx, keys []string, now int64, aggregate string) ([]storage.ZSetElem, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	q := zsql.inter
	if aggregate != sqlx.Sum {
		q = strings.Replace(q, sqlx.Sum, aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(q, ":keys", keys)
	args := append(keyArgs, now, len(keys))
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanZElems(rows)
}

func (zsetStore) InterWrite(tx sqlx.Tx, dest string, keys []string, now int64, aggregate string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(zsql.deleteAll1, dest, now); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(zsql.deleteAll2, dest, now); err != nil {
		return 0, err
	}
	var destID int
	if err := tx.QueryRow(zsql.add1, dest, now).Scan(&destID); err != nil {
		return 0, err
	}
	q := zsql.interStore
	if aggregate != sqlx.Sum {
		q = strings.Replace(q, sqlx.Sum, aggregate, 2)
	}
	query, keyArgs := sqlx.ExpandIn(q, ":keys", keys)
	args := slices.Concat([]any{destID}, keyArgs, []any{now, len(keys)})
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func scanZElems(rows *sql.Rows) ([]storage.ZSetElem, error) {
	var items []storage.ZSetElem
	for rows.Next() {
		var z storage.ZSetElem
		var elem []byte
		if err := rows.Scan(&elem, &z.Score); err != nil {
			return nil, err
		}
		z.Elem = elem
		items = append(items, z)
	}
	return items, rows.Err()
}

var zsql = struct {
	add1, add2, count, countScore, delete1, delete2, deleteAll1, deleteAll2 string
	deleteRank, deleteScore, getRank, getScore, incr, inter, interStore     string
	len, rangeRank, rangeScore, scan, union, unionStore                     string
}{
	add1: `
	insert into
	rkey   (key, type, version, mtime, len)
	values ( $1,    5,       1,    $2,   0)
	on conflict (key) do update set
		type = case when rkey.type = excluded.type then rkey.type else null end,
		version = rkey.version + 1,
		mtime = excluded.mtime
	returning id`,

	add2: `
	insert into rzset (kid, elem, score)
	values ($1, $2, $3)
	on conflict (kid, elem) do update
	set score = excluded.score`,

	count: `
	select count(elem)
	from rzset join rkey on kid = rkey.id and type = 5
	where key = ? and (etime is null or etime > ?) and elem in (:elems)`,

	countScore: `
	select count(elem)
	from rzset join rkey on kid = rkey.id and type = 5
	where key = $1 and (etime is null or etime > $2) and score between $3 and $4`,

	delete1: `
	delete from rzset
	where kid = (
			select id from rkey
			where key = ? and type = 5 and (etime is null or etime > ?)
		) and elem in (:elems)`,

	delete2: `
	update rkey set
		version = version + 1,
		mtime = $1,
		len = len - $2
	where key = $3 and type = 5 and (etime is null or etime > $4)`,

	deleteAll1: `
	delete from rzset
	where kid = (
		select id from rkey
		where key = $1 and type = 5 and (etime is null or etime > $2)
	)`,

	deleteAll2: `
	update rkey set
		version = 0,
		mtime = 0,
		len = 0
	where key = $1 and type = 5 and (etime is null or etime > $2)`,

	deleteRank: `
	with ranked as (
		select rowid, elem, score
		from rzset
		where kid = (
			select id from rkey
			where key = $1 and type = 5 and (etime is null or etime > $2)
		)
		order by score, elem
		limit $3
		offset $4
	)
	delete from rzset
	where rowid in (select rowid from ranked)`,

	deleteScore: `
	delete from rzset
	where kid = (
			select id from rkey
			where key = $1 and type = 5 and (etime is null or etime > $2)
		) and score between $3 and $4`,

	getRank: `
	with ranked as (
		select elem, score, (row_number() over w - 1) as rank
		from rzset join rkey on kid = rkey.id and type = 5
		where key = $1 and (etime is null or etime > $2)
		window w as (partition by kid order by score asc, elem asc)
	)
	select rank, score
	from ranked
	where elem = $3`,

	getScore: `
	select score
	from rzset join rkey on kid = rkey.id and type = 5
	where key = $1 and (etime is null or etime > $2) and elem = $3`,

	incr: `
	insert into rzset (kid, elem, score)
	values ($1, $2, $3)
	on conflict (kid, elem) do update
	set score = rzset.score + excluded.score
	returning score`,

	inter: `
	select elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	having count(distinct kid) = ?
	order by sum(score), elem`,

	interStore: `
	insert into rzset (kid, elem, score)
	select ?, elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	having count(distinct kid) = ?
	order by sum(score), elem`,

	len: `
	select len from rkey
	where key = $1 and type = 5 and (etime is null or etime > $2)`,

	rangeRank: `
	with ranked as (
		select elem, score, (row_number() over w - 1) as rank
		from rzset join rkey on kid = rkey.id and type = 5
		where key = $1 and (etime is null or etime > $2)
		window w as (partition by kid order by score asc, elem asc)
	)
	select elem, score
	from ranked
	where rank between $3 and $4
	order by rank, elem asc`,

	rangeScore: `
	select elem, score
	from rzset join rkey on kid = rkey.id and type = 5
	where key = $1 and (etime is null or etime > $2)
	and score between $3 and $4
	order by score asc, elem asc`,

	scan: `
	select rzset.rowid, elem, score
	from rzset join rkey on kid = rkey.id and type = 5
	where
		key = $1 and (etime is null or etime > $2)
		and rzset.rowid > $3 and elem glob $4
	limit $5`,

	union: `
	select elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	order by sum(score), elem`,

	unionStore: `
	insert into rzset (kid, elem, score)
	select ?, elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	order by sum(score), elem`,
}

var _ storage.ZSetStorage = zsetStore{}
