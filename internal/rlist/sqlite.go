package rlist

// SQLite queries for the list repository.
var sqlite = queries{
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
