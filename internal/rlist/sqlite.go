package rlist

var sqlite = queries{
	delete: `
	delete from rlist
	where kid = (
			select id from rkey
			where key = ? and type = 2 and (etime is null or etime > ?)
		) and elem = ?`,

	deleteBack: `
	with ids as (
		select rlist.rowid
		from rlist join rkey on kid = rkey.id and type = 2
		where key = ? and (etime is null or etime > ?) and elem = ?
		order by pos desc
		limit ?
	)
	delete from rlist
	where rowid in (select rowid from ids)`,

	deleteFront: `
	with ids as (
		select rlist.rowid
		from rlist join rkey on kid = rkey.id and type = 2
		where key = ? and (etime is null or etime > ?) and elem = ?
		order by pos
		limit ?
	)
	delete from rlist
	where rowid in (select rowid from ids)`,

	get: `
	with elems as (
		select elem, row_number() over (order by pos asc) as rownum
		from rlist join rkey on kid = rkey.id and type = 2
		where key = ? and (etime is null or etime > ?)
	)
	select elem
	from elems
	where rownum = ? + 1`,

	insert: `
	update rkey set
		version = version + 1,
		mtime = ?,
		len = len + 1
	where key = ? and type = 2 and (etime is null or etime > ?)
	returning id, len`,

	insertAfter: `
	with elprev as (
		select min(pos) as pos from rlist
		where kid = ? and elem = ?
	),
	elnext as (
		select min(pos) as pos from rlist
		where kid = ? and pos > (select pos from elprev)
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
	select ?, (select pos from newpos), ?
	from rlist
	where kid = ?
	limit 1`,

	insertBefore: `
	with elnext as (
		select min(pos) as pos from rlist
		where kid = ? and elem = ?
	),
	elprev as (
		select max(pos) as pos from rlist
		where kid = ? and pos < (select pos from elnext)
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
	select ?, (select pos from newpos), ?
	from rlist
	where kid = ?
	limit 1`,

	len: `
	select len from rkey
	where key = ? and type = 2 and (etime is null or etime > ?)`,

	popBack: `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
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
		where key = ? and type = 2 and (etime is null or etime > ?)
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
	values (  ?,    2,       1,     ?,   1)
	on conflict (key) do update set
		type = case when type = excluded.type then type else null end,
		version = version + 1,
		mtime = excluded.mtime,
		len = len + 1
	returning id, len`,

	pushBack: `
	insert into rlist (kid, pos, elem)
	select ?, coalesce(max(pos)+1, 0), ?
	from rlist
	where kid = ?`,

	pushFront: `
	insert into rlist (kid, pos, elem)
	select ?, coalesce(min(pos)-1, 0), ?
	from rlist
	where kid = ?`,

	lrange: `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as start,
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as stop
	)
	select elem
	from rlist
	where kid = (select id from curkey)
	order by pos
	limit
		(select start from bounds),
		((select stop from bounds) - (select start from bounds) + 1)`,

	set: `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
    ),
    elems as (
		select pos, row_number() over (order by pos asc) as rownum
		from rlist
		where kid = (select id from curkey)
    )
    update rlist set elem = ?
    where kid = (select id from curkey)
		and pos = (select pos from elems where rownum = ? + 1)`,

	trim: `
	with curkey as (
		select id from rkey
		where key = ? and type = 2 and (etime is null or etime > ?)
	),
	counts as (
		select len from rkey
		where id = (select id from curkey)
	),
	bounds as (
		select
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as start,
			case when ? < 0
				then (select len from counts) + ?
				else ?
			end as stop
	),
	remain as (
		select rowid from rlist
		where kid = (select id from curkey)
		order by pos
		limit
			(select start from bounds),
			((select stop from bounds) - (select start from bounds) + 1)
	)
	delete from rlist
	where
		kid = (select id from curkey)
		and rowid not in (select rowid from remain)`,
}
