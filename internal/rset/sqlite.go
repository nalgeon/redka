package rset

var sqlite = queries{
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

	// Same as delete2.
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
	limit ?`,

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
