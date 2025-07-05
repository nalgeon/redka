package rstring

var sqlite = queries{
	get: `
	select value
	from rstring join rkey on kid = rkey.id and type = 1
	where key = ? and (etime is null or etime > ?)`,

	getMany: `
	select key, value
	from rstring
	join rkey on kid = rkey.id and type = 1
	where key in (:keys) and (etime is null or etime > ?)`,

	set1: `
	insert into rkey (key, type, version, etime, mtime)
	values (?, 1, 1, ?, ?)
	on conflict (key) do update set
		type = case when type = excluded.type then type else null end,
		version = version+1,
		etime = excluded.etime,
		mtime = excluded.mtime`,

	set2: `
	insert into rstring (kid, value)
	values ((select id from rkey where key = ?), ?)
	on conflict (kid) do update
	set value = excluded.value`,

	update1: `
	insert into rkey (key, type, version, etime, mtime)
	values (?, 1, 1, null, ?)
	on conflict (key) do update set
		type = case when type = excluded.type then type else null end,
		version = version+1,
		mtime = excluded.mtime`,

	// Same as set2.
	update2: `
	insert into rstring (kid, value)
	values ((select id from rkey where key = ?), ?)
	on conflict (kid) do update
	set value = excluded.value`,
}
