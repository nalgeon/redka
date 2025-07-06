package rhash

// SQLite queries for the hash repository.
var sqlite = queries{
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
		mtime = ?,
		len = len - ?
	where key = ? and type = 4 and (etime is null or etime > ?)`,

	fields: `
	select field
	from rhash join rkey on kid = rkey.id and type = 4
	where key = ? and (etime is null or etime > ?)`,

	get: `
	select value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = ? and (etime is null or etime > ?) and field = ?`,

	getMany: `
	select field, value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = ? and (etime is null or etime > ?) and field in (:fields)`,

	items: `
	select field, value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = ? and (etime is null or etime > ?)`,

	len: `
	select len from rkey
	where key = ? and type = 4 and (etime is null or etime > ?)`,

	scan: `
	select rhash.rowid, field, value
	from rhash join rkey on kid = rkey.id and type = 4
	where
		key = ? and (etime is null or etime > ?)
		and rhash.rowid > ? and field glob ?
	limit ?`,

	set1: `
	insert into
	rkey   (key, type, version, mtime, len)
	values (  ?,    4,       1,     ?,   0)
	on conflict (key) do update set
		type = case when type = excluded.type then type else null end,
		version = version+1,
		mtime = excluded.mtime
	returning id`,

	set2: `
	insert into rhash (kid, field, value)
	values (?, ?, ?)
	on conflict (kid, field) do update
	set value = excluded.value`,

	values: `
	select value
	from rhash join rkey on kid = rkey.id and type = 4
	where key = ? and (etime is null or etime > ?)`,
}
