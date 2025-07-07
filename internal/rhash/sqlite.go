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
		and rhash.rowid > $3 and field glob $4
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
