package rkey

// SQLite queries for the key repository.
var sqlite = queries{
	count: `
	select count(id) from rkey
	where key in (:keys) and (etime is null or etime > ?)`,

	delete: `
	delete from rkey
	where key in (:keys) and (etime is null or etime > ?)`,

	deleteAll: `
	delete from rkey;
	vacuum;
	pragma integrity_check;`,

	deleteAllExpired: `
	delete from rkey
	where etime <= $1`,

	deleteNExpired: `
	delete from rkey
	where rowid in (
		select rowid from rkey
		where etime <= $1
		limit $2
	)`,

	expire: `
	update rkey set
		version = version + 1,
		etime = $1
	where key = $2 and (etime is null or etime > $3)`,

	get: `
	select id, key, type, version, etime, mtime
	from rkey
	where key = $1 and (etime is null or etime > $2)`,

	keys: `
	select id, key, type, version, etime, mtime from rkey
	where key glob $1 and (etime is null or etime > $2)`,

	len: `
	select count(*) from rkey`,

	persist: `
	update rkey set
		version = version + 1,
		etime = null
	where key = $1 and (etime is null or etime > $2)`,

	random: `
	select id, key, type, version, etime, mtime from rkey
	where etime is null or etime > $1
	order by random() limit 1`,

	rename1: `
	delete from rkey where id = $1`,

	rename2: `
	update rkey
	set
		key = $1,
		version = version + 1,
		mtime = $2
	where key = $3 and (etime is null or etime > $4)`,

	scan: `
	select id, key, type, version, etime, mtime from rkey
	where
		id > $1 and key glob $2 and (type = $3 or $4)
		and (etime is null or etime > $5)
	order by id asc
	limit $6`,
}
