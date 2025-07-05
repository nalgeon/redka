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
	where etime <= ?`,

	deleteNExpired: `
	delete from rkey
	where rowid in (
		select rowid from rkey
		where etime <= ?
		limit ?
	)`,

	expire: `
	update rkey set
		version = version + 1,
		etime = ?
	where key = ? and (etime is null or etime > ?)`,

	get: `
	select id, key, type, version, etime, mtime
	from rkey
	where key = ? and (etime is null or etime > ?)`,

	keys: `
	select id, key, type, version, etime, mtime from rkey
	where key glob ? and (etime is null or etime > ?)`,

	len: `
	select count(*) from rkey`,

	persist: `
	update rkey set
		version = version + 1,
		etime = null
	where key = ? and (etime is null or etime > ?)`,

	random: `
	select id, key, type, version, etime, mtime from rkey
	where etime is null or etime > ?
	order by random() limit 1`,

	rename: `
	update or replace rkey set
		id = old.id,
		key = ?,
		type = old.type,
		version = old.version+1,
		etime = old.etime,
		mtime = ?
	from (
		select id, key, type, version, etime, mtime
		from rkey
		where key = ? and (etime is null or etime > ?)
	) as old
	where rkey.key = ? and (rkey.etime is null or rkey.etime > ?)`,

	scan: `
	select id, key, type, version, etime, mtime from rkey
	where
		id > ? and key glob ? and (type = ? or ?)
		and (etime is null or etime > ?)
	order by id asc
	limit ?`,
}
