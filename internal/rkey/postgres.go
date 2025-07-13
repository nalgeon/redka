package rkey

// Postgres queries for the key repository.
var postgres = queries{
	deleteAll: `
	truncate table rkey cascade`,

	deleteNExpired: `
	delete from rkey
	where ctid in (
		select ctid from rkey
		where etime <= $1
		limit $2
	)`,

	keys: `
	select id, key, type, version, etime, mtime from rkey
	where key like $1 and (etime is null or etime > $2)`,

	scan: `
	select id, key, type, version, etime, mtime from rkey
	where
		id > $1 and key like $2 and (type = $3 or $4)
		and (etime is null or etime > $5)
	order by id asc
	limit $6`,
}

func init() {
	postgres.count = sqlite.count
	postgres.delete = sqlite.delete
	// postgres.deleteAll = sqlite.deleteAll
	postgres.deleteAllExpired = sqlite.deleteAllExpired
	// postgres.deleteNExpired = sqlite.deleteNExpired
	postgres.expire = sqlite.expire
	postgres.get = sqlite.get
	// postgres.keys = sqlite.keys
	postgres.len = sqlite.len
	postgres.persist = sqlite.persist
	postgres.random = sqlite.random
	postgres.rename1 = sqlite.rename1
	postgres.rename2 = sqlite.rename2
	// postgres.scan = sqlite.scan
}
