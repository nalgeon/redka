package rhash

// Postgres queries for the hash repository.
var postgres = queries{
	scan: `
	select rhash.rowid, field, value
	from rhash join rkey on kid = rkey.id and type = 4
	where
		key = $1 and (etime is null or etime > $2)
		and rhash.rowid > $3 and field like $4
	order by rhash.rowid asc
	limit $5`,
}

func init() {
	postgres.count = sqlite.count
	postgres.delete1 = sqlite.delete1
	postgres.delete2 = sqlite.delete2
	postgres.fields = sqlite.fields
	postgres.get = sqlite.get
	postgres.getMany = sqlite.getMany
	postgres.items = sqlite.items
	postgres.len = sqlite.len
	// postgres.scan = sqlite.scan
	postgres.set1 = sqlite.set1
	postgres.set2 = sqlite.set2
	postgres.values = sqlite.values
}
