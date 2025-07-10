package rset

// Postgres queries for the set repository.
var postgres = queries{
	scan: `
	select rset.rowid, elem
	from rset join rkey on kid = rkey.id and type = 3
	where
		key = $1 and (etime is null or etime > $2)
		and rset.rowid > $3 and elem like $4
	limit $5`,
}

func init() {
	postgres.add1 = sqlite.add1
	postgres.add2 = sqlite.add2
	postgres.clone = sqlite.clone
	postgres.delete1 = sqlite.delete1
	postgres.delete2 = sqlite.delete2
	postgres.deleteKey1 = sqlite.deleteKey1
	postgres.deleteKey2 = sqlite.deleteKey2
	postgres.diff = sqlite.diff
	postgres.diffStore = sqlite.diffStore
	postgres.exists = sqlite.exists
	postgres.inter = sqlite.inter
	postgres.interStore = sqlite.interStore
	postgres.items = sqlite.items
	postgres.len = sqlite.len
	postgres.pop1 = sqlite.pop1
	postgres.pop2 = sqlite.pop2
	postgres.random = sqlite.random
	// postgres.scan = sqlite.scan
	postgres.union = sqlite.union
	postgres.unionStore = sqlite.unionStore
}
