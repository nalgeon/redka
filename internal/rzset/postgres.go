package rzset

// Postgres queries for the sorted set repository.
var postgres = queries{
	scan: `
	select rzset.rowid, elem, score
	from rzset join rkey on kid = rkey.id and type = 5
	where
		key = $1 and (etime is null or etime > $2)
		and rzset.rowid > $3 and elem like $4
	limit $5`,
}

func init() {
	postgres.add1 = sqlite.add1
	postgres.add2 = sqlite.add2
	postgres.count = sqlite.count
	postgres.countScore = sqlite.countScore
	postgres.delete1 = sqlite.delete1
	postgres.delete2 = sqlite.delete2
	postgres.deleteAll1 = sqlite.deleteAll1
	postgres.deleteAll2 = sqlite.deleteAll2
	postgres.deleteRank = sqlite.deleteRank
	postgres.deleteScore = sqlite.deleteScore
	postgres.getRank = sqlite.getRank
	postgres.getScore = sqlite.getScore
	postgres.incr = sqlite.incr
	postgres.inter = sqlite.inter
	postgres.interStore = sqlite.interStore
	postgres.len = sqlite.len
	postgres.rangeRank = sqlite.rangeRank
	postgres.rangeScore = sqlite.rangeScore
	// postgres.scan = sqlite.scan
	postgres.union = sqlite.union
	postgres.unionStore = sqlite.unionStore
}
