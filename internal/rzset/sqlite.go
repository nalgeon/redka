package rzset

// SQLite queries for the sorted set repository.
var sqlite = queries{
	add1: `
	insert into
	rkey   (key, type, version, mtime, len)
	values ( $1,    5,       1,    $2,   0)
	on conflict (key) do update set
		type = case when rkey.type = excluded.type then rkey.type else null end,
		version = rkey.version + 1,
		mtime = excluded.mtime
	returning id`,

	add2: `
	insert into rzset (kid, elem, score)
	values ($1, $2, $3)
	on conflict (kid, elem) do update
	set score = excluded.score`,

	count: `
	select count(elem)
	from rzset join rkey on kid = rkey.id and type = 5
	where key = ? and (etime is null or etime > ?) and elem in (:elems)`,

	countScore: `
	select count(elem)
	from rzset join rkey on kid = rkey.id and type = 5
	where key = $1 and (etime is null or etime > $2) and score between $3 and $4`,

	delete1: `
	delete from rzset
	where kid = (
			select id from rkey
			where key = ? and type = 5 and (etime is null or etime > ?)
		) and elem in (:elems)`,

	delete2: `
	update rkey set
		version = version + 1,
		mtime = $1,
		len = len - $2
	where key = $3 and type = 5 and (etime is null or etime > $4)`,

	deleteAll1: `
	delete from rzset
	where kid = (
		select id from rkey
		where key = $1 and type = 5 and (etime is null or etime > $2)
	)`,

	deleteAll2: `
	update rkey set
		version = 0,
		mtime = 0,
		len = 0
	where key = $1 and type = 5 and (etime is null or etime > $2)`,

	deleteRank: `
	with ranked as (
		select rowid, elem, score
		from rzset
		where kid = (
			select id from rkey
			where key = $1 and type = 5 and (etime is null or etime > $2)
		)
		order by score, elem
		limit $3
		offset $4
	)
	delete from rzset
	where rowid in (select rowid from ranked)`,

	deleteScore: `
	delete from rzset
	where kid = (
			select id from rkey
			where key = $1 and type = 5 and (etime is null or etime > $2)
		) and score between $3 and $4`,

	getRank: `
	with ranked as (
		select elem, score, (row_number() over w - 1) as rank
		from rzset join rkey on kid = rkey.id and type = 5
		where key = $1 and (etime is null or etime > $2)
		window w as (partition by kid order by score asc, elem asc)
	)
	select rank, score
	from ranked
	where elem = $3`,

	getScore: `
	select score
	from rzset join rkey on kid = rkey.id and type = 5
	where key = $1 and (etime is null or etime > $2) and elem = $3`,

	incr: `
	insert into rzset (kid, elem, score)
	values ($1, $2, $3)
	on conflict (kid, elem) do update
	set score = rzset.score + excluded.score
	returning score`,

	inter: `
	select elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	having count(distinct kid) = ?
	order by sum(score), elem`,

	interStore: `
	insert into rzset (kid, elem, score)
	select ?, elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	having count(distinct kid) = ?
	order by sum(score), elem`,

	len: `
	select len from rkey
	where key = $1 and type = 5 and (etime is null or etime > $2)`,

	rangeRank: `
	with ranked as (
		select elem, score, (row_number() over w - 1) as rank
		from rzset join rkey on kid = rkey.id and type = 5
		where key = $1 and (etime is null or etime > $2)
		window w as (partition by kid order by score asc, elem asc)
	)
	select elem, score
	from ranked
	where rank between $3 and $4
	order by rank, elem asc`,

	rangeScore: `
	select elem, score
	from rzset join rkey on kid = rkey.id and type = 5
	where key = $1 and (etime is null or etime > $2)
	and score between $3 and $4
	order by score asc, elem asc`,

	scan: `
	select rzset.rowid, elem, score
	from rzset join rkey on kid = rkey.id and type = 5
	where
		key = $1 and (etime is null or etime > $2)
		and rzset.rowid > $3 and elem glob $4
	limit $5`,

	union: `
	select elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	order by sum(score), elem`,

	unionStore: `
	insert into rzset (kid, elem, score)
	select ?, elem, sum(score) as score
	from rzset join rkey on kid = rkey.id and type = 5
	where key in (:keys) and (etime is null or etime > ?)
	group by elem
	order by sum(score), elem`,
}
