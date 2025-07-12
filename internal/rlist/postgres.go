package rlist

// Postgres queries for the list repository.
var postgres queries

func init() {
	postgres.delete = sqlite.delete
	postgres.deleteBack = sqlite.deleteBack
	postgres.deleteFront = sqlite.deleteFront
	postgres.get = sqlite.get
	postgres.insert = sqlite.insert
	postgres.insertAfter = sqlite.insertAfter
	postgres.insertBefore = sqlite.insertBefore
	postgres.len = sqlite.len
	postgres.popBack = sqlite.popBack
	postgres.popFront = sqlite.popFront
	postgres.push = sqlite.push
	postgres.pushBack = sqlite.pushBack
	postgres.pushFront = sqlite.pushFront
	postgres.lrange = sqlite.lrange
	postgres.set = sqlite.set
	postgres.trim = sqlite.trim
}
