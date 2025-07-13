package rstring

// Postgres queries for the string repository.
var postgres queries

func init() {
	postgres.get = sqlite.get
	postgres.getMany = sqlite.getMany
	postgres.set1 = sqlite.set1
	postgres.set2 = sqlite.set2
	postgres.update1 = sqlite.update1
	postgres.update2 = sqlite.update2
}
