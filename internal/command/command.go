// Package command implements Redis-compatible commands
// for operations on data structures.
package command

import (
	"strings"

	"github.com/nalgeon/redka/internal/command/conn"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/command/list"
	"github.com/nalgeon/redka/internal/command/server"
	"github.com/nalgeon/redka/internal/command/set"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/redis"
)

// Parse parses a text representation of a command into a Cmd.
func Parse(args [][]byte) (redis.Cmd, error) {
	name := strings.ToLower(string(args[0]))
	b := redis.NewBaseCmd(args)
	switch name {
	// server
	case "command":
		return server.ParseOK(b)
	case "dbsize":
		return server.ParseDBSize(b)
	case "flushdb":
		return key.ParseFlushDB(b)
	case "flushall":
		return key.ParseFlushDB(b)
	case "info":
		return server.ParseOK(b)
	case "lolwut":
		return server.ParseLolwut(b)

	// connection
	case "echo":
		return conn.ParseEcho(b)
	case "ping":
		return conn.ParsePing(b)
	case "select":
		return conn.ParseSelect(b)

	// key
	case "del":
		return key.ParseDel(b)
	case "exists":
		return key.ParseExists(b)
	case "expire":
		return key.ParseExpire(b, 1000)
	case "expireat":
		return key.ParseExpireAt(b, 1000)
	case "keys":
		return key.ParseKeys(b)
	case "persist":
		return key.ParsePersist(b)
	case "pexpire":
		return key.ParseExpire(b, 1)
	case "pexpireat":
		return key.ParseExpireAt(b, 1)
	case "randomkey":
		return key.ParseRandomKey(b)
	case "rename":
		return key.ParseRename(b)
	case "renamenx":
		return key.ParseRenameNX(b)
	case "scan":
		return key.ParseScan(b)
	case "ttl":
		return key.ParseTTL(b)
	case "type":
		return key.ParseType(b)

	// list
	case "lindex":
		return list.ParseLIndex(b)
	case "linsert":
		return list.ParseLInsert(b)
	case "llen":
		return list.ParseLLen(b)
	case "lpop":
		return list.ParseLPop(b)
	case "lpush":
		return list.ParseLPush(b)
	case "lrange":
		return list.ParseLRange(b)
	case "lrem":
		return list.ParseLRem(b)
	case "lset":
		return list.ParseLSet(b)
	case "ltrim":
		return list.ParseLTrim(b)
	case "rpop":
		return list.ParseRPop(b)
	case "rpoplpush":
		return list.ParseRPopLPush(b)
	case "rpush":
		return list.ParseRPush(b)

	// string
	case "decr":
		return str.ParseIncr(b, -1)
	case "decrby":
		return str.ParseIncrBy(b, -1)
	case "get":
		return str.ParseGet(b)
	case "getset":
		return str.ParseGetSet(b)
	case "incr":
		return str.ParseIncr(b, 1)
	case "incrby":
		return str.ParseIncrBy(b, 1)
	case "incrbyfloat":
		return str.ParseIncrByFloat(b)
	case "mget":
		return str.ParseMGet(b)
	case "mset":
		return str.ParseMSet(b)
	case "psetex":
		return str.ParseSetEX(b, 1)
	case "set":
		return str.ParseSet(b)
	case "setex":
		return str.ParseSetEX(b, 1000)
	case "setnx":
		return str.ParseSetNX(b)

	// hash
	case "hdel":
		return hash.ParseHDel(b)
	case "hexists":
		return hash.ParseHExists(b)
	case "hget":
		return hash.ParseHGet(b)
	case "hgetall":
		return hash.ParseHGetAll(b)
	case "hincrby":
		return hash.ParseHIncrBy(b)
	case "hincrbyfloat":
		return hash.ParseHIncrByFloat(b)
	case "hkeys":
		return hash.ParseHKeys(b)
	case "hlen":
		return hash.ParseHLen(b)
	case "hmget":
		return hash.ParseHMGet(b)
	case "hmset":
		return hash.ParseHMSet(b)
	case "hscan":
		return hash.ParseHScan(b)
	case "hset":
		return hash.ParseHSet(b)
	case "hsetnx":
		return hash.ParseHSetNX(b)
	case "hvals":
		return hash.ParseHVals(b)

	// set
	case "sadd":
		return set.ParseSAdd(b)
	case "scard":
		return set.ParseSCard(b)
	case "sdiff":
		return set.ParseSDiff(b)
	case "sdiffstore":
		return set.ParseSDiffStore(b)
	case "sinter":
		return set.ParseSInter(b)
	case "sinterstore":
		return set.ParseSInterStore(b)
	case "sismember":
		return set.ParseSIsMember(b)
	case "smembers":
		return set.ParseSMembers(b)
	case "smove":
		return set.ParseSMove(b)
	case "spop":
		return set.ParseSPop(b)
	case "srandmember":
		return set.ParseSRandMember(b)
	case "srem":
		return set.ParseSRem(b)
	case "sscan":
		return set.ParseSScan(b)
	case "sunion":
		return set.ParseSUnion(b)
	case "sunionstore":
		return set.ParseSUnionStore(b)

	// sorted set
	case "zadd":
		return zset.ParseZAdd(b)
	case "zcard":
		return zset.ParseZCard(b)
	case "zcount":
		return zset.ParseZCount(b)
	case "zincrby":
		return zset.ParseZIncrBy(b)
	case "zinter":
		return zset.ParseZInter(b)
	case "zinterstore":
		return zset.ParseZInterStore(b)
	case "zrange":
		return zset.ParseZRange(b)
	case "zrangebyscore":
		return zset.ParseZRangeByScore(b)
	case "zrank":
		return zset.ParseZRank(b)
	case "zrem":
		return zset.ParseZRem(b)
	case "zremrangebyrank":
		return zset.ParseZRemRangeByRank(b)
	case "zremrangebyscore":
		return zset.ParseZRemRangeByScore(b)
	case "zrevrange":
		return zset.ParseZRevRange(b)
	case "zrevrangebyscore":
		return zset.ParseZRevRangeByScore(b)
	case "zrevrank":
		return zset.ParseZRevRank(b)
	case "zscan":
		return zset.ParseZScan(b)
	case "zscore":
		return zset.ParseZScore(b)
	case "zunion":
		return zset.ParseZUnion(b)
	case "zunionstore":
		return zset.ParseZUnionStore(b)

	default:
		return server.ParseUnknown(b)
	}
}
