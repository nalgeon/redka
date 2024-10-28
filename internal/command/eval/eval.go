package eval

import (
	"fmt"
	"strings"

	"github.com/nalgeon/redka/internal/command/conn"
	"github.com/nalgeon/redka/internal/command/hash"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/command/list"
	"github.com/nalgeon/redka/internal/command/server"
	"github.com/nalgeon/redka/internal/command/set"
	str "github.com/nalgeon/redka/internal/command/string"
	"github.com/nalgeon/redka/internal/command/zset"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
	lua "github.com/yuin/gopher-lua"
)

type Eval struct {
	redis.BaseCmd
	script string
	keys   []string
	args   []string
}

func ParseEval(b redis.BaseCmd) (Eval, error) {
	cmd := Eval{BaseCmd: b}
	var nKeys int
	err := parser.New(
		parser.String(&cmd.script),
		parser.Int(&nKeys),
		parser.StringsN(&cmd.keys, &nKeys),
		parser.Strings(&cmd.args),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return Eval{}, err
	}
	return cmd, nil
}

// Importing command.Parse is a circualr dependency, so we copy the function here.
// Not sure what is the best way to handle this.
func parse(args [][]byte) (redis.Cmd, error) {
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

	// eval
	case "eval":
		return ParseEval(b)

	default:
		return server.ParseUnknown(b)
	}
}

// A writer to write the results of the command back to the lua state.
type writer struct {
	L          *lua.LState
	numResults int
}

func (w *writer) WriteError(msg string) {
	w.L.Error(lua.LString(msg), 0)
	w.numResults++
}
func (w *writer) WriteString(str string) {
	w.L.Push(lua.LString(str))
	w.numResults++
}
func (w *writer) WriteBulk(bulk []byte) {
	w.L.Push(lua.LString(string(bulk)))
	w.numResults++
}
func (w *writer) WriteBulkString(bulk string) {
	w.L.Push(lua.LString(bulk))
	w.numResults++
}
func (w *writer) WriteInt(num int) {
	w.L.Push(lua.LNumber(num))
	w.numResults++
}
func (w *writer) WriteInt64(num int64) {
	w.L.Push(lua.LNumber(num))
	w.numResults++
}
func (w *writer) WriteUint64(num uint64) {
	w.L.Push(lua.LNumber(num))
	w.numResults++
}
func (w *writer) WriteArray(count int) {
	// do nothing
}
func (w *writer) WriteNull() {
	w.L.Push(lua.LNil)
	w.numResults++
}
func (w *writer) WriteRaw(data []byte) {
	w.L.Push(lua.LString(string(data)))
	w.numResults++
}
func (w *writer) WriteAny(v any) {
	w.L.Push(lua.LString(fmt.Sprintf("%v", v)))
	w.numResults++
}

func call(red redis.Redka) func(*lua.LState) int {
	return func(L *lua.LState) int {
		n := L.GetTop()
		args := make([][]byte, n)
		for i := 0; i < n; i++ {
			args[i] = []byte(L.ToString(i + 1))
		}

		cmd, err := parse(args)

		if err != nil {
			L.Error(lua.LString(err.Error()), 0)
			return 1
		}

		w := &writer{L, 0}

		_, err = cmd.Run(w, red)
		if err != nil {
			L.Error(lua.LString(err.Error()), 0)
			return 1
		}

		return w.numResults
	}
}

func (cmd Eval) Run(w redis.Writer, red redis.Redka) (any, error) {
	L := lua.NewState()
	defer L.Close()

	tb := L.NewTable()
	tb.RawSetString("call", L.NewFunction(call(red)))
	L.SetGlobal("redis", tb)

	if err := L.DoString(cmd.script); err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// This needs some work as the lua code need not return a string.
	// -1 takes the from the top of the stack.
	w.WriteString(L.ToString(-1))
	return nil, nil
}
