// Package command implements Redis-compatible commands
// for operations on data structures.
package command

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/internal/rkey"
	"github.com/nalgeon/redka/internal/rstring"
	"github.com/nalgeon/redka/internal/rzset"
)

// Redis-like errors.
var (
	ErrInvalidArgNum     = errors.New("ERR wrong number of arguments")
	ErrInvalidCursor     = errors.New("ERR invalid cursor")
	ErrInvalidExpireTime = errors.New("ERR invalid expire time")
	ErrInvalidFloat      = errors.New("ERR value is not a float")
	ErrInvalidInt        = errors.New("ERR value is not an integer")
	ErrKeyType           = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNestedMulti       = errors.New("ERR MULTI calls can not be nested")
	ErrNotFound          = errors.New("ERR no such key")
	ErrNotInMulti        = errors.New("ERR EXEC without MULTI")
	ErrSyntaxError       = errors.New("ERR syntax error")
	ErrUnknownCmd        = errors.New("ERR unknown command")
	ErrUnknownSubcmd     = errors.New("ERR unknown subcommand")
)

// Writer is an interface to write responses to the client.
type Writer interface {
	WriteAny(v any)
	WriteArray(count int)
	WriteBulk(bulk []byte)
	WriteBulkString(bulk string)
	WriteError(msg string)
	WriteInt(num int)
	WriteInt64(num int64)
	WriteNull()
	WriteRaw(data []byte)
	WriteString(str string)
	WriteUint64(num uint64)
}

// Cmd is a Redis-compatible command.
type Cmd interface {
	// Name returns the command name.
	Name() string

	// String returns the command string representation (name and arguments).
	String() string

	// Error translates a domain error to a command error
	// and returns its string representation.
	Error(err error) string

	// Run executes the command and writes the result to the writer.
	Run(w Writer, red Redka) (any, error)
}

// RHash is a hash repository.
type RHash interface {
	Delete(key string, fields ...string) (int, error)
	Exists(key, field string) (bool, error)
	Fields(key string) ([]string, error)
	Get(key, field string) (core.Value, error)
	GetMany(key string, fields ...string) (map[string]core.Value, error)
	Incr(key, field string, delta int) (int, error)
	IncrFloat(key, field string, delta float64) (float64, error)
	Items(key string) (map[string]core.Value, error)
	Len(key string) (int, error)
	Scan(key string, cursor int, pattern string, pageSize int) (rhash.ScanResult, error)
	Scanner(key, pattern string, pageSize int) *rhash.Scanner
	Set(key, field string, value any) (bool, error)
	SetMany(key string, items map[string]any) (int, error)
	SetNotExists(key, field string, value any) (bool, error)
	Values(key string) ([]core.Value, error)
}

// RKey is a key repository.
type RKey interface {
	Count(keys ...string) (int, error)
	Delete(keys ...string) (int, error)
	DeleteAll() error
	Exists(key string) (bool, error)
	Expire(key string, ttl time.Duration) error
	ExpireAt(key string, at time.Time) error
	Get(key string) (core.Key, error)
	Keys(pattern string) ([]core.Key, error)
	Persist(key string) error
	Random() (core.Key, error)
	Rename(key, newKey string) error
	RenameNotExists(key, newKey string) (bool, error)
	Scan(cursor int, pattern string, pageSize int) (rkey.ScanResult, error)
	Scanner(pattern string, pageSize int) *rkey.Scanner
}

// RStr is a string repository.
type RStr interface {
	Get(key string) (core.Value, error)
	GetMany(keys ...string) (map[string]core.Value, error)
	Incr(key string, delta int) (int, error)
	IncrFloat(key string, delta float64) (float64, error)
	Set(key string, value any) error
	SetExpires(key string, value any, ttl time.Duration) error
	SetMany(items map[string]any) error
	SetWith(key string, value any) rstring.SetCmd
}

// RZSet is a sorted set repository.
type RZSet interface {
	Add(key string, elem any, score float64) (bool, error)
	AddMany(key string, items map[any]float64) (int, error)
	Count(key string, min, max float64) (int, error)
	Delete(key string, elems ...any) (int, error)
	DeleteWith(key string) rzset.DeleteCmd
	GetRank(key string, elem any) (rank int, score float64, err error)
	GetRankRev(key string, elem any) (rank int, score float64, err error)
	GetScore(key string, elem any) (float64, error)
	Incr(key string, elem any, delta float64) (float64, error)
	Inter(keys ...string) ([]rzset.SetItem, error)
	InterWith(keys ...string) rzset.InterCmd
	Len(key string) (int, error)
	Range(key string, start, stop int) ([]rzset.SetItem, error)
	RangeWith(key string) rzset.RangeCmd
	Scan(key string, cursor int, pattern string, count int) (rzset.ScanResult, error)
	Scanner(key, pattern string, pageSize int) *rzset.Scanner
	Union(keys ...string) ([]rzset.SetItem, error)
	UnionWith(keys ...string) rzset.UnionCmd
}

// Redka is an abstraction for *redka.DB and *redka.Tx.
// Used to execute commands in a unified way.
type Redka struct {
	hash RHash
	key  RKey
	str  RStr
	zset RZSet
}

// RedkaDB creates a new Redka instance for a database.
func RedkaDB(db *redka.DB) Redka {
	return Redka{
		hash: db.Hash(),
		key:  db.Key(),
		str:  db.Str(),
		zset: db.ZSet(),
	}
}

// RedkaTx creates a new Redka instance for a transaction.
func RedkaTx(tx *redka.Tx) Redka {
	return Redka{
		hash: tx.Hash(),
		key:  tx.Key(),
		str:  tx.Str(),
		zset: tx.ZSet(),
	}
}

// Hash returns the hash repository.
func (r Redka) Hash() RHash {
	return r.hash
}

// Key returns the key repository.
func (r Redka) Key() RKey {
	return r.key
}

// Str returns the string repository.
func (r Redka) Str() RStr {
	return r.str
}

// ZSet returns the sorted set repository.
func (r Redka) ZSet() RZSet {
	return r.zset
}

type baseCmd struct {
	name string
	args [][]byte
}

func newBaseCmd(args [][]byte) baseCmd {
	return baseCmd{
		name: strings.ToLower(string(args[0])),
		args: args[1:],
	}
}

func (cmd baseCmd) Error(err error) string {
	switch err {
	case core.ErrNotFound:
		err = ErrNotFound
	case core.ErrKeyType:
		err = ErrKeyType
	}
	return fmt.Sprintf("%s (%s)", err, cmd.Name())
}

func (cmd baseCmd) Name() string {
	return cmd.name
}
func (cmd baseCmd) String() string {
	var b strings.Builder
	for i, arg := range cmd.args {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.Write(arg)
	}
	return b.String()
}

func writeFloat(w Writer, f float64) {
	w.WriteBulkString(strconv.FormatFloat(f, 'f', -1, 64))
}

// Parse parses a text representation of a command into a Cmd.
func Parse(args [][]byte) (Cmd, error) {
	name := strings.ToLower(string(args[0]))
	b := newBaseCmd(args)
	switch name {
	// server
	case "command":
		return parseOK(b)
	case "flushdb":
		return parseFlushDB(b)
	case "info":
		return parseOK(b)

	// connection
	case "echo":
		return parseEcho(b)

	// key
	case "del":
		return parseDel(b)
	case "exists":
		return parseExists(b)
	case "expire":
		return parseExpire(b, 1000)
	case "expireat":
		return parseExpireAt(b, 1000)
	case "keys":
		return parseKeys(b)
	case "persist":
		return parsePersist(b)
	case "pexpire":
		return parseExpire(b, 1)
	case "pexpireat":
		return parseExpireAt(b, 1)
	case "randomkey":
		return parseRandomKey(b)
	case "rename":
		return parseRename(b)
	case "renamenx":
		return parseRenameNX(b)
	case "scan":
		return parseScan(b)

	// string
	case "decr":
		return parseIncr(b, -1)
	case "decrby":
		return parseIncrBy(b, -1)
	case "get":
		return parseGet(b)
	case "getset":
		return parseGetSet(b)
	case "incr":
		return parseIncr(b, 1)
	case "incrby":
		return parseIncrBy(b, 1)
	case "incrbyfloat":
		return parseIncrByFloat(b)
	case "mget":
		return parseMGet(b)
	case "mset":
		return parseMSet(b)
	case "psetex":
		return parseSetEX(b, 1)
	case "set":
		return parseSet(b)
	case "setex":
		return parseSetEX(b, 1000)
	case "setnx":
		return parseSetNX(b)

	// hash
	case "hdel":
		return parseHDel(b)
	case "hexists":
		return parseHExists(b)
	case "hget":
		return parseHGet(b)
	case "hgetall":
		return parseHGetAll(b)
	case "hincrby":
		return parseHIncrBy(b)
	case "hincrbyfloat":
		return parseHIncrByFloat(b)
	case "hkeys":
		return parseHKeys(b)
	case "hlen":
		return parseHLen(b)
	case "hmget":
		return parseHMGet(b)
	case "hmset":
		return parseHMSet(b)
	case "hscan":
		return parseHScan(b)
	case "hset":
		return parseHSet(b)
	case "hsetnx":
		return parseHSetNX(b)
	case "hvals":
		return parseHVals(b)

	// sorted set
	case "zadd":
		return parseZAdd(b)
	case "zcard":
		return parseZCard(b)
	case "zcount":
		return parseZCount(b)
	case "zincrby":
		return parseZIncrBy(b)
	case "zinter":
		return parseZInter(b)
	case "zinterstore":
		return parseZInterStore(b)
	case "zrange":
		return parseZRange(b)
	case "zrangebyscore":
		return parseZRangeByScore(b)
	case "zrank":
		return parseZRank(b)
	case "zrem":
		return parseZRem(b)
	case "zremrangebyrank":
		return parseZRemRangeByRank(b)
	case "zremrangebyscore":
		return parseZRemRangeByScore(b)
	case "zrevrange":
		return parseZRevRange(b)
	case "zrevrangebyscore":
		return parseZRevRangeByScore(b)
	case "zrevrank":
		return parseZRevRank(b)
	case "zscan":
		return parseZScan(b)
	case "zscore":
		return parseZScore(b)
	case "zunion":
		return parseZUnion(b)
	case "zunionstore":
		return parseZUnionStore(b)

	default:
		return parseUnknown(b)
	}
}
