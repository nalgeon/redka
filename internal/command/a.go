// Package command implements Redis-compatible commands
// for operations on data structures.
package command

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/rhash"
	"github.com/nalgeon/redka/internal/rkey"
)

// Redis-like errors.
var (
	ErrInvalidArgNum     = errors.New("ERR wrong number of arguments")
	ErrInvalidCursor     = errors.New("ERR invalid cursor")
	ErrInvalidExpireTime = errors.New("ERR invalid expire time")
	ErrInvalidFloat      = errors.New("ERR value is not a float")
	ErrInvalidInt        = errors.New("ERR value is not an integer or out of range")
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
	WriteError(msg string)
	WriteString(str string)
	WriteBulk(bulk []byte)
	WriteBulkString(bulk string)
	WriteInt(num int)
	WriteInt64(num int64)
	WriteUint64(num uint64)
	WriteArray(count int)
	WriteNull()
	WriteRaw(data []byte)
	WriteAny(v any)
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
	GetSet(key string, value any, ttl time.Duration) (core.Value, error)
	Incr(key string, delta int) (int, error)
	IncrFloat(key string, delta float64) (float64, error)
	Set(key string, value any) error
	SetExists(key string, value any, ttl time.Duration) (bool, error)
	SetExpires(key string, value any, ttl time.Duration) error
	SetMany(items map[string]any) error
	SetManyNX(items map[string]any) (bool, error)
	SetNotExists(key string, value any, ttl time.Duration) (bool, error)
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

// Redka is an abstraction for *redka.DB and *redka.Tx.
// Used to execute commands in a unified way.
type Redka struct {
	key  RKey
	str  RStr
	hash RHash
}

// RedkaDB creates a new Redka instance for a database.
func RedkaDB(db *redka.DB) Redka {
	return Redka{
		key:  db.Key(),
		str:  db.Str(),
		hash: db.Hash(),
	}
}

// RedkaTx creates a new Redka instance for a transaction.
func RedkaTx(tx *redka.Tx) Redka {
	return Redka{
		key:  tx.Key(),
		str:  tx.Str(),
		hash: tx.Hash(),
	}
}

// Key returns the key repository.
func (r Redka) Key() RKey {
	return r.key
}

// Str returns the string repository.
func (r Redka) Str() RStr {
	return r.str
}

// Hash returns the hash repository.
func (r Redka) Hash() RHash {
	return r.hash
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
	case "msetnx":
		return parseMSetNX(b)
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

	default:
		return parseUnknown(b)
	}
}
