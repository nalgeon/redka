package command

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/core"
)

var ErrInvalidCursor = errors.New("ERR invalid cursor")
var ErrInvalidInt = errors.New("ERR value is not an integer or out of range")
var ErrNotFound = errors.New("ERR no such key")
var ErrKeyType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
var ErrNestedMulti = errors.New("ERR MULTI calls can not be nested")
var ErrNotInMulti = errors.New("ERR EXEC without MULTI")
var ErrSyntaxError = errors.New("ERR syntax error")

func ErrInvalidArgNum(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
func ErrInvalidExpireTime(cmd string) error {
	return fmt.Errorf("ERR invalid expire time in '%s' command", cmd)
}
func ErrUnknownCmd(cmd string) error {
	return fmt.Errorf("ERR unknown command '%s'", cmd)
}
func ErrUnknownSubcmd(cmd, subcmd string) error {
	return fmt.Errorf("ERR unknown subcommand '%s %s'", cmd, subcmd)
}

// translateError translates a domain error to a command error
// and returns its string representation.
func translateError(err error) string {
	switch err {
	case core.ErrNotFound:
		return ErrNotFound.Error()
	case core.ErrKeyType:
		return ErrKeyType.Error()
	default:
		return err.Error()
	}
}

// Redka is a Redis-like repository.
type Redka interface {
	Key() redka.Keys
	Str() redka.Strings
}

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
	Name() string
	Err() error
	String() string

	Run(w Writer, red Redka) (any, error)
}

type baseCmd struct {
	name string
	args [][]byte
	err  error
}

func newBaseCmd(args [][]byte) baseCmd {
	return baseCmd{
		name: strings.ToLower(string(args[0])),
		args: args[1:],
	}
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
func (cmd baseCmd) Err() error {
	return cmd.err
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

	default:
		return parseUnknown(b)
	}
}
