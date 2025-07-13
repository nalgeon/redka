// Package redis implements basis
// for Redis-compatible commands in Redka.
package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nalgeon/redka/internal/core"
)

// Redis-like errors.
var (
	ErrInvalidArgNum     = errors.New("ERR wrong number of arguments")
	ErrInvalidCursor     = errors.New("ERR invalid cursor")
	ErrInvalidExpireTime = errors.New("ERR invalid expire time")
	ErrInvalidFloat      = errors.New("ERR value is not a float")
	ErrInvalidInt        = errors.New("ERR value is not an integer")
	ErrNestedMulti       = errors.New("ERR MULTI calls can not be nested")
	ErrNotFound          = errors.New("ERR no such key")
	ErrNotInMulti        = errors.New("ERR EXEC without MULTI")
	ErrOutOfRange        = errors.New("ERR index out of range")
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

// BaseCmd is a base for Redis-compatible commands.
type BaseCmd struct {
	name string
	args [][]byte
}

// NewBaseCmd creates a new BaseCmd.
func NewBaseCmd(args [][]byte) BaseCmd {
	return BaseCmd{
		name: strings.ToLower(string(args[0])),
		args: args[1:],
	}
}

// Error translates a domain error to a command error.
func (cmd BaseCmd) Error(err error) string {
	switch err {
	case core.ErrNotFound:
		err = ErrNotFound
	}
	return fmt.Sprintf("%s (%s)", err, cmd.Name())
}

// Name returns the command name.
func (cmd BaseCmd) Name() string {
	return cmd.name
}

// Args returns the command arguments.
func (cmd BaseCmd) Args() [][]byte {
	return cmd.args
}

// String returns the command string representation (name and arguments).
func (cmd BaseCmd) String() string {
	var b strings.Builder
	for i, arg := range cmd.args {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.Write(arg)
	}
	return b.String()
}

// MustParse parses a text representation of a command
// into a command and panics if an error occurs.
func MustParse[T Cmd](parse func(BaseCmd) (T, error), s string) T {
	cmd, err := Parse(parse, s)
	if err != nil {
		panic(err)
	}
	return cmd
}

// Parse parses a text representation of a command into a command.
func Parse[T Cmd](parse func(BaseCmd) (T, error), s string) (T, error) {
	parts := strings.Split(s, " ")
	args := buildArgs(parts[0], parts[1:]...)
	b := NewBaseCmd(args)
	return parse(b)
}

// WriteFloat writes a float64 value to the writer.
func WriteFloat(w Writer, f float64) {
	w.WriteBulkString(strconv.FormatFloat(f, 'f', -1, 64))
}

// buildArgs builds a list of arguments for a command.
func buildArgs(name string, args ...string) [][]byte {
	rargs := make([][]byte, len(args)+1)
	rargs[0] = []byte(name)
	for i, arg := range args {
		rargs[i+1] = []byte(arg)
	}
	return rargs
}
