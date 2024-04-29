package string

import (
	"time"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/parser"
	"github.com/nalgeon/redka/internal/redis"
)

// Set sets the string value of a key.
// The key is created if it doesn't exist.
// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
// https://redis.io/commands/set
type Set struct {
	redis.BaseCmd
	Key     string
	Value   []byte
	IfNX    bool
	IfXX    bool
	Get     bool
	TTL     time.Duration
	At      time.Time
	KeepTTL bool
}

func ParseSet(b redis.BaseCmd) (*Set, error) {
	cmd := &Set{BaseCmd: b}

	// Parse the command arguments.
	var ttlSec, ttlMs, atSec, atMs int
	err := parser.New(
		parser.String(&cmd.Key),
		parser.Bytes(&cmd.Value),
		parser.OneOf(
			parser.Flag("nx", &cmd.IfNX),
			parser.Flag("xx", &cmd.IfXX),
		),
		parser.Flag("get", &cmd.Get),
		parser.OneOf(
			parser.Named("ex", parser.Int(&ttlSec)),
			parser.Named("px", parser.Int(&ttlMs)),
			parser.Named("exat", parser.Int(&atSec)),
			parser.Named("pxat", parser.Int(&atMs)),
			parser.Flag("keepttl", &cmd.KeepTTL),
		),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return cmd, err
	}

	// Set the expiration time.
	if ttlSec > 0 {
		cmd.TTL = time.Duration(ttlSec) * time.Second
	} else if ttlMs > 0 {
		cmd.TTL = time.Duration(ttlMs) * time.Millisecond
	} else if atSec > 0 {
		cmd.At = time.Unix(int64(atSec), 0)
	} else if atMs > 0 {
		cmd.At = time.Unix(0, int64(atMs)*int64(time.Millisecond))
	}
	if cmd.TTL < 0 {
		return cmd, redis.ErrInvalidExpireTime
	}

	return cmd, nil
}

func (cmd *Set) Run(w redis.Writer, red redis.Redka) (any, error) {
	if !cmd.IfNX && !cmd.IfXX && !cmd.Get && !cmd.KeepTTL && cmd.At.IsZero() {
		// Simple SET without additional options (except ttl).
		err := red.Str().SetExpires(cmd.Key, cmd.Value, cmd.TTL)
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		w.WriteString("OK")
		return true, nil
	}

	// SET with additional options.
	op := red.Str().SetWith(cmd.Key, cmd.Value)
	if cmd.IfXX {
		op = op.IfExists()
	} else if cmd.IfNX {
		op = op.IfNotExists()
	}
	if cmd.TTL > 0 {
		op = op.TTL(cmd.TTL)
	} else if !cmd.At.IsZero() {
		op = op.At(cmd.At)
	} else if cmd.KeepTTL {
		op = op.KeepTTL()
	}
	out, err := op.Run()

	// Determine the output status.
	var ok bool
	if cmd.IfXX {
		ok = out.Updated
	} else if cmd.IfNX {
		ok = out.Created
	} else {
		ok = err == nil
	}

	// Write the output.
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	if cmd.Get {
		// GET given: The key didn't exist before the SET.
		if !out.Prev.Exists() {
			w.WriteNull()
			return core.Value(nil), nil
		}
		// GET given: The previous value of the key.
		w.WriteBulk(out.Prev)
		return out.Prev, nil
	} else {
		// GET not given: Operation was aborted (conflict with one of the XX/NX options).
		if !ok {
			w.WriteNull()
			return false, nil
		}
		// GET not given: The key was set.
		w.WriteString("OK")
		return true, nil
	}
}
