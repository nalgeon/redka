package command

import (
	"time"

	"github.com/nalgeon/redka/internal/parser"
)

// Set sets the string value of a key, ignoring its type.
// The key is created if it doesn't exist.
// SET key value [NX | XX] [EX seconds | PX milliseconds ]
// https://redis.io/commands/set
type Set struct {
	baseCmd
	key   string
	value []byte
	ifNX  bool
	ifXX  bool
	ttl   time.Duration
}

func parseSet(b baseCmd) (*Set, error) {
	cmd := &Set{baseCmd: b}

	// Parse the command arguments.
	var ttlSec, ttlMs int
	err := parser.New(
		parser.String(&cmd.key),
		parser.Bytes(&cmd.value),
		parser.OneOf(
			parser.Flag("nx", &cmd.ifNX),
			parser.Flag("xx", &cmd.ifXX),
		),
		parser.OneOf(
			parser.NamedInt("ex", &ttlSec),
			parser.NamedInt("px", &ttlMs),
		),
	).Required(2).Run(cmd.args)
	if err != nil {
		return cmd, err
	}

	// Parse the TTL.
	if ttlSec > 0 {
		cmd.ttl = time.Duration(ttlSec) * time.Second
	} else if ttlMs > 0 {
		cmd.ttl = time.Duration(ttlMs) * time.Millisecond
	}
	if cmd.ttl < 0 {
		return cmd, ErrInvalidExpireTime
	}

	return cmd, nil
}

func (cmd *Set) Run(w Writer, red Redka) (any, error) {
	// Build and run the command.
	op := red.Str().SetWith(cmd.key, cmd.value)
	if cmd.ifXX {
		op = op.IfExists()
	} else if cmd.ifNX {
		op = op.IfNotExists()
	}
	if cmd.ttl > 0 {
		op = op.TTL(cmd.ttl)
	}
	out, err := op.Run()

	// Determine the output status.
	var ok bool
	if cmd.ifXX {
		ok = out.Updated
	} else if cmd.ifNX {
		ok = out.Created
	} else {
		ok = err == nil
	}

	// Write the output.
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if !ok {
		w.WriteNull()
		return false, nil
	}
	w.WriteString("OK")
	return true, nil
}
