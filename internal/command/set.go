package command

import (
	"strconv"
	"time"
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
	if len(cmd.args) < 2 {
		return cmd, ErrInvalidArgNum
	}

	cmd.key = string(cmd.args[0])
	cmd.value = cmd.args[1]
	cmd.args = cmd.args[2:]

	err := cmd.parseExists()
	if err != nil {
		return cmd, err
	}

	err = cmd.parseExpires()
	if err != nil {
		return cmd, err
	}

	if len(cmd.args) > 0 {
		return cmd, ErrSyntaxError
	}

	return cmd, nil
}

func (cmd *Set) parseExists() error {
	if len(cmd.args) == 0 {
		return nil
	}

	value := string(cmd.args[0])
	switch value {
	case "nx":
		cmd.ifNX = true
		cmd.args = cmd.args[1:]
	case "xx":
		cmd.ifXX = true
		cmd.args = cmd.args[1:]
	}
	return nil
}

func (cmd *Set) parseExpires() error {
	if len(cmd.args) == 0 {
		return nil
	}

	unit := string(cmd.args[0])
	if unit != "ex" && unit != "px" {
		return nil
	}

	valueInt, err := strconv.Atoi(string(cmd.args[1]))
	if err != nil {
		return ErrInvalidInt
	}

	switch unit {
	case "ex":
		cmd.ttl = time.Duration(valueInt) * time.Second
		cmd.args = cmd.args[2:]
	case "px":
		cmd.ttl = time.Duration(valueInt) * time.Millisecond
		cmd.args = cmd.args[2:]
	}

	if cmd.ttl <= 0 {
		return ErrInvalidExpireTime
	}
	return nil
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
