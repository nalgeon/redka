package command

import (
	"strconv"
	"time"

	"github.com/nalgeon/redka/internal/rstring"
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
	parseExists := func(cmd *Set, value string) error {
		switch value {
		case "nx":
			cmd.ifNX = true
		case "xx":
			cmd.ifXX = true
		default:
			return ErrSyntaxError
		}
		return nil
	}

	parseExpires := func(cmd *Set, unit string, value string) error {
		valueInt, err := strconv.Atoi(value)
		if err != nil {
			return ErrInvalidInt
		}

		switch string(unit) {
		case "ex":
			cmd.ttl = time.Duration(valueInt) * time.Second
		case "px":
			cmd.ttl = time.Duration(valueInt) * time.Millisecond
		default:
			return ErrSyntaxError
		}

		if cmd.ttl <= 0 {
			return ErrInvalidExpireTime
		}
		return nil
	}

	cmd := &Set{baseCmd: b}
	if len(cmd.args) < 2 || len(cmd.args) > 5 {
		return cmd, ErrInvalidArgNum
	}

	cmd.key = string(cmd.args[0])
	cmd.value = cmd.args[1]

	if len(cmd.args) == 3 || len(cmd.args) == 5 {
		err := parseExists(cmd, string(cmd.args[2]))
		if err != nil {
			return cmd, err
		}
	}

	if len(cmd.args) == 4 {
		err := parseExpires(cmd, string(cmd.args[2]), string(cmd.args[3]))
		if err != nil {
			return cmd, err
		}
	}

	if len(cmd.args) == 5 {
		err := parseExpires(cmd, string(cmd.args[3]), string(cmd.args[4]))
		if err != nil {
			return cmd, err
		}
	}

	return cmd, nil
}

func (cmd *Set) Run(w Writer, red Redka) (any, error) {
	var out rstring.SetOut
	var ok bool
	var err error
	if cmd.ifXX {
		out, err = red.Str().SetWith(cmd.key, cmd.value).TTL(cmd.ttl).IfExists().Run()
		ok = out.Updated
	} else if cmd.ifNX {
		out, err = red.Str().SetWith(cmd.key, cmd.value).TTL(cmd.ttl).IfNotExists().Run()
		ok = out.Created
	} else {
		err = red.Str().SetExpires(cmd.key, cmd.value, cmd.ttl)
		ok = err == nil
	}
	return cmd.run(w, ok, err)
}

func (cmd *Set) run(w Writer, ok bool, err error) (any, error) {
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
