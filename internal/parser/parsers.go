package parser

import (
	"strconv"
)

// Bytes parses a positional argument as a byte slice.
func Bytes(dest *[]byte) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args) == 0 {
			return false, args, nil
		}
		*dest = args[0]
		return true, args[1:], nil
	}
}

// String parses a positional argument as a string.
func String(dest *string) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args) == 0 {
			return false, args, nil
		}
		*dest = string(args[0])
		return true, args[1:], nil
	}
}

// Int parses a positional argument as an integer.
func Int(dest *int) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args) == 0 {
			return false, args, nil
		}
		var err error
		*dest, err = strconv.Atoi(string(args[0]))
		if err != nil {
			return true, args, ErrInvalidInt
		}
		return true, args[1:], nil
	}
}

// Float parses a positional argument as an float.
func Float(dest *float64) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args) == 0 {
			return false, args, nil
		}
		var err error
		*dest, err = strconv.ParseFloat(string(args[0]), 64)
		if err != nil {
			return true, args, ErrInvalidFloat
		}
		return true, args[1:], nil
	}
}

// Strings parses variadic arguments as a slice of strings.
func Strings(dest *[]string) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args) == 0 {
			return false, args, nil
		}
		*dest = make([]string, len(args))
		for i, arg := range args {
			(*dest)[i] = string(arg)
		}
		return true, nil, nil
	}
}

// AnyMap parses variadic name-value pairs.
func AnyMap(dest *map[string]any) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args)%2 != 0 {
			return false, args, nil
		}
		*dest = make(map[string]any, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			(*dest)[string(args[i])] = args[i+1]
		}
		return true, nil, nil
	}
}

// FloatMap parses variadic float-value pairs.
func FloatMap(dest *map[any]float64) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args)%2 != 0 {
			return false, args, nil
		}
		*dest = make(map[any]float64, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			flo, err := strconv.ParseFloat(string(args[i]), 64)
			if err != nil {
				return true, args, ErrInvalidFloat
			}
			(*dest)[string(args[i+1])] = flo
		}
		return true, nil, nil
	}
}

// Flag parses a named argument as a bool.
func Flag(name string, dest *bool) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args) == 0 {
			return false, args, nil
		}
		if string(args[0]) != name {
			return false, args, nil
		}
		*dest = true
		return true, args[1:], nil
	}
}

// Named parses a named argument with given parsers.
// Returns an error if any of the parsers does not fire.
func Named(name string, parsers ...ParserFunc) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		if len(args) == 0 {
			return false, args, nil
		}
		if string(args[0]) != name {
			return false, args, nil
		}
		allFired := true
		for _, parser := range parsers {
			var fired bool
			var err error
			fired, args, err = parser(args[1:])
			if err != nil {
				return fired, args, err
			}
			allFired = allFired && fired
		}
		if !allFired {
			return true, args, ErrSyntaxError
		}
		return true, args, nil
	}
}

// OneOf parses the arguments with one of the given parsers.
// Returns an error if more than one parser fires.
func OneOf(parsers ...ParserFunc) ParserFunc {
	return func(args [][]byte) (bool, [][]byte, error) {
		nFired := 0
		for _, parser := range parsers {
			var fired bool
			var err error
			fired, args, err = parser(args)
			if err != nil {
				return fired, args, err
			}
			if fired {
				nFired++
			}
		}
		if nFired > 1 {
			return true, args, ErrSyntaxError
		}
		return nFired > 0, args, nil
	}
}
