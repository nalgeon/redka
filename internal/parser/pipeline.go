// Package parser implements command arguments parsing.
package parser

import (
	"errors"
)

var (
	ErrInvalidArgNum = errors.New("ERR wrong number of arguments")
	ErrInvalidFloat  = errors.New("ERR value is not a float")
	ErrInvalidInt    = errors.New("ERR value is not an integer")
	ErrSyntaxError   = errors.New("ERR syntax error")
)

// ParserFunc parses some of the arguments and returns the rest.
type ParserFunc func(args [][]byte) (bool, [][]byte, error)

// Pipeline parses command arguments according to a sequence of parsers.
type Pipeline struct {
	parsers   []ParserFunc
	nRequired int
}

// New creates a new pipeline with the given parsers.
func New(parsers ...ParserFunc) *Pipeline {
	return &Pipeline{parsers: parsers}
}

// Required sets the number of required positional arguments.
func (p *Pipeline) Required(n int) *Pipeline {
	p.nRequired = n
	return p
}

// Run parses the arguments according to the pipeline.
func (p *Pipeline) Run(args [][]byte) error {
	if len(args) < p.nRequired {
		return ErrInvalidArgNum
	}

	// Named arguments order is not guaranteed,
	// so we need to try all parsers for each argument.
	for len(args) > 0 && len(p.parsers) > 0 {
		var err error
		var fired bool
		firedIdx := -1

		// Try all parsers until one fires.
		for i, parser := range p.parsers {
			fired, args, err = parser(args)
			if err != nil {
				return err
			}
			if fired {
				firedIdx = i
				break
			}
		}

		if firedIdx == -1 {
			// No parser fired.
			break
		}

		// Remove the fired parser.
		p.parsers = append(p.parsers[:firedIdx], p.parsers[firedIdx+1:]...)
	}

	// Check if all arguments were parsed.
	if len(args) != 0 {
		return ErrSyntaxError
	}
	return nil
}
