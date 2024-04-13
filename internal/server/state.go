package server

import (
	"fmt"
	"strings"

	"github.com/nalgeon/redka/internal/command"
	"github.com/tidwall/redcon"
)

// normName returns the normalized command name.
func normName(cmd redcon.Command) string {
	return strings.ToLower(string(cmd.Args[0]))
}

// getState returns the connection state.
func getState(conn redcon.Conn) *connState {
	state := conn.Context()
	if state == nil {
		state = new(connState)
		conn.SetContext(state)
	}
	return state.(*connState)
}

// connState represents the connection state.
type connState struct {
	inMulti bool
	cmds    []command.Cmd
}

// push adds a command to the state.
func (s *connState) push(cmd command.Cmd) {
	s.cmds = append(s.cmds, cmd)
}

// pop removes the last command from the state and returns it.
func (s *connState) pop() command.Cmd {
	if len(s.cmds) == 0 {
		return nil
	}
	var last command.Cmd
	s.cmds, last = s.cmds[:len(s.cmds)-1], s.cmds[len(s.cmds)-1]
	return last
}

// clear removes all commands from the state.
func (s *connState) clear() {
	s.cmds = []command.Cmd{}
}

// String returns the string representation of the state.
func (s *connState) String() string {
	cmds := make([]string, len(s.cmds))
	for i, cmd := range s.cmds {
		cmds[i] = cmd.Name()
	}
	return fmt.Sprintf("[inMulti=%v,commands=%v]", s.inMulti, cmds)
}
