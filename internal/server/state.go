package server

import (
	"fmt"
	"strings"

	"github.com/nalgeon/redka/internal/command"
	"github.com/tidwall/redcon"
)

func normName(cmd redcon.Command) string {
	return strings.ToLower(string(cmd.Args[0]))
}

func getState(conn redcon.Conn) *connState {
	state := conn.Context()
	if state == nil {
		state = new(connState)
		conn.SetContext(state)
	}
	return state.(*connState)
}

type connState struct {
	inMulti bool
	cmds    []command.Cmd
}

func (s *connState) push(cmd command.Cmd) {
	s.cmds = append(s.cmds, cmd)
}
func (s *connState) pop() command.Cmd {
	if len(s.cmds) == 0 {
		return nil
	}
	var last command.Cmd
	s.cmds, last = s.cmds[:len(s.cmds)-1], s.cmds[len(s.cmds)-1]
	return last
}
func (s *connState) clear() {
	s.cmds = []command.Cmd{}
}
func (s *connState) String() string {
	cmds := make([]string, len(s.cmds))
	for i, cmd := range s.cmds {
		cmds[i] = cmd.Name()
	}
	return fmt.Sprintf("[inMulti=%v,commands=%v]", s.inMulti, cmds)
}
