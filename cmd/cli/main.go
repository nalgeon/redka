// Redka CLI. Executes commands from a file.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
	"github.com/nalgeon/redka/internal/command"
)

const dbURI = ":memory:"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: redka-cli <filename>\n")
		flag.PrintDefaults()
	}
}

func main() {
	// Parse command line arguments.
	flag.Parse()
	if len(flag.Args()) != 1 {
		flag.Usage()
		os.Exit(1)
	}

	// File with commands.
	filename := flag.Arg(0)

	// Open the database.
	db, err := redka.Open(dbURI, nil)
	if err != nil {
		fail("failed to open database: %v\n", err)
	}

	// Read commands from the file.
	cmds, err := readCommands(filename)
	if err != nil {
		fail("failed to read commands: %v\n", err)
	}

	// Execute the commands.
	r := newRunner(db)
	err = r.run(cmds)
	if err != nil {
		os.Exit(1)
	}
}

// readCommands reads commands from a file.
func readCommands(filename string) ([][]byte, error) {
	if filename == "" {
		return nil, nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(data, []byte{'\n'})
	commands := make([][]byte, 0, len(lines))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || bytes.HasPrefix(line, []byte{'#'}) {
			continue
		}
		commands = append(commands, line)
	}

	return commands, nil
}

// multiHandlers executes commands when the runner is in a MULTI state.
var multiHandlers = map[string]func(*runner, []byte) error{
	"multi": func(r *runner, cmd []byte) error {
		fmt.Println(command.ErrNestedMulti)
		return command.ErrNestedMulti
	},
	"exec": func(r *runner, cmd []byte) error {
		fmt.Println(len(r.cmds))
		err := r.db.Update(func(tx *redka.Tx) error {
			return r.runBatch(r.cmds, command.RedkaTx(tx))
		})
		r.inMulti = false
		r.clear()
		return err
	},
	"discard": func(r *runner, cmd []byte) error {
		fmt.Println("OK")
		r.inMulti = false
		r.clear()
		return nil
	},
	"_": func(r *runner, cmd []byte) error {
		fmt.Println("QUEUED")
		r.push(cmd)
		return nil
	},
}

// singleHandlers executes commands when the runner is in a regular state.
var singleHandlers = map[string]func(*runner, []byte) error{
	"multi": func(r *runner, cmd []byte) error {
		fmt.Println("OK")
		r.inMulti = true
		return nil
	},
	"exec": func(r *runner, cmd []byte) error {
		fmt.Println(command.ErrNotInMulti)
		return command.ErrNotInMulti
	},
	"discard": func(r *runner, cmd []byte) error {
		fmt.Println(command.ErrNotInMulti)
		return command.ErrNotInMulti
	},
	"_": func(r *runner, cmd []byte) error {
		return r.runSingle(cmd, command.RedkaDB(r.db))
	},
}

// runner executes commands.
type runner struct {
	db *redka.DB
	w  writer
	*state
}

func newRunner(db *redka.DB) *runner {
	return &runner{db: db, w: writer{}, state: newState()}
}

// run executes commands.
func (r *runner) run(cmds [][]byte) error {
	if len(cmds) == 1 {
		return r.runSingle(cmds[0], command.RedkaDB(r.db))
	}
	for _, cmd := range cmds {
		r.print(cmd)
		err := r.handle(cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

// handle manages mode switching and command execution.
func (r *runner) handle(cmd []byte) error {
	name := normName(cmd)
	var handlers map[string]func(*runner, []byte) error
	if r.inMulti {
		handlers = multiHandlers
	} else {
		handlers = singleHandlers
	}
	if h, ok := handlers[name]; ok {
		return h(r, cmd)
	} else {
		return handlers["_"](r, cmd)
	}
}

// runBatch executes a batch of commands.
func (r *runner) runBatch(cmds [][]byte, red command.Redka) error {
	for _, cmd := range cmds {
		err := r.runSingle(cmd, red)
		if err != nil {
			return err
		}
	}
	return nil
}

// runSingle executes a single command.
func (r *runner) runSingle(cmd []byte, red command.Redka) error {
	c, err := command.Parse(bytes.Fields(cmd))
	if err != nil {
		return fmt.Errorf("parse command '%s': %v", cmd, err)
	}
	_, err = c.Run(r.w, red)
	if err != nil {
		return fmt.Errorf("run command '%s': %v", cmd, err)
	}
	return nil
}

// print prints a command and its arguments.
func (r *runner) print(cmd []byte) {
	fmt.Println("---")
	fmt.Printf("> %s\n", cmd)
}

// state stores the state of the runner.
type state struct {
	inMulti bool
	cmds    [][]byte
}

func newState() *state {
	return &state{cmds: [][]byte{}}
}
func (s *state) push(cmd []byte) {
	s.cmds = append(s.cmds, cmd)
}
func (s *state) clear() {
	s.cmds = [][]byte{}
}

// writer writes command results to the standard output.
type writer struct{}

func (w writer) WriteError(msg string) {
	fmt.Println(msg)
}
func (w writer) WriteString(str string) {
	fmt.Println(str)
}
func (w writer) WriteBulk(bulk []byte) {
	fmt.Println(string(bulk))
}
func (w writer) WriteBulkString(bulk string) {
	fmt.Println(bulk)
}
func (w writer) WriteInt(num int) {
	fmt.Printf("%d\n", num)
}
func (w writer) WriteInt64(num int64) {
	fmt.Printf("%d\n", num)
}
func (w writer) WriteUint64(num uint64) {
	fmt.Printf("%d\n", num)
}
func (w writer) WriteArray(count int) {
	// do nothing
}
func (w writer) WriteNull() {
	fmt.Println("(nil)")
}
func (w writer) WriteRaw(data []byte) {
	fmt.Println(string(data))
}
func (w writer) WriteAny(v any) {
	fmt.Printf("%v\n", v)
}

// fail prints an error message and exits with status 1.
func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}

// normName returns the name of a command.
func normName(cmd []byte) string {
	return string(bytes.Fields(cmd)[0])
}
