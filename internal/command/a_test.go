package command

import (
	"net"
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

func getDB(tb testing.TB) (*redka.DB, *redka.Tx) {
	tb.Helper()
	db, err := redka.Open(":memory:", nil)
	if err != nil {
		tb.Fatal(err)
	}
	return db, db.NoTx()
}

func mustParse[T Cmd](s string) T {
	parts := strings.Split(s, " ")
	args := buildArgs(parts[0], parts[1:]...)
	cmd, err := Parse(args)
	if err != nil {
		panic(err)
	}
	return cmd.(T)
}

func buildArgs(name string, args ...string) [][]byte {
	rargs := make([][]byte, len(args)+1)
	rargs[0] = []byte(name)
	for i, arg := range args {
		rargs[i+1] = []byte(arg)
	}
	return rargs
}

type fakeConn struct {
	parts []string
	ctx   any
}

func (c *fakeConn) RemoteAddr() string {
	return ""
}
func (c *fakeConn) Close() error {
	return nil
}
func (c *fakeConn) WriteError(msg string) {
	c.append(msg)
}
func (c *fakeConn) WriteString(str string) {
	c.append(str)
}
func (c *fakeConn) WriteBulk(bulk []byte) {
	c.append(string(bulk))
}
func (c *fakeConn) WriteBulkString(bulk string) {
	c.append(bulk)
}
func (c *fakeConn) WriteInt(num int) {
	c.append(strconv.Itoa(num))
}
func (c *fakeConn) WriteInt64(num int64) {
	c.append(strconv.FormatInt(num, 10))
}
func (c *fakeConn) WriteUint64(num uint64) {
	c.append(strconv.FormatUint(num, 10))
}
func (c *fakeConn) WriteArray(count int) {
	c.append(strconv.Itoa(count))
}
func (c *fakeConn) WriteNull() {
	c.append("(nil)")
}
func (c *fakeConn) WriteRaw(data []byte) {
	c.append(string(data))
}
func (c *fakeConn) WriteAny(any interface{}) {
	c.append(any.(string))
}
func (c *fakeConn) Context() interface{} {
	return c.ctx
}
func (c *fakeConn) SetContext(v interface{}) {
	c.ctx = v
}
func (c *fakeConn) SetReadBuffer(bytes int) {}
func (c *fakeConn) Detach() redcon.DetachedConn {
	return nil
}
func (c *fakeConn) ReadPipeline() []redcon.Command {
	return nil
}
func (c *fakeConn) PeekPipeline() []redcon.Command {
	return nil
}
func (c *fakeConn) NetConn() net.Conn {
	return nil
}
func (c *fakeConn) append(str string) {
	c.parts = append(c.parts, str)
}
func (c *fakeConn) out() string {
	return strings.Join(c.parts, ",")
}
