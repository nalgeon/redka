package server

import (
	"net"
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nalgeon/redka"
	"github.com/tidwall/redcon"
)

func TestHandlers(t *testing.T) {
	db, err := redka.Open("file:/data.db?vfs=memdb", nil)
	if err != nil {
		t.Fatal(err)
	}

	mux := createHandlers(db)
	tests := []struct {
		cmd  redcon.Command
		want string
	}{
		{
			cmd: redcon.Command{
				Raw:  []byte("ECHO hello"),
				Args: [][]byte{[]byte("ECHO"), []byte("hello")},
			},
			want: "hello",
		},
	}
	for _, test := range tests {
		conn := new(fakeConn)
		mux.ServeRESP(conn, test.cmd)
		if conn.out() != test.want {
			t.Fatalf("want '%s', got '%s'", test.want, conn.out())
		}
	}
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
