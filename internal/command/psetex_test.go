package command

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/testx"
)

func TestPSetEXParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want SetEX
		err  error
	}{
		{
			name: "psetex",
			args: buildArgs("psetex"),
			want: SetEX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "psetex name",
			args: buildArgs("psetex", "name"),
			want: SetEX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "psetex name alice",
			args: buildArgs("psetex", "name", "alice"),
			want: SetEX{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "psetex name alice 60",
			args: buildArgs("psetex", "name", "alice", "60"),
			want: SetEX{},
			err:  ErrInvalidInt,
		},
		{
			name: "psetex name 60 alice",
			args: buildArgs("psetex", "name", "60", "alice"),
			want: SetEX{key: "name", value: []byte("alice"), ttl: 60 * time.Millisecond},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				cm := cmd.(*SetEX)
				testx.AssertEqual(t, cm.key, test.want.key)
				testx.AssertEqual(t, cm.value, test.want.value)
				testx.AssertEqual(t, cm.ttl, test.want.ttl)
			}
		})
	}
}

func TestPSetEXExec(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		cmd := mustParse[*SetEX]("psetex name 60000 alice")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "alice")
	})

	t.Run("update", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*SetEX]("psetex name 60000 bob")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
	})

	t.Run("change ttl", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := mustParse[*SetEX]("psetex name 10000 bob")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "OK")

		expireAt := time.Now().Add(10 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)

		name, _ := db.Str().Get("name")
		testx.AssertEqual(t, name.String(), "bob")
	})

}
