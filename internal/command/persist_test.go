package command

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/testx"
)

func TestPersistParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		err  error
	}{
		{
			name: "persist",
			args: buildArgs("persist"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
		{
			name: "persist name",
			args: buildArgs("persist", "name"),
			key:  "name",
			err:  nil,
		},
		{
			name: "persist name age",
			args: buildArgs("persist", "name", "age"),
			key:  "",
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Persist).key, test.key)
			}
		})
	}
}

func TestPersistExec(t *testing.T) {
	t.Run("persist to persist", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Persist]("persist name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})

	t.Run("volatile to persist", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := mustParse[*Persist]("persist name")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.ETime, (*int64)(nil))
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Persist]("persist age")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		key, _ := db.Key().Get("age")
		testx.AssertEqual(t, key.Exists(), false)
	})
}
