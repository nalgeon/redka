package command

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/testx"
)

func TestPExpireParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		ttl  time.Duration
		err  error
	}{
		{
			name: "pexpire",
			args: buildArgs("pexpire"),
			key:  "",
			ttl:  0,
			err:  ErrInvalidArgNum,
		},
		{
			name: "pexpire name",
			args: buildArgs("pexpire", "name"),
			key:  "",
			ttl:  0,
			err:  ErrInvalidArgNum,
		},
		{
			name: "pexpire name 5000",
			args: buildArgs("pexpire", "name", "5000"),
			key:  "name",
			ttl:  5000 * time.Millisecond,
			err:  nil,
		},
		{
			name: "pexpire name age",
			args: buildArgs("pexpire", "name", "age"),
			key:  "",
			ttl:  0,
			err:  ErrInvalidInt,
		},
		{
			name: "pexpire name 100 age 100",
			args: buildArgs("pexpire", "name", "100", "age", "100"),
			key:  "",
			ttl:  0,
			err:  ErrSyntaxError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*Expire).key, test.key)
				testx.AssertEqual(t, cmd.(*Expire).ttl, test.ttl)
			}
		})
	}
}

func TestPExpireExec(t *testing.T) {
	db, red := getDB(t)
	defer db.Close()

	t.Run("create pexpire", func(t *testing.T) {
		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("pexpire name 60000")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("update pexpire", func(t *testing.T) {
		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := mustParse[*Expire]("pexpire name 30000")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		expireAt := time.Now().Add(30 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("set to zero", func(t *testing.T) {
		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("pexpire name 0")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
	})

	t.Run("negative", func(t *testing.T) {
		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("pexpire name -1000")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
	})

	t.Run("not found", func(t *testing.T) {
		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("pexpire age 1000")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		key, _ := db.Key().Get("age")
		testx.AssertEqual(t, key.Exists(), false)
	})
}
