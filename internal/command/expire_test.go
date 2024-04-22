package command

import (
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/testx"
)

func TestExpireParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		ttl  time.Duration
		err  error
	}{
		{
			name: "expire",
			args: buildArgs("expire"),
			key:  "",
			ttl:  0,
			err:  ErrInvalidArgNum,
		},
		{
			name: "expire name",
			args: buildArgs("expire", "name"),
			key:  "",
			ttl:  0,
			err:  ErrInvalidArgNum,
		},
		{
			name: "expire name 60",
			args: buildArgs("expire", "name", "60"),
			key:  "name",
			ttl:  60 * 1000 * time.Millisecond,
			err:  nil,
		},
		{
			name: "expire name age",
			args: buildArgs("expire", "name", "age"),
			key:  "",
			ttl:  0,
			err:  ErrInvalidInt,
		},
		{
			name: "expire name 60 age 60",
			args: buildArgs("expire", "name", "60", "age", "60"),
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

func TestExpireExec(t *testing.T) {
	t.Run("create expire", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("expire name 60")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		expireAt := time.Now().Add(60 * time.Second)
		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("update expire", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().SetExpires("name", "alice", 60*time.Second)

		cmd := mustParse[*Expire]("expire name 30")
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
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("expire name 0")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
	})

	t.Run("negative", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("expire name -10")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, key.Exists(), false)
	})

	t.Run("not found", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*Expire]("expire age 60")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		key, _ := db.Key().Get("age")
		testx.AssertEqual(t, key.Exists(), false)
	})
}
