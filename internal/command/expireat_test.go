package command

import (
	"fmt"
	"testing"
	"time"

	"github.com/nalgeon/redka/internal/testx"
)

func TestExpireAtParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		key  string
		at   time.Time
		err  error
	}{
		{
			name: "expireat",
			args: buildArgs("expireat"),
			key:  "",
			at:   time.Time{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "expireat name",
			args: buildArgs("expire", "name"),
			key:  "",
			at:   time.Time{},
			err:  ErrInvalidArgNum,
		},
		{
			name: "expireat name 60",
			args: buildArgs("expireat", "name", fmt.Sprintf("%d", time.Now().Add(60*time.Second).Unix())),
			key:  "name",
			at:   time.Now().Add(60 * time.Second),
			err:  nil,
		},
		{
			name: "expireat name age",
			args: buildArgs("expireat", "name", "age"),
			key:  "",
			at:   time.Time{},
			err:  ErrInvalidInt,
		},
		{
			name: "expireat name 60 age 60",
			args: buildArgs("expireat", "name", "60", "age", "60"),
			key:  "",
			at:   time.Time{},
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*ExpireAt).key, test.key)
				testx.AssertEqual(t, cmd.(*ExpireAt).at.Unix(), test.at.Unix())
			}
		})
	}
}

func TestExpireAtExec(t *testing.T) {
	t.Run("create expireat", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		expireAt := time.Now().Add(60 * time.Second)
		cmd := mustParse[*ExpireAt](fmt.Sprintf("expireat name %d", expireAt.Unix()))
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.UnixMilli()/1000)
	})

	t.Run("update expire", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		expireAt := time.Now()
		cmd := mustParse[*ExpireAt](fmt.Sprintf("expireat name %d", expireAt.Add(60*time.Second).Unix()))
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		cmd = mustParse[*ExpireAt](fmt.Sprintf("expireat name %d", expireAt.Add(20*time.Second).Unix()))
		conn = new(fakeConn)
		res, err = cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, true)
		testx.AssertEqual(t, conn.out(), "1")

		key, _ := db.Key().Get("name")
		testx.AssertEqual(t, *key.ETime/1000, expireAt.Add(20*time.Second).UnixMilli()/1000)
	})

	t.Run("set to zero", func(t *testing.T) {
		db, red := getDB(t)
		defer db.Close()

		_ = db.Str().Set("name", "alice")

		cmd := mustParse[*ExpireAt]("expireat name 0")
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

		cmd := mustParse[*ExpireAt]("expireat name -10")
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

		cmd := mustParse[*ExpireAt]("expireat age 1700000000")
		conn := new(fakeConn)
		res, err := cmd.Run(conn, red)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, false)
		testx.AssertEqual(t, conn.out(), "0")

		key, _ := db.Key().Get("age")
		testx.AssertEqual(t, key.Exists(), false)
	})
}
