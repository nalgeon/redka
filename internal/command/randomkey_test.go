package command

import (
	"slices"
	"testing"

	"github.com/nalgeon/redka/internal/core"
	"github.com/nalgeon/redka/internal/testx"
)

func TestRandomKeyParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		err  error
	}{
		{
			name: "randomkey",
			args: buildArgs("randomkey"),
			err:  nil,
		},
		{
			name: "randomkey name",
			args: buildArgs("randomkey", "name"),
			err:  ErrInvalidArgNum,
		},
		{
			name: "randomkey name age",
			args: buildArgs("randomkey", "name", "age"),
			err:  ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.args)
			testx.AssertEqual(t, err, test.err)
		})
	}
}

func TestRandomKeyExec(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()
		_ = db.Str().Set("name", "alice")
		_ = db.Str().Set("age", 25)
		_ = db.Str().Set("city", "paris")
		keys := []string{"name", "age", "city"}

		conn := new(fakeConn)
		cmd := mustParse[*RandomKey]("randomkey")
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, slices.Contains(keys, res.(core.Key).Key), true)
		testx.AssertEqual(t, slices.Contains(keys, conn.out()), true)
	})
	t.Run("not found", func(t *testing.T) {
		db := getDB(t)
		defer db.Close()
		conn := new(fakeConn)
		cmd := mustParse[*RandomKey]("randomkey")
		res, err := cmd.Run(conn, db)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, res, nil)
		testx.AssertEqual(t, conn.out(), "(nil)")
	})
}
