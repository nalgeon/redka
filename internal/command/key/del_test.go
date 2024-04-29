package key_test

import (
	"testing"

	"github.com/nalgeon/redka/internal/command"
	"github.com/nalgeon/redka/internal/command/key"
	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestDelParse(t *testing.T) {
	tests := []struct {
		name string
		args [][]byte
		want []string
		err  error
	}{
		{
			name: "del",
			args: command.BuildArgs("del"),
			want: nil,
			err:  redis.ErrInvalidArgNum,
		},
		{
			name: "del name",
			args: command.BuildArgs("del", "name"),
			want: []string{"name"},
			err:  nil,
		},
		{
			name: "del name age",
			args: command.BuildArgs("del", "name", "age"),
			want: []string{"name", "age"},
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := command.Parse(test.args)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.(*key.Del).Keys, test.want)
			}
		})
	}
}

func TestDelExec(t *testing.T) {
	tests := []struct {
		name string
		cmd  *key.Del
		res  any
		out  string
	}{
		{
			name: "del one",
			cmd:  command.MustParse[*key.Del]("del name"),
			res:  1,
			out:  "1",
		},
		{
			name: "del all",
			cmd:  command.MustParse[*key.Del]("del name age"),
			res:  2,
			out:  "2",
		},
		{
			name: "del some",
			cmd:  command.MustParse[*key.Del]("del name age street"),
			res:  2,
			out:  "2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, red := getDB(t)
			defer db.Close()

			_ = db.Str().Set("name", "alice")
			_ = db.Str().Set("age", 50)
			_ = db.Str().Set("city", "paris")

			conn := redis.NewFakeConn()
			res, err := test.cmd.Run(conn, red)
			testx.AssertNoErr(t, err)
			testx.AssertEqual(t, res, test.res)
			testx.AssertEqual(t, conn.Out(), test.out)

			name, _ := db.Str().Get("name")
			testx.AssertEqual(t, name.Exists(), false)
			city, _ := db.Str().Get("city")
			testx.AssertEqual(t, city.String(), "paris")
		})
	}
}
