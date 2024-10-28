package eval

import (
	"testing"

	"github.com/nalgeon/redka/internal/redis"
	"github.com/nalgeon/redka/internal/testx"
)

func TestEvalParse(t *testing.T) {
	tests := []struct {
		cmd  redis.BaseCmd
		want Eval
		err  error
	}{
		{
			cmd:  redis.NewBaseCmd([][]byte{[]byte("EVAL"), []byte("return 0"), []byte("0")}),
			want: Eval{script: "return 0", keys: []string(nil), args: []string(nil)},
			err:  nil,
		},
		{
			cmd:  redis.NewBaseCmd([][]byte{[]byte("EVAL"), []byte("return 0"), []byte("1"), []byte("key1")}),
			want: Eval{script: "return 0", keys: []string{"key1"}, args: []string(nil)},
			err:  nil,
		},
		{
			cmd:  redis.NewBaseCmd([][]byte{[]byte("EVAL"), []byte("return 0"), []byte("2"), []byte("key1"), []byte("key2")}),
			want: Eval{script: "return 0", keys: []string{"key1", "key2"}, args: []string(nil)},
			err:  nil,
		},
		{
			cmd:  redis.NewBaseCmd([][]byte{[]byte("EVAL"), []byte("return 0"), []byte("1"), []byte("key1"), []byte("arg1")}),
			want: Eval{script: "return 0", keys: []string{"key1"}, args: []string{"arg1"}},
			err:  nil,
		},
		{
			cmd:  redis.NewBaseCmd([][]byte{[]byte("EVAL"), []byte("return 0"), []byte("0"), []byte("arg1"), []byte("arg2")}),
			want: Eval{script: "return 0", keys: []string{}, args: []string{"arg1", "arg2"}},
			err:  nil,
		},
		{
			cmd:  redis.NewBaseCmd([][]byte{[]byte("EVAL")}),
			want: Eval{},
			err:  redis.ErrInvalidArgNum,
		},
		{
			cmd:  redis.NewBaseCmd([][]byte{[]byte("EVAL"), []byte("return 0")}),
			want: Eval{},
			err:  redis.ErrInvalidArgNum,
		},
	}

	for _, test := range tests {
		t.Run(test.cmd.String(), func(t *testing.T) {
			// redis.Parse does not play well with quoted strings
			cmd, err := ParseEval(test.cmd)
			testx.AssertEqual(t, err, test.err)
			if err == nil {
				testx.AssertEqual(t, cmd.script, test.want.script)
				testx.AssertEqual(t, cmd.keys, test.want.keys)
				testx.AssertEqual(t, cmd.args, test.want.args)
			} else {
				testx.AssertEqual(t, cmd, test.want)
			}
		})
	}
}
